//! The long-lived half of the client.
//!
//! One process per developer machine, holding one presence **per Claude Code
//! session**. It owns the credential, the sessions, their WebSockets and their
//! inboxes, and it is the only thing here that talks to the network - so the
//! MCP server, the hooks and the CLI all stay credential-free and the token
//! never appears in a subprocess environment.
//!
//! ```text
//! hook / MCP / CLI  --unix socket-->  daemon  --wss (one per session)-->  gateway
//! ```
//!
//! A developer with three Claude Code windows open is three agents to the
//! network: each has its own session, its own repository and branch, its own
//! inbox and its own heartbeat. The gateway's `/v1/agent` connection is bound
//! to exactly one session (`ClientFrame::Connect`), so each session keeps its
//! own link; what they share is the process, the credential and the socket.
//!
//! # Two rules it exists to keep
//!
//! **It never blocks Claude.** A gateway that is down, a token that was
//! revoked, a laptop on a train: all of them make the daemon retry quietly and
//! make the MCP tools return an error the agent can ignore. Nothing here is
//! allowed to become a reason somebody cannot code.
//!
//! **A message is only acknowledged when the agent has actually seen it.** The
//! gateway marks a message delivered when the bytes go out; this daemon acks
//! it when `kintri_inbox` hands it to Claude. Between those two points the
//! message is still owed, which is what makes a crash mid-handover survivable.
//!
//! # Which session is "this one"
//!
//! Claude Code tells its hooks which session they belong to (`session_id` on
//! stdin) but tells an MCP server nothing. The one fact both share is the
//! working directory, so a request from the MCP server carries its `cwd` and
//! the daemon answers as the session registered from that directory - the
//! newest one, when two windows share a checkout. A request with no match
//! (a `kintri` command run by hand somewhere else) is answered as the newest
//! session of all, and the inbox drains every session's messages, because a
//! person at a terminal wants to see what arrived, wherever it was addressed.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::agent_protocol::{ClientFrame, MessageView, ServerFrame};
use crate::model::AgentSessionId;
use anyhow::{anyhow, Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{watch, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::client::Gateway;
use crate::credentials::Credentials;
use crate::ipc::{socket_path, Request, Response};
use crate::workspace::{self, Checkout};

/// Reconnect delays, in seconds, then repeat the last one.
///
/// A laptop that closed its lid comes back to a gateway that has not moved,
/// and a gateway that is being deployed comes back within a minute. Jitter is
/// added so that a fleet of daemons does not reconnect in lockstep and do to
/// the gateway what the outage did.
const BACKOFF_SECS: [u64; 6] = [1, 2, 5, 10, 30, 60];

/// How many messages one session's inbox holds before the oldest is dropped.
///
/// Bounded on purpose: a Claude that never calls `kintri_inbox` must not turn
/// this process into a memory leak on somebody's laptop.
const INBOX_CAPACITY: usize = 500;

/// One registered Claude Code session: its presence on the gateway, its link
/// and its inbox.
struct Session {
    client_session_id: String,
    session_id: AgentSessionId,
    client: String,
    /// The directory the session was started in; how MCP requests find it.
    cwd: PathBuf,
    checkout: Checkout,
    started_at: chrono::DateTime<chrono::Utc>,
    inbox: Mutex<VecDeque<MessageView>>,
    connected: Mutex<bool>,
    /// Flipped to stop this session's tasks without touching the others'.
    stop: watch::Sender<bool>,
}

impl Session {
    async fn push(&self, message: MessageView) {
        let mut inbox = self.inbox.lock().await;
        if inbox.iter().any(|m| m.id == message.id) {
            // Redelivery is expected - the gateway hands a message over again
            // until it is acknowledged - so it must not become a duplicate.
            return;
        }
        if inbox.len() >= INBOX_CAPACITY {
            inbox.pop_front();
        }
        inbox.push_back(message);
    }

    fn describe(&self) -> Value {
        json!({
            "client_session_id": self.client_session_id,
            "session_id": self.session_id,
            "client": self.client,
            "cwd": self.cwd,
            "repository": self.checkout.repository,
            "branch": self.checkout.branch,
            "started_at": self.started_at,
        })
    }
}

/// Everything the daemon shares between its tasks.
struct State {
    gateway: Gateway,
    /// By `client_session_id`, the id the hook knows.
    sessions: Mutex<HashMap<String, Arc<Session>>>,
    started_at: chrono::DateTime<chrono::Utc>,
    github_login: Option<String>,
    display_name: Option<String>,
}

/// What `kintri daemon start` was told.
pub struct Options {
    /// Where credentials and the socket live.
    pub config_dir: PathBuf,
    /// A session to register at once, for a daemon started by hand. A daemon
    /// started for a hook begins empty and is told about sessions over IPC.
    pub initial: Option<InitialSession>,
    /// GitHub login, if the developer configured one.
    pub github_login: Option<String>,
    /// Display name, used only as a hint.
    pub display_name: Option<String>,
}

pub struct InitialSession {
    pub client_session_id: String,
    pub client: String,
    pub cwd: PathBuf,
}

/// Run until the process is told to stop.
pub async fn run(credentials: Credentials, options: Options) -> Result<()> {
    let gateway = Gateway::new(&credentials)?;
    let state = Arc::new(State {
        gateway,
        sessions: Mutex::new(HashMap::new()),
        started_at: chrono::Utc::now(),
        github_login: options.github_login,
        display_name: options.display_name,
    });

    // The socket comes up before any session is registered, so a hook that
    // spawned this process finds it answering and registers over IPC like
    // every later session does. One code path, not two.
    let listener = bind(&options.config_dir)?;
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let ipc = tokio::spawn(serve_ipc(listener, Arc::clone(&state), stop_tx.clone()));

    if let Some(initial) = options.initial {
        if let Err(e) = register(
            &state,
            initial.client_session_id,
            initial.client,
            initial.cwd,
        )
        .await
        {
            tracing::warn!(error = %e, "could not register the initial session; waiting for hooks");
        }
    }

    tokio::select! {
        _ = stop_rx.changed() => {}
        _ = shutdown_signal() => {
            let _ = stop_tx.send(true);
        }
    }

    ipc.abort();
    let _ = std::fs::remove_file(socket_path(&options.config_dir));

    // Best effort, and deliberately not retried: the TTL is what actually
    // takes a session offline, and an end that never arrives is the case the
    // whole design is built around.
    let sessions: Vec<Arc<Session>> = state
        .sessions
        .lock()
        .await
        .drain()
        .map(|(_, s)| s)
        .collect();
    for session in sessions {
        let _ = session.stop.send(true);
        if let Err(e) = state.gateway.end_session(session.session_id).await {
            tracing::debug!(error = %e, session = %session.session_id, "could not report the end of a session");
        }
    }
    Ok(())
}

/// Register a session with the gateway and start keeping it alive.
///
/// Idempotent on `client_session_id`: a retried hook, or a daemon that was
/// restarted under a still-running Claude, refreshes the presence rather than
/// creating a second one. The gateway upserts on the same key.
async fn register(
    state: &Arc<State>,
    client_session_id: String,
    client: String,
    cwd: PathBuf,
) -> Result<Arc<Session>> {
    if let Some(existing) = state.sessions.lock().await.get(&client_session_id) {
        return Ok(Arc::clone(existing));
    }

    // Canonical, so the hook's `/tmp/x` and the MCP server's `/private/tmp/x`
    // are the same directory.
    let cwd = canonical(&cwd);
    let checkout = workspace::inspect(&cwd);
    let registration = state
        .gateway
        .register_session(&json!({
            "client": client,
            "client_version": env!("CARGO_PKG_VERSION"),
            "client_session_id": client_session_id,
            "project": checkout.project,
            "repository": checkout.repository,
            "branch": checkout.branch,
            "email": checkout.email,
            "github_login": state.github_login,
            "display_name": state.display_name,
        }))
        .await
        .context("register this session with the gateway")?;

    let heartbeat_secs = registration.heartbeat_interval_secs.max(1);
    tracing::info!(
        session_id = %registration.session_id,
        client_session_id = %client_session_id,
        repository = ?checkout.repository,
        branch = ?checkout.branch,
        heartbeat_secs,
        "registered"
    );

    let (stop, stop_rx) = watch::channel(false);
    let session = Arc::new(Session {
        client_session_id: client_session_id.clone(),
        session_id: registration.session_id,
        client,
        cwd,
        checkout,
        started_at: chrono::Utc::now(),
        inbox: Mutex::new(VecDeque::new()),
        connected: Mutex::new(false),
        stop,
    });

    let mut sessions = state.sessions.lock().await;
    // A second hook for the same id raced us to the gateway; keep the one
    // already in the map and let ours lapse by TTL.
    if let Some(existing) = sessions.get(&client_session_id) {
        return Ok(Arc::clone(existing));
    }
    sessions.insert(client_session_id, Arc::clone(&session));
    drop(sessions);

    tokio::spawn(maintain_link(
        Arc::clone(state),
        Arc::clone(&session),
        heartbeat_secs,
        stop_rx.clone(),
    ));
    tokio::spawn(keep_alive(
        Arc::clone(state),
        Arc::clone(&session),
        heartbeat_secs,
        stop_rx,
    ));
    Ok(session)
}

/// Take one session offline; the rest of the daemon carries on.
async fn unregister(state: &Arc<State>, client_session_id: &str) -> Result<bool> {
    let Some(session) = state.sessions.lock().await.remove(client_session_id) else {
        return Ok(false);
    };
    let _ = session.stop.send(true);
    if let Err(e) = state.gateway.end_session(session.session_id).await {
        tracing::debug!(error = %e, "could not report the end of the session");
    }
    tracing::info!(session_id = %session.session_id, "ended");
    Ok(true)
}

/// The sessions a request from `cwd` speaks for: those registered from that
/// directory, newest first; or every session when nothing matches.
async fn sessions_for(state: &State, cwd: Option<&str>) -> Vec<Arc<Session>> {
    let all: Vec<Arc<Session>> = state.sessions.lock().await.values().cloned().collect();
    let mut chosen: Vec<Arc<Session>> = match cwd.map(Path::new).map(canonical) {
        Some(dir) => all.iter().filter(|s| s.cwd == dir).cloned().collect(),
        None => Vec::new(),
    };
    if chosen.is_empty() {
        chosen = all;
    }
    chosen.sort_by(|a, b| b.started_at.cmp(&a.started_at));
    chosen
}

/// A directory with symlinks resolved, or as given when it no longer exists.
fn canonical(dir: &Path) -> PathBuf {
    std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf())
}

/// The one session a request acts as, or an error that says why there is none.
async fn session_for(state: &State, cwd: Option<&str>) -> Result<Arc<Session>> {
    sessions_for(state, cwd)
        .await
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no Claude Code session is registered with the daemon yet"))
}

fn bind(config_dir: &Path) -> Result<UnixListener> {
    std::fs::create_dir_all(config_dir).context("create config directory")?;
    let path = socket_path(config_dir);
    // A socket left behind by a daemon that was killed. Connecting to it
    // fails with ECONNREFUSED, so removing it is safe once we know nobody
    // answered.
    if path.exists() && std::os::unix::net::UnixStream::connect(&path).is_err() {
        let _ = std::fs::remove_file(&path);
    }
    let listener = UnixListener::bind(&path)
        .with_context(|| format!("bind {}. Is a daemon already running?", path.display()))?;
    restrict_socket(&path)?;
    tracing::info!(socket = %path.display(), "listening");
    Ok(listener)
}

#[cfg(unix)]
fn restrict_socket(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .context("restrict the daemon socket to this user")
}

/// Keep one session's WebSocket up, and the session alive.
async fn maintain_link(
    state: Arc<State>,
    session: Arc<Session>,
    heartbeat_secs: u64,
    mut stop: watch::Receiver<bool>,
) {
    let mut attempt = 0usize;
    loop {
        if *stop.borrow() {
            return;
        }
        match connect_once(&state, &session, heartbeat_secs, &mut stop).await {
            Ok(()) => {
                // A clean close still means reconnecting: the session is
                // alive as long as it is registered.
                attempt = 0;
            }
            Err(e) => {
                tracing::warn!(error = %e, session = %session.session_id, "agent link dropped");
            }
        }
        *session.connected.lock().await = false;
        if *stop.borrow() {
            return;
        }

        let base = BACKOFF_SECS[attempt.min(BACKOFF_SECS.len() - 1)];
        attempt += 1;
        let delay = Duration::from_millis(base * 1000 + jitter_ms(base));
        tracing::debug!(seconds = delay.as_secs_f32(), "reconnecting");
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = stop.changed() => return,
        }
    }
}

/// Keep one session's presence alive while its WebSocket is not.
///
/// The heartbeat normally rides the connection, which is cheaper and proves
/// more. But a gateway rollout, or a laptop that changed networks, can leave
/// the socket down for longer than the TTL - and a developer who is sitting
/// there working would go offline for no reason anybody could see. So while
/// the link is down, the heartbeat goes over HTTP instead. Failures are not
/// worth reporting: if this cannot reach the gateway either, neither could
/// the reconnect, and the TTL lapsing is then the correct outcome.
async fn keep_alive(
    state: Arc<State>,
    session: Arc<Session>,
    heartbeat_secs: u64,
    mut stop: watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(heartbeat_secs));
    tick.tick().await;
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            _ = tick.tick() => {
                if *session.connected.lock().await {
                    continue;
                }
                match state.gateway.heartbeat(session.session_id).await {
                    Ok(_) => tracing::debug!("presence kept alive over HTTP while reconnecting"),
                    Err(e) => tracing::debug!(error = %e, "HTTP heartbeat failed"),
                }
            }
        }
    }
}

/// Up to a quarter of the delay, so a fleet does not reconnect in lockstep.
fn jitter_ms(base_secs: u64) -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % (base_secs * 250 + 1)
}

async fn connect_once(
    state: &Arc<State>,
    session: &Arc<Session>,
    heartbeat_secs: u64,
    stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    let mut request = state.gateway.websocket_url().into_client_request()?;
    request.headers_mut().insert(
        "authorization",
        state
            .gateway
            .authorization()
            .parse()
            .context("credential")?,
    );

    let (socket, _) = tokio_tungstenite::connect_async(request)
        .await
        .context("open the agent connection")?;
    let (mut sink, mut stream) = socket.split();

    send(
        &mut sink,
        ClientFrame::Connect {
            session_id: session.session_id,
            client_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
        },
    )
    .await?;
    *session.connected.lock().await = true;
    tracing::info!(session = %session.session_id, "connected");

    let mut heartbeat = tokio::time::interval(Duration::from_secs(heartbeat_secs));
    heartbeat.tick().await;

    loop {
        tokio::select! {
            _ = stop.changed() => return Ok(()),
            _ = heartbeat.tick() => {
                send(&mut sink, ClientFrame::Heartbeat).await?;
            }
            incoming = stream.next() => {
                let Some(message) = incoming else { return Ok(()) };
                match message.context("read from the agent connection")? {
                    WsMessage::Text(text) => {
                        match serde_json::from_str::<ServerFrame>(&text) {
                            Ok(ServerFrame::Message(m)) => {
                                tracing::debug!(id = %m.id, "message received");
                                session.push(m).await;
                            }
                            Ok(ServerFrame::Ping) => send(&mut sink, ClientFrame::Pong).await?,
                            Ok(ServerFrame::Connected { .. })
                            | Ok(ServerFrame::HeartbeatAck { .. }) => {}
                            Ok(ServerFrame::Error { message, fatal }) => {
                                tracing::warn!(%message, fatal, "gateway reported a problem");
                                if fatal {
                                    return Ok(());
                                }
                            }
                            Err(e) => tracing::debug!(error = %e, "unrecognised frame"),
                        }
                    }
                    WsMessage::Ping(payload) => sink.send(WsMessage::Pong(payload)).await?,
                    WsMessage::Close(_) => return Ok(()),
                    _ => {}
                }
            }
        }
    }
}

async fn send<S>(sink: &mut S, frame: ClientFrame) -> Result<()>
where
    S: SinkExt<WsMessage> + Unpin,
    <S as futures::Sink<WsMessage>>::Error: std::error::Error + Send + Sync + 'static,
{
    let text = serde_json::to_string(&frame)?;
    sink.send(WsMessage::Text(text.into()))
        .await
        .context("write to the agent connection")?;
    Ok(())
}

/// Answer whatever is asking on the local socket.
async fn serve_ipc(listener: UnixListener, state: Arc<State>, stop: watch::Sender<bool>) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let state = Arc::clone(&state);
        let stop = stop.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_ipc(stream, state, stop).await {
                tracing::debug!(error = %e, "local request failed");
            }
        });
    }
}

async fn handle_ipc(
    stream: UnixStream,
    state: Arc<State>,
    stop: watch::Sender<bool>,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();
    reader.read_line(&mut line).await?;

    let response = match serde_json::from_str::<Request>(&line) {
        Ok(request) => match dispatch(request, &state, &stop).await {
            Ok(value) => Response::Ok(value),
            Err(e) => Response::Error(format!("{e:#}")),
        },
        Err(e) => Response::Error(format!("unparseable request: {e}")),
    };

    write_half
        .write_all(serde_json::to_string(&response)?.as_bytes())
        .await?;
    write_half.write_all(b"\n").await?;
    write_half.flush().await?;
    Ok(())
}

/// The `cwd` a request carries, if it does.
fn cwd_of(body: &Value) -> Option<String> {
    body.get("cwd").and_then(Value::as_str).map(str::to_owned)
}

async fn dispatch(
    request: Request,
    state: &Arc<State>,
    stop: &watch::Sender<bool>,
) -> Result<Value> {
    match request {
        Request::Status => {
            let sessions = sessions_for(state, None).await;
            let mut listed = Vec::with_capacity(sessions.len());
            let mut pending = 0usize;
            let mut connected = 0usize;
            for s in &sessions {
                let mut view = s.describe();
                let up = *s.connected.lock().await;
                let waiting = s.inbox.lock().await.len();
                if up {
                    connected += 1;
                }
                pending += waiting;
                if let Value::Object(ref mut map) = view {
                    map.insert("connected".to_owned(), json!(up));
                    map.insert("pending_messages".to_owned(), json!(waiting));
                }
                listed.push(view);
            }
            Ok(json!({
                // The newest session's id, for callers that still expect one.
                "session_id": sessions.first().map(|s| s.session_id),
                "connected": !sessions.is_empty() && connected == sessions.len(),
                "sessions": listed,
                "pending_messages": pending,
                "started_at": state.started_at,
                "version": env!("CARGO_PKG_VERSION"),
            }))
        }

        Request::Register {
            client_session_id,
            client,
            cwd,
        } => {
            let session = register(state, client_session_id, client, PathBuf::from(cwd)).await?;
            Ok(session.describe())
        }

        Request::Unregister { client_session_id } => {
            let ended = unregister(state, &client_session_id).await?;
            Ok(json!({ "ended": ended }))
        }

        Request::Inbox { limit, cwd } => {
            let take = limit.unwrap_or(50);
            let mut taken: Vec<MessageView> = Vec::new();
            for session in sessions_for(state, cwd.as_deref()).await {
                // Poll as well as drain: a message for an agent connected to
                // another gateway replica is never pushed, only stored, and
                // this is where it is found.
                match state.gateway.inbox(session.session_id, take).await {
                    Ok(polled) => {
                        for message in polled {
                            session.push(message).await;
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "inbox poll failed; using what is buffered")
                    }
                }

                let drained: Vec<MessageView> = {
                    let mut inbox = session.inbox.lock().await;
                    let how_many = take.saturating_sub(taken.len()).min(inbox.len());
                    inbox.drain(..how_many).collect()
                };
                // Acknowledged only now: the gateway called it delivered when
                // the bytes went out, but it has only actually arrived once
                // something has asked for it.
                for message in &drained {
                    if let Err(e) = state
                        .gateway
                        .acknowledge(message.id, session.session_id)
                        .await
                    {
                        tracing::debug!(error = %e, "could not acknowledge a message");
                    }
                }
                taken.extend(drained);
                if taken.len() >= take {
                    break;
                }
            }
            Ok(json!({ "messages": taken }))
        }

        Request::Remember(mut body) => {
            let session = session_for(state, cwd_of(&body).as_deref()).await?;
            if let Value::Object(ref mut map) = body {
                map.remove("cwd");
                map.insert("session_id".to_owned(), json!(session.session_id));
            }
            state.gateway.remember(&body).await
        }

        Request::Search(body) => {
            let query: Vec<(&str, String)> = [
                "q",
                "project",
                "repository",
                "files",
                "technologies",
                "limit",
            ]
            .into_iter()
            .filter_map(|key| {
                let value = body.get(key)?;
                let text = match value {
                    Value::String(s) => s.clone(),
                    Value::Array(items) => items
                        .iter()
                        .filter_map(|i| i.as_str())
                        .collect::<Vec<_>>()
                        .join(","),
                    Value::Null => return None,
                    other => other.to_string(),
                };
                if text.is_empty() {
                    None
                } else {
                    Some((key, text))
                }
            })
            .collect();
            let memories = state.gateway.search(&query).await?;
            Ok(json!({ "memories": memories }))
        }

        Request::Message(mut body) => {
            let session = session_for(state, cwd_of(&body).as_deref()).await?;
            if let Value::Object(ref mut map) = body {
                map.remove("cwd");
                map.insert("session_id".to_owned(), json!(session.session_id));
            }
            state.gateway.send_message(&body).await
        }

        Request::Online => {
            let agents = state.gateway.online().await?;
            Ok(json!({ "agents": agents }))
        }

        Request::Shutdown => {
            let _ = stop.send(true);
            Ok(json!({ "stopping": true }))
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut sig) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            sig.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
