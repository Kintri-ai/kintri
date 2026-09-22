//! The long-lived half of the client.
//!
//! One process per developer machine. It owns the credential, the session, the
//! WebSocket and the local inbox, and it is the only thing here that talks to
//! the network - so the MCP server, the hooks and the CLI all stay credential
//! -free and the token never appears in a subprocess environment.
//!
//! ```text
//! MCP / CLI  --unix socket-->  daemon  --wss-->  gateway
//! ```
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

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::agent_protocol::{ClientFrame, MessageView, ServerFrame};
use crate::model::AgentSessionId;
use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message as WsMessage;

use crate::client::Gateway;
use crate::credentials::Credentials;
use crate::ipc::{socket_path, Request, Response};
use crate::workspace::Checkout;

/// Reconnect delays, in seconds, then repeat the last one.
///
/// A laptop that closed its lid comes back to a gateway that has not moved,
/// and a gateway that is being deployed comes back within a minute. Jitter is
/// added so that a fleet of daemons does not reconnect in lockstep and do to
/// the gateway what the outage did.
const BACKOFF_SECS: [u64; 6] = [1, 2, 5, 10, 30, 60];

/// How many messages the local inbox holds before the oldest is dropped.
///
/// Bounded on purpose: a Claude that never calls `kintri_inbox` must not turn
/// this process into a memory leak on somebody's laptop.
const INBOX_CAPACITY: usize = 500;

/// Everything the daemon shares between its tasks.
struct State {
    gateway: Gateway,
    session_id: AgentSessionId,
    inbox: Mutex<VecDeque<MessageView>>,
    connected: Mutex<bool>,
    started_at: chrono::DateTime<chrono::Utc>,
}

impl State {
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
}

/// What `kintri daemon start` was told.
pub struct Options {
    /// Where credentials and the socket live.
    pub config_dir: PathBuf,
    /// The agent's own session id, from the Claude Code hook.
    pub client_session_id: String,
    /// Which agent.
    pub client: String,
    /// The checkout it is working in.
    pub checkout: Checkout,
    /// GitHub login, if the developer configured one.
    pub github_login: Option<String>,
    /// Display name, used only as a hint.
    pub display_name: Option<String>,
}

/// Run until the process is told to stop.
pub async fn run(credentials: Credentials, options: Options) -> Result<()> {
    let gateway = Gateway::new(&credentials)?;

    let registration = gateway
        .register_session(&json!({
            "client": options.client,
            "client_version": env!("CARGO_PKG_VERSION"),
            "client_session_id": options.client_session_id,
            "project": options.checkout.project,
            "repository": options.checkout.repository,
            "branch": options.checkout.branch,
            "email": options.checkout.email,
            "github_login": options.github_login,
            "display_name": options.display_name,
        }))
        .await
        .context("register this session with the gateway")?;

    tracing::info!(
        session_id = %registration.session_id,
        heartbeat_secs = registration.heartbeat_interval_secs,
        "registered"
    );

    let state = Arc::new(State {
        gateway,
        session_id: registration.session_id,
        inbox: Mutex::new(VecDeque::new()),
        connected: Mutex::new(false),
        started_at: chrono::Utc::now(),
    });

    let listener = bind(&options.config_dir)?;
    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);

    let ipc = tokio::spawn(serve_ipc(listener, Arc::clone(&state), stop_tx.clone()));
    let heartbeat_secs = registration.heartbeat_interval_secs.max(1);
    let link = tokio::spawn(maintain_link(
        Arc::clone(&state),
        heartbeat_secs,
        stop_rx.clone(),
    ));
    let keep_alive = tokio::spawn(keep_alive(
        Arc::clone(&state),
        heartbeat_secs,
        stop_rx.clone(),
    ));

    tokio::select! {
        _ = stop_rx.changed() => {}
        _ = shutdown_signal() => {
            let _ = stop_tx.send(true);
        }
    }

    ipc.abort();
    link.abort();
    keep_alive.abort();
    let _ = std::fs::remove_file(socket_path(&options.config_dir));

    // Best effort, and deliberately not retried: the TTL is what actually
    // takes this session offline, and an end that never arrives is the case
    // the whole design is built around.
    if let Err(e) = state.gateway.end_session(state.session_id).await {
        tracing::debug!(error = %e, "could not report the end of the session");
    }
    Ok(())
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

/// Keep the WebSocket up, and the session alive.
async fn maintain_link(
    state: Arc<State>,
    heartbeat_secs: u64,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut attempt = 0usize;
    loop {
        if *stop.borrow() {
            return;
        }
        match connect_once(&state, heartbeat_secs, &mut stop).await {
            Ok(()) => {
                // A clean close still means reconnecting: the session is
                // alive as long as this process is.
                attempt = 0;
            }
            Err(e) => {
                tracing::warn!(error = %e, "agent link dropped");
            }
        }
        *state.connected.lock().await = false;
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

/// Keep presence alive while the WebSocket is not.
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
    heartbeat_secs: u64,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut tick = tokio::time::interval(Duration::from_secs(heartbeat_secs));
    tick.tick().await;
    loop {
        tokio::select! {
            _ = stop.changed() => return,
            _ = tick.tick() => {
                if *state.connected.lock().await {
                    continue;
                }
                match state.gateway.heartbeat(state.session_id).await {
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
    heartbeat_secs: u64,
    stop: &mut tokio::sync::watch::Receiver<bool>,
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
            session_id: state.session_id,
            client_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
        },
    )
    .await?;
    *state.connected.lock().await = true;
    tracing::info!("connected");

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
                                state.push(m).await;
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
async fn serve_ipc(
    listener: UnixListener,
    state: Arc<State>,
    stop: tokio::sync::watch::Sender<bool>,
) {
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
    stop: tokio::sync::watch::Sender<bool>,
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

async fn dispatch(
    request: Request,
    state: &Arc<State>,
    stop: &tokio::sync::watch::Sender<bool>,
) -> Result<Value> {
    match request {
        Request::Status => Ok(json!({
            "session_id": state.session_id,
            "connected": *state.connected.lock().await,
            "pending_messages": state.inbox.lock().await.len(),
            "started_at": state.started_at,
            "version": env!("CARGO_PKG_VERSION"),
        })),

        Request::Inbox { limit } => {
            // Poll as well as drain: a message for an agent connected to
            // another gateway replica is never pushed, only stored, and this
            // is where it is found.
            match state
                .gateway
                .inbox(state.session_id, limit.unwrap_or(50))
                .await
            {
                Ok(polled) => {
                    for message in polled {
                        state.push(message).await;
                    }
                }
                Err(e) => tracing::debug!(error = %e, "inbox poll failed; using what is buffered"),
            }

            let take = limit.unwrap_or(50);
            let taken: Vec<MessageView> = {
                let mut inbox = state.inbox.lock().await;
                let how_many = take.min(inbox.len());
                inbox.drain(..how_many).collect()
            };

            // Acknowledged only now: the gateway called it delivered when the
            // bytes went out, but it has only actually arrived once something
            // has asked for it.
            for message in &taken {
                if let Err(e) = state
                    .gateway
                    .acknowledge(message.id, state.session_id)
                    .await
                {
                    tracing::debug!(error = %e, "could not acknowledge a message");
                }
            }
            Ok(json!({ "messages": taken }))
        }

        Request::Remember(mut body) => {
            if let Value::Object(ref mut map) = body {
                map.insert("session_id".to_owned(), json!(state.session_id));
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
            if let Value::Object(ref mut map) = body {
                map.insert("session_id".to_owned(), json!(state.session_id));
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
