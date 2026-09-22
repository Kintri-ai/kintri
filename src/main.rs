//! `kintri` - the agent network client.
//!
//! One binary with three jobs: a developer-facing CLI, the long-lived daemon,
//! and the MCP server Claude Code talks to. They are one binary because they
//! share the wire format and the credential handling, and because asking a
//! developer to install three things to try a product is how a product does
//! not get tried.
//!
//! Nothing in here reads a source file, a prompt or a transcript. What it
//! sends is what the subcommands say: a repository name, a branch, a session
//! id, and whatever an agent explicitly asked to publish.

#![forbid(unsafe_code)]
#![warn(clippy::all)]

// Verbatim copy of the platform's em_core::agent_protocol; see the file header.
// Its server-side request types are unused in a client on purpose.
#[allow(dead_code)]
mod agent_protocol;
mod client;
mod credentials;
mod daemon;
mod ipc;
mod login;
mod mcp;
mod model;
mod workspace;

use std::path::Path;
use std::process::ExitCode;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use credentials::{Credentials, FileStore, Store};
use serde_json::{json, Value};

#[derive(Parser)]
#[command(
    name = "kintri",
    version,
    about = "Share engineering knowledge between coding agents.",
    long_about = "Kintri connects ephemeral coding agents so that what one of them \
                  learns can reach the others.\n\nIt does not watch you work: no \
                  transcripts, no prompts, no commands, no timings. See \
                  https://github.com/Kintri-ai/kintri#what-it-sends."
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Connect this machine, through your browser.
    Login {
        /// Your Kintri address, e.g. https://kintri.example.com
        ///
        /// The webapp, not the gateway: this is the URL you already sign in
        /// to, and it tells the CLI where the gateway is.
        #[arg(long, env = "KINTRI_URL")]
        url: String,
        /// Skip the browser and use a token minted in Settings.
        ///
        /// For machines with no browser - a build agent, a container, an SSH
        /// session with no port forwarding. Everywhere else, leave it out: the
        /// browser flow means no credential is ever pasted, and the token it
        /// mints reaches the agent network and nothing else.
        #[arg(long, env = "KINTRI_TOKEN")]
        token: Option<String>,
        /// Where the gateway answers. Only needed with `--token`, and only if
        /// it is not the address the platform publishes.
        #[arg(long, env = "KINTRI_GATEWAY_URL")]
        gateway_url: Option<String>,
    },
    /// Forget the credential on this machine.
    Logout,
    /// Whether this machine is logged in and connected.
    Status,
    /// Check the things that usually turn out to be wrong.
    Doctor,
    /// Presence for one coding session.
    #[command(subcommand)]
    Session(SessionCommand),
    /// The background connection.
    #[command(subcommand)]
    Daemon(DaemonCommand),
    /// Publish a memory to the workspace.
    Remember {
        /// discovery | decision | warning | convention | architecture | bug | question
        #[arg(long = "type", default_value = "discovery")]
        memory_type: String,
        /// The memory itself.
        content: String,
        /// A file it is about. Repeatable.
        #[arg(long)]
        file: Vec<String>,
        /// A technology it is about. Repeatable.
        #[arg(long)]
        technology: Vec<String>,
    },
    /// Ask what the workspace already knows.
    Search {
        /// Free text.
        query: Option<String>,
        /// A file you are working on. Repeatable, and the strongest signal.
        #[arg(long)]
        file: Vec<String>,
        /// How many to return.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Show the messages waiting for this agent.
    Inbox {
        /// How many at most.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Who else is working right now.
    Online,
    /// Serve the four tools over MCP, on stdio. Claude Code runs this.
    Mcp,
}

#[derive(Subcommand)]
enum SessionCommand {
    /// Register presence and start the daemon if it is not running.
    Start {
        /// The agent's own session id.
        #[arg(long, env = "CLAUDE_SESSION_ID")]
        session_id: Option<String>,
        /// Which agent.
        #[arg(long, default_value = "claude-code")]
        client: String,
    },
    /// Report the session over. Best effort; the TTL is the real mechanism.
    End,
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Run in the foreground until stopped.
    Start {
        /// The agent's own session id.
        #[arg(long, env = "CLAUDE_SESSION_ID")]
        session_id: Option<String>,
        /// Which agent.
        #[arg(long, default_value = "claude-code")]
        client: String,
    },
    /// Ask a running daemon to stop.
    Stop,
    /// What a running daemon reports about itself.
    Status,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("kintri: {e}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("kintri: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let store = FileStore::discover()?;
    let config_dir = store.dir().to_path_buf();

    match cli.command {
        Command::Login {
            url,
            token,
            gateway_url,
        } => login(&store, url, token, gateway_url).await,
        Command::Logout => {
            store.clear()?;
            println!(
                "Logged out. The credential at {} is gone.",
                store.describe()
            );
            Ok(())
        }
        Command::Status => status(&store, &config_dir).await,
        Command::Doctor => doctor(&store, &config_dir).await,

        Command::Session(SessionCommand::Start { session_id, client }) => {
            // The hook runs this; it must never fail a Claude Code startup,
            // so a gateway that is unreachable is reported and shrugged off.
            if let Err(e) = start_daemon(&store, &config_dir, session_id, client).await {
                eprintln!("kintri: not connected ({e:#}). Claude Code continues as normal.");
            }
            Ok(())
        }
        Command::Session(SessionCommand::End) | Command::Daemon(DaemonCommand::Stop) => {
            match ipc::call(&config_dir, &ipc::Request::Shutdown).await {
                Ok(_) => Ok(()),
                // Nothing running is the desired end state anyway.
                Err(_) => Ok(()),
            }
        }
        Command::Daemon(DaemonCommand::Start { session_id, client }) => {
            tracing_subscriber::fmt()
                .with_env_filter(
                    std::env::var("KINTRI_LOG").unwrap_or_else(|_| "kintri=info".to_owned()),
                )
                .init();
            let credentials = require_login(&store)?;
            let options = daemon_options(&config_dir, session_id, client)?;
            daemon::run(credentials, options).await
        }
        Command::Daemon(DaemonCommand::Status) => {
            let value = ipc::call(&config_dir, &ipc::Request::Status).await?;
            println!("{}", serde_json::to_string_pretty(&value)?);
            Ok(())
        }

        Command::Remember {
            memory_type,
            content,
            file,
            technology,
        } => {
            let value = ipc::call(
                &config_dir,
                &ipc::Request::Remember(json!({
                    "type": memory_type,
                    "content": content,
                    "files": file,
                    "technologies": technology,
                })),
            )
            .await?;
            println!("Published. {}", value["id"].as_str().unwrap_or_default());
            Ok(())
        }
        Command::Search { query, file, limit } => {
            let value = ipc::call(
                &config_dir,
                &ipc::Request::Search(json!({
                    "q": query,
                    "files": file,
                    "limit": limit,
                })),
            )
            .await?;
            print_memories(&value);
            Ok(())
        }
        Command::Inbox { limit } => {
            let value = ipc::call(&config_dir, &ipc::Request::Inbox { limit }).await?;
            let messages = value["messages"].as_array().cloned().unwrap_or_default();
            if messages.is_empty() {
                println!("Nothing waiting.");
            }
            for message in messages {
                let from = message["from"].as_str().unwrap_or("another agent");
                println!(
                    "{from}: {}",
                    message["content"].as_str().unwrap_or_default()
                );
            }
            Ok(())
        }
        Command::Online => {
            let value = ipc::call(&config_dir, &ipc::Request::Online).await?;
            let agents = value["agents"].as_array().cloned().unwrap_or_default();
            if agents.is_empty() {
                println!("Nobody else is working right now.");
            }
            for agent in agents {
                println!(
                    "{}  {}  {}",
                    agent["developer"].as_str().unwrap_or("someone"),
                    agent["repository"].as_str().unwrap_or("-"),
                    agent["branch"].as_str().unwrap_or("-"),
                );
            }
            Ok(())
        }

        Command::Mcp => mcp::serve(config_dir).await,
    }
}

async fn login(
    store: &FileStore,
    url: String,
    token: Option<String>,
    gateway_url: Option<String>,
) -> Result<()> {
    let url = normalize_url(&url)?;

    let (gateway, token, workspace) = match token {
        // The escape hatch: a token minted in Settings, for a machine with no
        // browser. The gateway has to be named, because without the browser
        // flow nothing told us where it is.
        Some(token) => {
            let gateway = normalize_url(gateway_url.as_deref().unwrap_or(&url))?;
            (gateway, token, None)
        }
        None => {
            let granted = login::browser_login(&url).await?;
            let gateway = match (granted.gateway_url.as_deref(), gateway_url.as_deref()) {
                (Some(published), _) => normalize_url(published)?,
                (None, Some(given)) => normalize_url(given)?,
                (None, None) => {
                    return Err(anyhow!(
                        "this platform has not published an agent network address. \
                         Ask the operator to set EM_AGENT_PUBLIC_ENDPOINT, or pass \
                         --gateway-url yourself"
                    ))
                }
            };
            (gateway, granted.token, Some(granted.workspace))
        }
    };

    if !token.starts_with("emt_") {
        return Err(anyhow!(
            "that does not look like a workspace token; they start with `emt_`"
        ));
    }

    let credentials = Credentials {
        gateway_url: gateway,
        token,
    };
    // Proven before it is saved. A credential that turns out to be wrong at
    // the first SessionStart is a confusing failure hours later, in a hook
    // whose output nobody reads.
    let client = client::Gateway::new(&credentials)?;
    client
        .online()
        .await
        .context("the gateway did not accept this credential")?;

    store.save(&credentials)?;
    match workspace {
        Some(workspace) => println!(
            "Connected to {workspace}. This machine can now share with the team's other agents."
        ),
        None => println!("Logged in to {}.", credentials.gateway_url),
    }
    println!("Credential saved to {}.", store.describe());
    Ok(())
}

/// Accept `https://`, and `http://` only for a loopback address.
///
/// A token travels to this host. Letting it travel in clear text to anything
/// but the machine the developer is sitting at is not a convenience worth
/// having, and `--url http://…` is exactly the typo that would do it.
fn normalize_url(raw: &str) -> Result<String> {
    let url = raw.trim().trim_end_matches('/').to_owned();
    let is_local = url.starts_with("http://localhost")
        || url.starts_with("http://127.0.0.1")
        || url.starts_with("http://[::1]");
    if !url.starts_with("https://") && !is_local {
        return Err(anyhow!(
            "the URL must be https:// (http:// is allowed for localhost only): {url}"
        ));
    }
    Ok(url)
}

async fn status(store: &FileStore, config_dir: &std::path::Path) -> Result<()> {
    match store.load()? {
        None => {
            println!("Not logged in. Run `kintri login --url … --token …`.");
            return Ok(());
        }
        Some(c) => println!("Logged in to {} as {}", c.gateway_url, c.fingerprint()),
    }
    match ipc::call(config_dir, &ipc::Request::Status).await {
        Ok(value) => println!(
            "Daemon: {} · session {} · {} message(s) waiting",
            if value["connected"].as_bool().unwrap_or(false) {
                "connected"
            } else {
                "reconnecting"
            },
            value["session_id"].as_str().unwrap_or("-"),
            value["pending_messages"].as_u64().unwrap_or(0),
        ),
        Err(e) => println!("Daemon: not running ({e})"),
    }
    Ok(())
}

async fn doctor(store: &FileStore, config_dir: &std::path::Path) -> Result<()> {
    let checkout = workspace::inspect(&std::env::current_dir()?);
    println!("Config directory : {}", config_dir.display());
    println!("Credentials      : {}", store.describe());
    println!(
        "Repository       : {}",
        checkout
            .repository
            .as_deref()
            .unwrap_or("(not a git checkout with an origin)")
    );
    println!(
        "Branch           : {}",
        checkout.branch.as_deref().unwrap_or("-")
    );
    println!(
        "Git email        : {}",
        checkout
            .email
            .as_deref()
            .unwrap_or("(unset - you will not be identified)")
    );

    match store.load()? {
        None => println!("Login            : not logged in"),
        Some(c) => {
            println!("Gateway          : {}", c.gateway_url);
            match client::Gateway::new(&c) {
                Ok(g) => match g.online().await {
                    Ok(agents) => {
                        println!("Gateway reachable: yes, {} agent(s) online", agents.len())
                    }
                    Err(e) => println!("Gateway reachable: no ({e:#})"),
                },
                Err(e) => println!("Gateway reachable: no ({e:#})"),
            }
        }
    }
    match ipc::call(config_dir, &ipc::Request::Status).await {
        Ok(v) => println!(
            "Daemon           : running, version {}",
            v["version"].as_str().unwrap_or("?")
        ),
        Err(e) => println!("Daemon           : {e}"),
    }
    Ok(())
}

fn require_login(store: &FileStore) -> Result<Credentials> {
    store.load()?.ok_or_else(|| {
        anyhow!("not logged in on this machine. Run `kintri login --url … --token …`")
    })
}

fn daemon_options(
    config_dir: &std::path::Path,
    session_id: Option<String>,
    client: String,
) -> Result<daemon::Options> {
    let cwd = std::env::current_dir()?;
    let checkout = workspace::inspect(&cwd);
    Ok(daemon::Options {
        config_dir: config_dir.to_path_buf(),
        // No session id means this is not a hook-started daemon; the process
        // id keeps two manual daemons on one machine apart.
        client_session_id: session_id.unwrap_or_else(|| format!("local-{}", std::process::id())),
        client,
        github_login: std::env::var("KINTRI_GITHUB_LOGIN").ok(),
        display_name: std::env::var("KINTRI_DISPLAY_NAME").ok(),
        checkout,
    })
}

/// Start a daemon in the background, unless one is already answering.
async fn start_daemon(
    store: &FileStore,
    config_dir: &Path,
    session_id: Option<String>,
    client: String,
) -> Result<()> {
    require_login(store)?;
    if ipc::call(config_dir, &ipc::Request::Status).await.is_ok() {
        return Ok(());
    }

    let exe = std::env::current_exe().context("find this binary")?;
    let mut command = std::process::Command::new(exe);
    command
        .arg("daemon")
        .arg("start")
        .arg("--client")
        .arg(client);
    if let Some(id) = session_id {
        command.arg("--session-id").arg(id);
    }
    command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    command.spawn().context("start the kintri daemon")?;

    // Give it long enough to bind its socket, so that the MCP server starting
    // a moment later finds it. Not a correctness requirement: every tool
    // degrades to "daemon not running" rather than failing.
    for _ in 0..20 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        if ipc::call(config_dir, &ipc::Request::Status).await.is_ok() {
            return Ok(());
        }
    }
    Err(anyhow!("the daemon did not come up within two seconds"))
}

fn print_memories(value: &Value) {
    let memories = value["memories"].as_array().cloned().unwrap_or_default();
    if memories.is_empty() {
        println!("Nothing relevant. That is an answer, not a failure.");
        return;
    }
    for memory in memories {
        let score = memory["relevance"]["score"].as_f64().unwrap_or(0.0);
        println!(
            "[{:>4.2}] {:<12} {}",
            score,
            memory["type"].as_str().unwrap_or("-"),
            memory["content"].as_str().unwrap_or_default()
        );
        if let Some(because) = memory["relevance"]["because"].as_array() {
            let reasons: Vec<&str> = because.iter().filter_map(|b| b.as_str()).collect();
            if !reasons.is_empty() {
                println!("         why: {}", reasons.join("; "));
            }
        }
    }
}
