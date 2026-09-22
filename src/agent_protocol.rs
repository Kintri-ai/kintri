//! The agent network wire format, in one place.
//!
//! Both ends of every exchange are ours - `em-agent` serving, the `kintri`
//! daemon on a developer's machine calling - so these types exist to stop the
//! two drifting apart. They live in the shared crate rather than in the
//! gateway because a client binary should not have to compile a server
//! framework to learn what a frame looks like; both sides depend on this
//! module instead of re-declaring the shapes, which is the whole reason the
//! daemon is Rust and not a second language.
//!
//! The `kintri` client is not in this workspace any more - it lives in
//! <https://github.com/Kintri-ai/kintri> so a developer can install it without
//! access to the platform - and it compiles a VERBATIM copy of this file as
//! `src/agent_protocol.rs`, with the names imported from `crate::model` below
//! provided by its own `src/model.rs`. Change this file, copy it over, run
//! `make check-protocol`. Do not add a `sqlx` or server-only import here.
//!
//! Frames are JSON with a `type` tag. Not because JSON is fast, but because a
//! protocol a person can read in a log is one a person can debug at 23:00,
//! and this connection carries a few frames a minute, not a few thousand.

use crate::model::{AgentMessageId, AgentSessionId, MemoryType};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// REST
// ---------------------------------------------------------------------------

/// `POST /v1/sessions` - register, or re-register, one coding session.
#[derive(Debug, Clone, Deserialize)]
pub struct RegisterSession {
    /// Which agent this is. `claude-code` today.
    pub client: String,
    /// The client's own version string.
    #[serde(default)]
    pub client_version: Option<String>,
    /// The agent's own session id. Repeating it updates one row instead of
    /// creating a second presence for one Claude.
    pub client_session_id: String,
    /// Project name, as the developer's checkout knows it.
    #[serde(default)]
    pub project: Option<String>,
    /// Repository, ideally `owner/name`.
    #[serde(default)]
    pub repository: Option<String>,
    /// Branch.
    #[serde(default)]
    pub branch: Option<String>,
    /// Email from the local git config, for identity resolution.
    #[serde(default)]
    pub email: Option<String>,
    /// GitHub login, for identity resolution.
    #[serde(default)]
    pub github_login: Option<String>,
    /// Human name, used only as a display hint.
    #[serde(default)]
    pub display_name: Option<String>,
}

/// What the daemon needs to keep presence alive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRegistered {
    /// The platform's id for this session. Used everywhere afterwards.
    pub session_id: AgentSessionId,
    /// When presence lapses without a further heartbeat.
    pub expires_at: DateTime<Utc>,
    /// How often to heartbeat. Comfortably under the TTL, so one lost
    /// heartbeat does not take a live agent offline.
    pub heartbeat_interval_secs: u64,
}

/// `POST /v1/sessions/{id}/heartbeat`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// The new expiry.
    pub expires_at: DateTime<Utc>,
}

/// One other agent currently online, as `GET /v1/sessions` reports it.
///
/// Deliberately thin. This answers "who could I talk to", not "what is
/// everyone doing": there is no activity, no duration and no count here, and
/// there is not going to be one.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineAgent {
    /// The session to address a message to.
    pub session_id: AgentSessionId,
    /// Display name of the person behind it, when identity resolved one.
    pub developer: Option<String>,
    /// What they are working in.
    pub repository: Option<String>,
    /// Which branch.
    pub branch: Option<String>,
    /// When they were last heard from.
    pub last_seen_at: DateTime<Utc>,
}

/// `POST /v1/memories` - publish knowledge to the workspace.
#[derive(Debug, Clone, Deserialize)]
pub struct PublishMemory {
    /// What kind of knowledge this is.
    #[serde(rename = "type")]
    pub memory_type: MemoryType,
    /// The memory itself.
    pub content: String,
    /// The session publishing it, when the agent has one.
    #[serde(default)]
    pub session_id: Option<AgentSessionId>,
    /// Project it concerns.
    #[serde(default)]
    pub project: Option<String>,
    /// Repository it concerns.
    #[serde(default)]
    pub repository: Option<String>,
    /// File paths it concerns - paths only.
    #[serde(default)]
    pub files: Vec<String>,
    /// Technologies it concerns.
    #[serde(default)]
    pub technologies: Vec<String>,
    /// How sure the publisher is, 0.0 to 1.0. Defaults to 1.0.
    #[serde(default)]
    pub confidence: Option<f32>,
}

/// `GET /v1/memories/search` - what the workspace already knows about this.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SearchMemories {
    /// Free text. Ignored by the deterministic filter beyond substring
    /// matching; it is what the embedding will use once there is one.
    #[serde(default)]
    pub q: Option<String>,
    /// Narrow to a project.
    #[serde(default)]
    pub project: Option<String>,
    /// Narrow to a repository.
    #[serde(default)]
    pub repository: Option<String>,
    /// Files the asking agent is working on. The heaviest relevance signal.
    #[serde(default)]
    pub files: Option<String>,
    /// Technologies in play.
    #[serde(default)]
    pub technologies: Option<String>,
    /// How many to return.
    #[serde(default)]
    pub limit: Option<usize>,
}

impl SearchMemories {
    /// Split a comma-separated query parameter into trimmed, non-empty items.
    ///
    /// Repeated `?files=` parameters would be tidier, but a comma-separated
    /// list is what an MCP tool can build from a string without a query
    /// serializer, and this endpoint's clients are agents.
    pub fn split(raw: Option<&str>) -> Vec<String> {
        raw.map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
    }
}

/// One memory as an agent receives it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryView {
    /// Identifier, for confirming or superseding it later.
    pub id: String,
    /// What kind of knowledge it is.
    #[serde(rename = "type")]
    pub memory_type: MemoryType,
    /// The memory.
    pub content: String,
    /// Project it concerns.
    pub project: Option<String>,
    /// Repository it concerns.
    pub repository: Option<String>,
    /// Files it concerns.
    pub files: Vec<String>,
    /// Technologies it concerns.
    pub technologies: Vec<String>,
    /// Who published it, when identity knows and has not been erased.
    pub author: Option<String>,
    /// When it was published.
    pub created_at: DateTime<Utc>,
    /// Why it was returned: the score, and the signals that produced it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relevance: Option<Relevance>,
}

/// Why a memory was returned.
///
/// Returned with every hit, because an agent that cannot tell *why* something
/// surfaced will either trust all of it or none of it, and both are wrong.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Relevance {
    /// 0.0 to 1.0.
    pub score: f32,
    /// The signals that fired, in plain words.
    pub because: Vec<String>,
}

/// `POST /v1/messages` - tell another agent something now.
#[derive(Debug, Clone, Deserialize)]
pub struct SendMessage {
    /// The session to reach.
    pub target_session_id: AgentSessionId,
    /// The message.
    pub content: String,
    /// The sending session, when the agent has one.
    #[serde(default)]
    pub session_id: Option<AgentSessionId>,
}

/// One message as an agent receives it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageView {
    /// Identifier, to acknowledge with.
    pub id: AgentMessageId,
    /// The message.
    pub content: String,
    /// Who sent it, when identity knows.
    pub from: Option<String>,
    /// The sending session, so a reply can be addressed.
    pub from_session_id: Option<AgentSessionId>,
    /// When it was sent.
    pub created_at: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

/// A frame from a daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientFrame {
    /// Bind this connection to a session. Always the first frame.
    Connect {
        /// The session registered over REST.
        session_id: AgentSessionId,
        /// The daemon's version.
        #[serde(default)]
        client_version: Option<String>,
    },
    /// Keep presence alive. Cheaper than the REST call and on the same
    /// connection, so a daemon that can talk at all stays online.
    Heartbeat,
    /// Confirm a message arrived.
    Ack {
        /// The message being acknowledged.
        message_id: AgentMessageId,
    },
    /// Answer to a `Ping`.
    Pong,
}

/// A frame from the gateway.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerFrame {
    /// The connection is bound and presence is live.
    Connected {
        /// Echoed back so a daemon reconnecting can be sure which session it
        /// is on.
        session_id: AgentSessionId,
        /// How often to heartbeat.
        heartbeat_interval_secs: u64,
        /// The gateway's version, for a version-skew line in `kintri doctor`.
        server_version: String,
    },
    /// Presence extended.
    HeartbeatAck {
        /// The new expiry.
        expires_at: DateTime<Utc>,
    },
    /// A message for this agent. Goes to the local inbox, not to Claude.
    Message(MessageView),
    /// Liveness check for an idle connection.
    Ping,
    /// Something was wrong with the last frame. The connection stays open
    /// unless `fatal` is set: a daemon that sent one bad frame should not
    /// lose its presence over it.
    Error {
        /// What went wrong, safe to log.
        message: String,
        /// Whether the gateway is about to close the connection.
        fatal: bool,
    },
}
