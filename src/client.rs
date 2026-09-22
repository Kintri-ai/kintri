//! Talking to the gateway.
//!
//! Every call returns a typed error the caller can decide about, and no call
//! panics or exits: this library is used by the daemon, by the CLI and by the
//! MCP server, and in the MCP case a failure has to become a message Claude
//! can shrug at rather than something that takes a developer's session with
//! it.

use crate::agent_protocol::{Heartbeat, MemoryView, MessageView, OnlineAgent, SessionRegistered};
use crate::model::{AgentMessageId, AgentSessionId};
use anyhow::{anyhow, Context, Result};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use serde_json::{json, Value};

use crate::credentials::Credentials;

/// An HTTP client bound to one gateway and one credential.
pub struct Gateway {
    http: Client,
    base: String,
    authorization: String,
}

impl Gateway {
    /// Build a client. Fails only if TLS cannot be initialised.
    pub fn new(credentials: &Credentials) -> Result<Self> {
        let http = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .user_agent(concat!("kintri/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("build HTTP client")?;
        Ok(Gateway {
            http,
            base: credentials.gateway_url.trim_end_matches('/').to_owned(),
            authorization: credentials.header(),
        })
    }

    /// The WebSocket URL for this gateway: `https` becomes `wss`.
    pub fn websocket_url(&self) -> String {
        let base = self
            .base
            .replacen("https://", "wss://", 1)
            .replacen("http://", "ws://", 1);
        format!("{base}/v1/agent")
    }

    /// The `Authorization` header value, for the WebSocket handshake.
    pub fn authorization(&self) -> &str {
        &self.authorization
    }

    async fn post<B: Serialize>(&self, path: &str, body: &B) -> Result<Value> {
        let response = self
            .http
            .post(format!("{}{path}", self.base))
            .header(reqwest::header::AUTHORIZATION, &self.authorization)
            .json(body)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;
        decode(response, path).await
    }

    async fn get(&self, path: &str, query: &[(&str, String)]) -> Result<Value> {
        let response = self
            .http
            .get(format!("{}{path}", self.base))
            .header(reqwest::header::AUTHORIZATION, &self.authorization)
            .query(query)
            .send()
            .await
            .with_context(|| format!("GET {path}"))?;
        decode(response, path).await
    }

    /// Register, or refresh, this agent's presence.
    pub async fn register_session(&self, body: &Value) -> Result<SessionRegistered> {
        let value = self.post("/v1/sessions", body).await?;
        serde_json::from_value(value).context("decode session registration")
    }

    /// Push the expiry forward.
    pub async fn heartbeat(&self, session_id: AgentSessionId) -> Result<Heartbeat> {
        let value = self
            .post(&format!("/v1/sessions/{session_id}/heartbeat"), &json!({}))
            .await?;
        serde_json::from_value(value).context("decode heartbeat")
    }

    /// Report the session over. Best effort - the TTL is the real mechanism.
    pub async fn end_session(&self, session_id: AgentSessionId) -> Result<()> {
        self.post(&format!("/v1/sessions/{session_id}/end"), &json!({}))
            .await?;
        Ok(())
    }

    /// Who else is online in this workspace.
    pub async fn online(&self) -> Result<Vec<OnlineAgent>> {
        let value = self.get("/v1/sessions", &[]).await?;
        serde_json::from_value(value["agents"].clone()).context("decode online agents")
    }

    /// Publish a memory.
    pub async fn remember(&self, body: &Value) -> Result<Value> {
        self.post("/v1/memories", body).await
    }

    /// Search the workspace's memories.
    pub async fn search(&self, query: &[(&str, String)]) -> Result<Vec<MemoryView>> {
        let value = self.get("/v1/memories/search", query).await?;
        serde_json::from_value(value["memories"].clone()).context("decode memories")
    }

    /// Message another live agent.
    pub async fn send_message(&self, body: &Value) -> Result<Value> {
        self.post("/v1/messages", body).await
    }

    /// Take what a session is owed.
    pub async fn inbox(
        &self,
        session_id: AgentSessionId,
        limit: usize,
    ) -> Result<Vec<MessageView>> {
        let value = self
            .get(
                "/v1/messages/inbox",
                &[
                    ("session_id", session_id.to_string()),
                    ("limit", limit.to_string()),
                ],
            )
            .await?;
        serde_json::from_value(value["messages"].clone()).context("decode messages")
    }

    /// Confirm a message arrived.
    pub async fn acknowledge(
        &self,
        message_id: AgentMessageId,
        session_id: AgentSessionId,
    ) -> Result<()> {
        self.post(
            &format!("/v1/messages/{message_id}/ack"),
            &json!({ "session_id": session_id }),
        )
        .await?;
        Ok(())
    }
}

/// Turn a response into JSON, or into an error a person can act on.
async fn decode(response: reqwest::Response, path: &str) -> Result<Value> {
    let status = response.status();
    if status == StatusCode::NO_CONTENT {
        return Ok(Value::Null);
    }
    let body = response.text().await.unwrap_or_default();
    if status.is_success() {
        if body.trim().is_empty() {
            return Ok(Value::Null);
        }
        return serde_json::from_str(&body)
            .with_context(|| format!("{path} returned a body that is not JSON"));
    }

    // The gateway's own `{"error": "..."}` if there is one, the status if not.
    let detail = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|v| v["error"].as_str().map(str::to_owned))
        .unwrap_or_else(|| status.to_string());
    Err(match status {
        StatusCode::UNAUTHORIZED => anyhow!(
            "the gateway rejected this credential. Run `kintri login` again; \
             the token may have been revoked"
        ),
        StatusCode::NOT_FOUND => anyhow!("{detail}"),
        _ => anyhow!("{path}: {detail}"),
    })
}
