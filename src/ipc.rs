//! The local protocol between the daemon and everything else on the machine.
//!
//! A Unix socket in the user's config directory, line-delimited JSON, one
//! request and one response per connection. No local TCP port is opened: a
//! port would be reachable by every process on the machine *and* by anything
//! that can talk to localhost, and the socket's file permissions are a real
//! boundary that a port number is not.
//!
//! The daemon holds the credential. Nothing else does - not the MCP server,
//! not the hooks - which is what keeps the token out of a subprocess
//! environment where a `ps` would show it.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

/// What somebody wants from the daemon.
///
/// The daemon holds one presence per Claude Code session, not one per
/// machine, so a request that acts *as* a session (`Remember`, `Message`,
/// `Inbox`) says where it comes from: the MCP server passes its working
/// directory as `cwd`, and the daemon answers as the session registered from
/// there. Claude Code does not tell an MCP server which session started it;
/// the directory is the one fact the hook and the server share.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// Is it connected, and as whom.
    Status,
    /// Register one Claude Code session with the gateway. Idempotent on
    /// `client_session_id`: a retried hook refreshes rather than duplicates.
    Register {
        client_session_id: String,
        client: String,
        cwd: String,
    },
    /// Report one session over. The others carry on.
    Unregister { client_session_id: String },
    /// Take the messages this agent has not seen.
    Inbox {
        /// How many at most.
        #[serde(default)]
        limit: Option<usize>,
        /// The asking process's directory, to pick the session; none means all.
        #[serde(default)]
        cwd: Option<String>,
    },
    /// Publish a memory. The body is passed through to the gateway.
    Remember(Value),
    /// Search memories. The map becomes the query string.
    Search(Value),
    /// Message another agent.
    Message(Value),
    /// Who else is online.
    Online,
    /// Stop the daemon.
    Shutdown,
}

/// What the daemon answers.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Response {
    /// It worked.
    Ok(Value),
    /// It did not, and here is a sentence saying why.
    Error(String),
}

impl Response {
    /// Unwrap into a `Result`, so callers handle failure the usual way.
    pub fn into_result(self) -> Result<Value> {
        match self {
            Response::Ok(v) => Ok(v),
            Response::Error(e) => Err(anyhow!(e)),
        }
    }
}

/// Where the daemon listens.
pub fn socket_path(config_dir: &Path) -> PathBuf {
    config_dir.join("daemon.sock")
}

/// Send one request to a running daemon.
///
/// A missing socket is reported as a plain "not running", because that is the
/// normal state on a machine where nobody started it, and every caller of
/// this - the CLI, the MCP server - has to degrade rather than fail loudly.
pub async fn call(config_dir: &Path, request: &Request) -> Result<Value> {
    let path = socket_path(config_dir);
    let stream = match UnixStream::connect(&path).await {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(anyhow!(
                "the kintri daemon is not running. Start it with `kintri daemon start`"
            ))
        }
        Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
            // A socket file left behind by a daemon that died.
            return Err(anyhow!(
                "the kintri daemon is not running (stale socket at {}). \
                 Start it with `kintri daemon start`",
                path.display()
            ));
        }
        Err(e) => return Err(e).context("connect to the kintri daemon"),
    };

    let (read_half, mut write_half) = stream.into_split();
    let line = serde_json::to_string(request)?;
    write_half.write_all(line.as_bytes()).await?;
    write_half.write_all(b"\n").await?;
    write_half.flush().await?;

    let mut reader = BufReader::new(read_half);
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .context("read the daemon's answer")?;
    if response.trim().is_empty() {
        return Err(anyhow!(
            "the daemon closed the connection without answering"
        ));
    }
    serde_json::from_str::<Response>(&response)
        .context("the daemon's answer was not understood")?
        .into_result()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_round_trip_as_tagged_json() {
        let encoded = serde_json::to_string(&Request::Inbox {
            limit: Some(10),
            cwd: None,
        })
        .unwrap();
        assert!(encoded.contains("\"op\":\"inbox\""));
        let decoded: Request = serde_json::from_str(&encoded).unwrap();
        assert!(matches!(
            decoded,
            Request::Inbox {
                limit: Some(10),
                cwd: None
            }
        ));
    }

    #[test]
    fn an_old_inbox_request_without_cwd_still_parses() {
        let decoded: Request = serde_json::from_str(r#"{"op":"inbox","limit":5}"#).unwrap();
        assert!(matches!(
            decoded,
            Request::Inbox {
                limit: Some(5),
                cwd: None
            }
        ));
    }

    #[test]
    fn an_error_response_becomes_an_error() {
        let r = Response::Error("no".to_owned());
        assert!(r.into_result().is_err());
    }
}
