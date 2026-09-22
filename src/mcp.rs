//! The four tools, served over MCP on stdio.
//!
//! Claude Code starts this process and speaks JSON-RPC 2.0 over stdin and
//! stdout. It holds no credential: every call becomes one request to the local
//! daemon, which is the only thing on the machine that talks to the network.
//!
//! # Why four tools and not fourteen
//!
//! Every tool definition costs context in every session that has the server
//! installed, whether or not it is used. Four is what the product needs:
//! publish, retrieve, send, receive. A `kintri_list_memory_types` would be
//! free to write and would be paid for by every developer, forever.
//!
//! # Why failure is quiet
//!
//! A tool error here must be something Claude can shrug at. If the daemon is
//! not running, the answer is a sentence saying so - not an exception, not a
//! retry loop, and never anything that stops the session. Kintri being down
//! is not a reason somebody cannot code.

use std::path::{Path, PathBuf};

use anyhow::Result;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::ipc::{self, Request};

/// The MCP revision this server implements.
const PROTOCOL_VERSION: &str = "2024-11-05";

/// Read requests from stdin until it closes.
pub async fn serve(config_dir: PathBuf) -> Result<()> {
    let mut stdin = BufReader::new(tokio::io::stdin()).lines();
    let mut stdout = tokio::io::stdout();

    while let Some(line) = stdin.next_line().await? {
        if line.trim().is_empty() {
            continue;
        }
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        // A notification has no id and takes no answer.
        let Some(id) = request.get("id").cloned() else {
            continue;
        };
        let method = request["method"].as_str().unwrap_or_default();
        let params = request["params"].clone();

        let response = match handle(method, params, &config_dir).await {
            Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
            Err(e) => json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32603, "message": format!("{e:#}") }
            }),
        };
        stdout
            .write_all(serde_json::to_string(&response)?.as_bytes())
            .await?;
        stdout.write_all(b"\n").await?;
        stdout.flush().await?;
    }
    Ok(())
}

async fn handle(method: &str, params: Value, config_dir: &Path) -> Result<Value> {
    match method {
        "initialize" => Ok(json!({
            "protocolVersion": PROTOCOL_VERSION,
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "kintri", "version": env!("CARGO_PKG_VERSION") }
        })),
        "tools/list" => Ok(json!({ "tools": tools() })),
        "tools/call" => call_tool(params, config_dir).await,
        // ping, and anything else with an id, gets an empty result rather
        // than an error: an unknown method is not worth a red mark in a
        // client's log.
        _ => Ok(json!({})),
    }
}

async fn call_tool(params: Value, config_dir: &Path) -> Result<Value> {
    let name = params["name"].as_str().unwrap_or_default();
    let args = params["arguments"].clone();
    // Claude Code does not tell an MCP server which session started it; the
    // working directory is how the daemon finds the right one.
    let cwd = std::env::current_dir()
        .map(|d| d.to_string_lossy().into_owned())
        .ok();

    let request = match name {
        "kintri_remember" => Request::Remember(json!({
            "type": args["type"].as_str().unwrap_or("discovery"),
            "content": args["content"],
            "project": args.get("project").cloned().unwrap_or(Value::Null),
            "repository": args.get("repository").cloned().unwrap_or(Value::Null),
            "files": args.get("files").cloned().unwrap_or(json!([])),
            "technologies": args.get("technologies").cloned().unwrap_or(json!([])),
            "cwd": cwd,
        })),
        "kintri_search" => Request::Search(json!({
            "q": args.get("query").cloned().unwrap_or(Value::Null),
            "project": args.get("project").cloned().unwrap_or(Value::Null),
            "repository": args.get("repository").cloned().unwrap_or(Value::Null),
            "files": args.get("files").cloned().unwrap_or(Value::Null),
            "technologies": args.get("technologies").cloned().unwrap_or(Value::Null),
            "limit": args.get("limit").cloned().unwrap_or(Value::Null),
        })),
        "kintri_message" => Request::Message(json!({
            "target_session_id": args["target_session_id"],
            "content": args["content"],
            "cwd": cwd,
        })),
        "kintri_inbox" => Request::Inbox {
            limit: args
                .get("limit")
                .and_then(|l| l.as_u64())
                .map(|l| l as usize),
            cwd,
        },
        other => return Ok(tool_error(format!("no such tool: {other}"))),
    };

    match ipc::call(config_dir, &request).await {
        Ok(value) => Ok(json!({
            "content": [{ "type": "text", "text": render(name, &value) }]
        })),
        // Not a protocol error: the session continues, and Claude is told
        // plainly that the network is unavailable.
        Err(e) => Ok(tool_error(format!("{e:#}"))),
    }
}

fn tool_error(message: String) -> Value {
    json!({
        "content": [{ "type": "text", "text": message }],
        "isError": true
    })
}

/// Turn the daemon's JSON into something worth spending an agent's context on.
fn render(tool: &str, value: &Value) -> String {
    match tool {
        "kintri_search" => {
            let memories = value["memories"].as_array().cloned().unwrap_or_default();
            if memories.is_empty() {
                // Said plainly, because an agent that reads "no results" as
                // "the tool failed" will start working around it.
                return "Nothing the team has published is relevant to this. That is \
                        an answer, not a failure."
                    .to_owned();
            }
            let mut out = String::new();
            for memory in memories {
                let reasons: Vec<&str> = memory["relevance"]["because"]
                    .as_array()
                    .map(|b| b.iter().filter_map(|r| r.as_str()).collect())
                    .unwrap_or_default();
                out.push_str(&format!(
                    "[{}] {}\n  by {} · {}\n",
                    memory["type"].as_str().unwrap_or("-"),
                    memory["content"].as_str().unwrap_or_default(),
                    memory["author"].as_str().unwrap_or("a colleague's agent"),
                    if reasons.is_empty() {
                        "recent".to_owned()
                    } else {
                        reasons.join("; ")
                    }
                ));
            }
            out
        }
        "kintri_inbox" => {
            let messages = value["messages"].as_array().cloned().unwrap_or_default();
            if messages.is_empty() {
                return "No messages.".to_owned();
            }
            messages
                .iter()
                .map(|m| {
                    format!(
                        "{}: {}",
                        m["from"].as_str().unwrap_or("another agent"),
                        m["content"].as_str().unwrap_or_default()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
        "kintri_remember" => "Published to the team.".to_owned(),
        "kintri_message" => {
            if value["pushed"].as_bool().unwrap_or(false) {
                "Delivered.".to_owned()
            } else {
                "Queued; their agent will see it when it next checks.".to_owned()
            }
        }
        _ => value.to_string(),
    }
}

/// The tool definitions.
///
/// The descriptions are written for the model, not for a docs page: they say
/// *when* to reach for the tool, because a tool an agent does not know when to
/// use is one it will either never call or call on everything.
fn tools() -> Value {
    json!([
        {
            "name": "kintri_remember",
            "description": "Publish something you have learned to the rest of the team's \
                            agents, so the next person does not rediscover it. Use it when \
                            you find out something non-obvious that cost you time: a \
                            provider that behaves unexpectedly, a decision the codebase \
                            encodes, a convention, a trap. Do not use it for things already \
                            written in the code or the README, and never paste conversation \
                            or file contents - one or two sentences a colleague could have \
                            said.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["discovery", "decision", "warning", "convention",
                                 "architecture", "bug", "question"],
                        "description": "warning and decision are what other agents must not \
                                        work around unknowingly."
                    },
                    "content": { "type": "string", "description": "One or two sentences." },
                    "files": {
                        "type": "array", "items": { "type": "string" },
                        "description": "Paths this is about. The strongest signal for \
                                        reaching the right agent - include them when you know them."
                    },
                    "technologies": { "type": "array", "items": { "type": "string" } },
                    "project": { "type": "string" },
                    "repository": { "type": "string", "description": "owner/name" }
                },
                "required": ["type", "content"]
            }
        },
        {
            "name": "kintri_search",
            "description": "Ask what the team's other agents have already found out about \
                            what you are working on. Worth calling before you start on an \
                            unfamiliar file, when something behaves surprisingly, and before \
                            integrating an external provider. Pass the files you are actually \
                            touching: relevance is decided mostly by file overlap. An empty \
                            answer means nobody has published anything relevant - it is a \
                            real answer, not a failure.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "What you want to know." },
                    "files": {
                        "type": "array", "items": { "type": "string" },
                        "description": "Files you are working on."
                    },
                    "technologies": { "type": "array", "items": { "type": "string" } },
                    "project": { "type": "string" },
                    "repository": { "type": "string", "description": "owner/name" },
                    "limit": { "type": "integer" }
                }
            }
        },
        {
            "name": "kintri_message",
            "description": "Tell another agent that is working right now something it needs \
                            to know in the next few minutes - typically that you have just \
                            changed something under it. For knowledge worth keeping, use \
                            kintri_remember instead: messages expire, memories do not.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target_session_id": {
                        "type": "string",
                        "description": "The session to reach."
                    },
                    "content": { "type": "string" }
                },
                "required": ["target_session_id", "content"]
            }
        },
        {
            "name": "kintri_inbox",
            "description": "Read messages other agents have sent to this session. Check when \
                            you start a task and when you come back from a long operation. \
                            Only raise what materially affects what the user is doing - a \
                            message about a file nobody here is touching is not worth \
                            interrupting them for.",
            "inputSchema": {
                "type": "object",
                "properties": { "limit": { "type": "integer" } }
            }
        }
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initialize_advertises_tools_and_a_protocol_version() {
        let dir = PathBuf::from("/nonexistent");
        let result = handle("initialize", Value::Null, &dir).await.unwrap();
        assert_eq!(result["protocolVersion"], PROTOCOL_VERSION);
        assert!(result["capabilities"]["tools"].is_object());
    }

    #[test]
    fn exactly_the_four_tools_the_product_needs_are_offered() {
        let names: Vec<String> = tools()
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_owned())
            .collect();
        assert_eq!(
            names,
            vec![
                "kintri_remember",
                "kintri_search",
                "kintri_message",
                "kintri_inbox"
            ]
        );
    }

    /// A daemon that is not running is a sentence, never a crash: the whole
    /// point is that Kintri cannot take a coding session down with it.
    #[tokio::test]
    async fn a_missing_daemon_is_a_tool_error_not_a_protocol_error() {
        let dir = PathBuf::from("/nonexistent-kintri-dir");
        let result = handle(
            "tools/call",
            json!({ "name": "kintri_inbox", "arguments": {} }),
            &dir,
        )
        .await
        .expect("the call itself must succeed");
        assert_eq!(result["isError"], true);
        assert!(result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("daemon"));
    }

    #[test]
    fn an_empty_search_says_so_in_words_an_agent_will_not_misread() {
        let rendered = render("kintri_search", &json!({ "memories": [] }));
        assert!(rendered.contains("not a failure"));
    }

    #[test]
    fn an_unknown_tool_is_reported_without_taking_the_session_down() {
        let rendered = tool_error("no such tool: kintri_nope".to_owned());
        assert_eq!(rendered["isError"], true);
    }
}
