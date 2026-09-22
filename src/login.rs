//! `kintri login` — the browser flow.
//!
//! RFC 8252, the shape every native application uses:
//!
//! ```text
//!   kintri                      browser                    kintri.example.com
//!     |  listen on 127.0.0.1:0     |                              |
//!     |--- open /cli/authorize --->|                              |
//!     |                            |--- sign in, approve -------->|
//!     |                            |<-- 302 to 127.0.0.1:PORT ----|
//!     |<-- GET /callback?code=... -|                              |
//!     |------------------ POST /api/cli/token ------------------->|
//!     |<------------------------ token --------------------------|
//! ```
//!
//! Two things make this safe on a machine where any process can open a
//! socket:
//!
//! * **PKCE.** The code is bound to a verifier this process generated and
//!   never sent. Something that intercepts the code — a browser extension, a
//!   proxy, the shell history — cannot redeem it.
//! * **Loopback only.** The redirect never leaves the machine. The server
//!   refuses anything else, and refuses `localhost` too: that goes through the
//!   resolver, and the resolver is not a guarantee.
//!
//! The listener is bound *before* the browser opens, so the port in the
//! redirect is one this process already holds and nothing else can claim in
//! between.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// How long the person has to approve before the CLI gives up. The server's
/// code expires in five minutes; waiting longer would only produce a worse
/// error message.
const APPROVAL_TIMEOUT: Duration = Duration::from_secs(300);

/// What the token endpoint hands back.
#[derive(Debug, Deserialize)]
pub struct Granted {
    /// The workspace token. Agent-scoped: it reaches the agent network and
    /// cannot write telemetry.
    pub token: String,
    /// Where this platform's gateway answers. `None` when the operator has not
    /// published one.
    #[serde(rename = "gatewayUrl")]
    pub gateway_url: Option<String>,
    /// The workspace's name, so the CLI can say what it joined.
    pub workspace: String,
}

/// Run the whole flow against `base` (the webapp, not the gateway).
pub async fn browser_login(base: &str) -> Result<Granted> {
    let base = base.trim_end_matches('/').to_owned();

    // Bound first: the URL we are about to open names this port, so it has to
    // be one we already hold.
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .context("listen on 127.0.0.1 for the browser to come back to")?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://127.0.0.1:{port}/callback");

    let verifier = random_base64(32);
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let state = random_base64(16);
    let client_name = client_name();

    let authorize = format!(
        "{base}/cli/authorize?code_challenge={}&code_challenge_method=S256\
         &redirect_uri={}&state={}&name={}",
        urlencode(&challenge),
        urlencode(&redirect_uri),
        urlencode(&state),
        urlencode(&client_name),
    );

    println!("Opening your browser to approve this machine.");
    println!("If it does not open, paste this into a browser:\n\n  {authorize}\n");
    open_browser(&authorize);

    let code =
        match tokio::time::timeout(APPROVAL_TIMEOUT, wait_for_callback(&listener, &state)).await {
            Ok(result) => result?,
            Err(_) => {
                return Err(anyhow!(
                    "timed out after {} minutes waiting for the browser. Run `kintri login` again",
                    APPROVAL_TIMEOUT.as_secs() / 60
                ))
            }
        };

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .user_agent(concat!("kintri/", env!("CARGO_PKG_VERSION")))
        .build()?;
    let response = http
        .post(format!("{base}/api/cli/token"))
        .json(&serde_json::json!({ "code": code, "codeVerifier": verifier }))
        .send()
        .await
        .context("exchange the approval for a token")?;

    if !response.status().is_success() {
        let status = response.status();
        // The server answers every failure identically on purpose, so there is
        // nothing more specific to report than "it did not work".
        return Err(anyhow!(
            "the approval could not be exchanged for a token ({status}). \
             It may have expired or already been used; run `kintri login` again"
        ));
    }
    response
        .json::<Granted>()
        .await
        .context("decode the token response")
}

/// Serve exactly one request: the browser's redirect back.
///
/// Anything else that connects to this port gets a 404 and is ignored, and the
/// loop keeps waiting — a stray probe must not be able to cancel a login.
async fn wait_for_callback(listener: &TcpListener, expected_state: &str) -> Result<String> {
    loop {
        let (mut stream, _) = listener.accept().await.context("accept the browser")?;
        let Some(target) = read_request_target(&mut stream).await? else {
            continue;
        };

        let (path, query) = target.split_once('?').unwrap_or((target.as_str(), ""));
        if path != "/callback" {
            respond(&mut stream, "404 Not Found", "Not this port.").await;
            continue;
        }

        let mut code = None;
        let mut state = None;
        for pair in query.split('&') {
            match pair.split_once('=') {
                Some(("code", v)) => code = Some(urldecode(v)),
                Some(("state", v)) => state = Some(urldecode(v)),
                _ => {}
            }
        }

        // The state proves this callback belongs to the login we started, and
        // not to one something else on this machine kicked off.
        if state.as_deref() != Some(expected_state) {
            respond(
                &mut stream,
                "400 Bad Request",
                "This callback does not belong to the login this machine started.",
            )
            .await;
            continue;
        }

        match code {
            Some(code) if !code.is_empty() => {
                respond(
                    &mut stream,
                    "200 OK",
                    "Approved. You can close this tab and go back to your terminal.",
                )
                .await;
                return Ok(code);
            }
            _ => {
                respond(
                    &mut stream,
                    "400 Bad Request",
                    "No authorization code was returned.",
                )
                .await;
                return Err(anyhow!(
                    "the browser came back without an authorization code"
                ));
            }
        }
    }
}

/// Read just enough of the request to get its target.
///
/// A deliberately small hand-rolled reader rather than an HTTP server: this
/// listener accepts one request, from the local browser, for one path. Pulling
/// a web framework into the client binary to parse a request line that is
/// always under 300 bytes would be a strange trade.
async fn read_request_target(stream: &mut TcpStream) -> Result<Option<String>> {
    let mut buffer = [0u8; 2048];
    let read = stream
        .read(&mut buffer)
        .await
        .context("read the callback")?;
    if read == 0 {
        return Ok(None);
    }
    let text = String::from_utf8_lossy(&buffer[..read]);
    let Some(line) = text.lines().next() else {
        return Ok(None);
    };
    let mut parts = line.split_whitespace();
    let (Some(method), Some(target)) = (parts.next(), parts.next()) else {
        return Ok(None);
    };
    if method != "GET" {
        return Ok(None);
    }
    Ok(Some(target.to_owned()))
}

async fn respond(stream: &mut TcpStream, status: &str, message: &str) {
    let body = format!(
        "<!doctype html><meta charset=\"utf-8\"><title>kintri</title>\
         <body style=\"font:16px system-ui;margin:4rem auto;max-width:32rem\">\
         <p>{message}</p></body>"
    );
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: text/html; charset=utf-8\r\n\
         content-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.flush().await;
}

fn random_base64(bytes: usize) -> String {
    let mut buffer = vec![0u8; bytes];
    rand::rng().fill_bytes(&mut buffer);
    URL_SAFE_NO_PAD.encode(buffer)
}

/// What the approval page shows the person, so they can tell their own login
/// from somebody else's.
fn client_name() -> String {
    let host = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .and_then(|o| String::from_utf8(o.stdout).ok())
        })
        .map(|h| h.trim().to_owned())
        .filter(|h| !h.is_empty());
    match host {
        Some(host) => format!("kintri on {host}"),
        None => "kintri".to_owned(),
    }
}

/// Best effort. The URL is printed either way, so a machine with no browser -
/// or an SSH session - is a copy and paste rather than a dead end.
fn open_browser(url: &str) {
    let (program, args): (&str, Vec<&str>) = if cfg!(target_os = "macos") {
        ("open", vec![])
    } else if cfg!(target_os = "windows") {
        ("cmd", vec!["/C", "start", ""])
    } else {
        ("xdg-open", vec![])
    };
    let _ = std::process::Command::new(program)
        .args(args)
        .arg(url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn();
}

/// Percent-encode everything that is not unreserved.
///
/// Small enough to write, and it keeps a URL-encoding crate out of a binary
/// that needs it for four query parameters.
fn urlencode(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

fn urldecode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => match u8::from_str_radix(&raw[i + 1..i + 3], 16) {
                Ok(byte) => {
                    out.push(byte);
                    i += 3;
                }
                Err(_) => {
                    out.push(bytes[i]);
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The challenge must be what the server recomputes from the verifier.
    ///
    /// Known answer from RFC 7636 appendix B, which is the only way to be sure
    /// the two ends agree without running both.
    #[test]
    fn pkce_matches_the_rfc_7636_known_answer() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    #[test]
    fn a_generated_verifier_is_the_shape_the_server_accepts() {
        let verifier = random_base64(32);
        assert_eq!(verifier.len(), 43);
        assert!(verifier
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));

        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(challenge.len(), 43);
    }

    #[test]
    fn query_values_survive_the_round_trip() {
        for value in ["kintri on pj-macbook", "a/b+c=d", "háčky a čárky", ""] {
            assert_eq!(urldecode(&urlencode(value)), value, "{value}");
        }
    }

    #[test]
    fn url_encoding_escapes_what_would_break_a_query() {
        assert_eq!(
            urlencode("http://127.0.0.1:53421/callback"),
            "http%3A%2F%2F127.0.0.1%3A53421%2Fcallback"
        );
        assert_eq!(urlencode("a&b=c"), "a%26b%3Dc");
    }
}
