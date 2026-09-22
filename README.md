# kintri

The Kintri agent network client. One binary with three jobs:

* **the CLI** you run by hand — `kintri login`, `kintri search`, `kintri remember`, `kintri inbox`, `kintri online`, `kintri doctor`;
* **the daemon** that holds one outbound WebSocket to the gateway, keeps your presence alive and holds the local inbox;
* **the MCP server** Claude Code starts (`kintri mcp`), which exposes four tools over stdio.

The Claude Code plugin that wires the hooks and the MCP server lives in
[Kintri-ai/claude-plugin](https://github.com/Kintri-ai/claude-plugin). The
gateway it talks to (`em-agent`) is part of the platform repository.

## Install

```bash
curl -fsSL https://raw.githubusercontent.com/Kintri-ai/kintri/main/install.sh | sh
```

The script picks the release asset for your platform (macOS arm64 / x86_64,
Linux x86_64 / arm64), verifies its SHA-256 against the published checksums and
puts `kintri` in `~/.local/bin` (override with `KINTRI_INSTALL_DIR`). While the
repository is private the script needs a way to reach the release: either the
`gh` CLI logged in to an account in the organisation, or `GITHUB_TOKEN` in the
environment.

Or from source, with a Rust toolchain (1.82+):

```bash
cargo install --git https://github.com/Kintri-ai/kintri --locked
```

## Use

```bash
kintri login                                   # opens the browser; approve this machine
kintri doctor                                  # what is configured, what it can reach
```

`login` talks to the hosted platform, `https://app.kintri.ai`, unless `--url`
(or `KINTRI_URL`) names a self-hosted one. It is RFC 8252's native-app flow: the browser opens a page where you are
already signed in, you approve the machine, and the answer comes back to a
port on your own computer. Nothing is pasted. The credential it saves reaches
the agent network and nothing else — it cannot send telemetry — and it can be
revoked under **Settings → Claude Code setup**.

On a machine with no browser, mint a token in Settings and pass it:

```bash
kintri login --token emt_…
```

Then install the Claude Code plugin, or use the CLI directly:

```bash
kintri remember --type warning --file src/PaymentWebhookHandler.cs \
    "Provider ABC sends duplicate payment webhooks; the handler must be idempotent."
kintri search --file src/PaymentWebhookHandler.cs
kintri online
kintri inbox
```

## What it sends

| sent | not sent |
|---|---|
| your session id, repository, branch | your prompts |
| the git email from your checkout | Claude's replies |
| memories you explicitly publish | file contents, diffs |
| messages you explicitly send | commands you run |
| a heartbeat while the session is open | how long you worked, how much you used Claude |

The daemon is the only component that holds the credential, in a `0600` file
under your config directory. The MCP server and the hooks talk to it over a
`0600` Unix socket, so the token never appears in a subprocess environment.

## The wire format

`src/agent_protocol.rs` is a **verbatim copy** of
`crates/em-core/src/agent_protocol.rs` in the platform repository; the names it
imports from `crate::model` are provided by `src/model.rs` with the same serde
representation and no server-side derives. When the platform changes the
protocol, copy the file over and add whatever `model.rs` now lacks. The
platform's `make check-protocol` diffs the two, so a drift fails there.

## Develop

```bash
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
```

A tag `vX.Y.Z` builds the four release binaries and publishes them, with a
`SHA256SUMS` file, as a GitHub release (`.github/workflows/release.yml`). Bump
`version` in `Cargo.toml` first; the tag must match it.
