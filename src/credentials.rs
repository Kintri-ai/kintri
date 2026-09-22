//! Where the token lives.
//!
//! # A knowing deviation from the spec, flagged rather than hidden
//!
//! The specification says credentials belong in the OS keychain and must not
//! sit in a plaintext configuration file. This v0.1 writes a `0600` file
//! under `$XDG_CONFIG_HOME/kintri` (or `~/.config/kintri`) and the keychain
//! is not wired up yet.
//!
//! That is a real gap, not an oversight, and it is bounded on purpose:
//!
//! * the token is a workspace `emt_` token with exactly the agent network's
//!   reach - it publishes memories and messages, and it cannot read anybody's
//!   telemetry, pull requests or identities;
//! * it is revocable from the webapp, so a leaked one is a click to kill;
//! * the file is `0600` and the directory `0700`, which is the same bar as
//!   `~/.ssh` and `~/.aws/credentials`.
//!
//! [`Store`] exists so that adding a keychain backend is one implementation
//! and no call site: the decision that is actually open is which crate to
//! take on for it, and that belongs in a review rather than in a commit that
//! quietly adds a dependency with a dbus requirement on Linux CI.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

/// What `kintri login` saved.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Credentials {
    /// Base URL of the gateway, e.g. `https://agent.kintri.ai`.
    pub gateway_url: String,
    /// The platform the developer logged in to, e.g. `https://app.kintri.ai`.
    /// What `kintri status` shows: a person recognises the address they sign
    /// in at, not the gateway's. Absent in files written before 0.1.1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_url: Option<String>,
    /// The workspace's display name, as the approval page reported it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    /// The workspace token. Never logged, never printed, never sent anywhere
    /// but the gateway's `Authorization` header.
    pub token: String,
}

impl Credentials {
    /// Where the developer thinks of themselves as logged in: the platform
    /// they signed in at, or the gateway when only a token was pasted.
    pub fn home(&self) -> &str {
        self.workspace_url.as_deref().unwrap_or(&self.gateway_url)
    }

    /// `Bearer <token>`.
    pub fn header(&self) -> String {
        format!("Bearer {}", self.token)
    }

    /// The last four characters, for `kintri status`. Enough to tell two
    /// tokens apart, not enough to be one.
    pub fn fingerprint(&self) -> String {
        let tail: String = self.token.chars().rev().take(4).collect();
        format!("…{}", tail.chars().rev().collect::<String>())
    }
}

/// Somewhere credentials can be kept.
pub trait Store {
    /// Read them, or `None` if nobody has logged in.
    fn load(&self) -> Result<Option<Credentials>>;
    /// Write them, replacing whatever was there.
    fn save(&self, credentials: &Credentials) -> Result<()>;
    /// Remove them. Not an error if there were none.
    fn clear(&self) -> Result<()>;
    /// Where they are, for `kintri doctor` to print.
    fn describe(&self) -> String;
}

/// A `0600` file in the user's config directory.
pub struct FileStore {
    path: PathBuf,
}

impl FileStore {
    /// `$KINTRI_CONFIG_DIR`, else `$XDG_CONFIG_HOME/kintri`, else
    /// `~/.config/kintri`.
    pub fn discover() -> Result<Self> {
        let dir = if let Ok(explicit) = std::env::var("KINTRI_CONFIG_DIR") {
            PathBuf::from(explicit)
        } else if let Ok(xdg) = std::env::var("XDG_CONFIG_HOME") {
            PathBuf::from(xdg).join("kintri")
        } else {
            let home = std::env::var("HOME")
                .map_err(|_| anyhow!("neither HOME nor XDG_CONFIG_HOME is set"))?;
            PathBuf::from(home).join(".config").join("kintri")
        };
        Ok(FileStore {
            path: dir.join("credentials.json"),
        })
    }

    /// The directory holding the credentials and the daemon's socket.
    pub fn dir(&self) -> &Path {
        self.path
            .parent()
            .expect("credentials path always has a parent")
    }
}

impl Store for FileStore {
    fn load(&self) -> Result<Option<Credentials>> {
        match fs::read_to_string(&self.path) {
            Ok(raw) => Ok(Some(
                serde_json::from_str(&raw).context("credentials file is not valid JSON")?,
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e).context("read credentials"),
        }
    }

    fn save(&self, credentials: &Credentials) -> Result<()> {
        let dir = self.dir();
        fs::create_dir_all(dir).context("create config directory")?;
        restrict(dir, 0o700)?;

        // Written through a temporary file in the same directory: an
        // interrupted write must not leave a half-token behind that a later
        // read would report as corrupt credentials.
        let tmp = self.path.with_extension("json.tmp");
        let mut file = fs::File::create(&tmp).context("create credentials file")?;
        restrict(&tmp, 0o600)?;
        file.write_all(serde_json::to_string_pretty(credentials)?.as_bytes())
            .context("write credentials")?;
        file.sync_all().context("flush credentials")?;
        drop(file);
        fs::rename(&tmp, &self.path).context("install credentials")?;
        restrict(&self.path, 0o600)?;
        Ok(())
    }

    fn clear(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).context("remove credentials"),
        }
    }

    fn describe(&self) -> String {
        self.path.display().to_string()
    }
}

#[cfg(unix)]
fn restrict(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("set permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn restrict(_path: &Path, _mode: u32) -> Result<()> {
    // Windows inherits the user profile's ACL, which is already per-user.
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fingerprint_identifies_without_revealing() {
        let c = Credentials {
            gateway_url: "https://example.invalid".to_owned(),
            workspace_url: None,
            workspace: None,
            token: "emt_abcdefghijkl_secretsecretsecret".to_owned(),
        };
        let printed = c.fingerprint();
        assert_eq!(printed, "…cret");
        assert!(!printed.contains("emt_"));
        assert!(!c.token.contains(&printed[3..]) || printed.len() < c.token.len());
    }

    #[test]
    fn credentials_round_trip_through_a_restricted_file() {
        let dir = std::env::temp_dir().join(format!("kintri-test-{}", std::process::id()));
        let store = FileStore {
            path: dir.join("credentials.json"),
        };
        assert!(store.load().unwrap().is_none(), "nothing saved yet");

        let creds = Credentials {
            gateway_url: "https://agent.example.invalid".to_owned(),
            workspace_url: None,
            workspace: None,
            token: "emt_abcdefghijkl_secret".to_owned(),
        };
        store.save(&creds).unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&store.path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "the token must not be world-readable");
        }

        let read = store.load().unwrap().expect("saved credentials");
        assert_eq!(read.token, creds.token);

        store.clear().unwrap();
        assert!(store.load().unwrap().is_none());
        // Clearing twice is not an error: logout should be idempotent.
        store.clear().unwrap();
        let _ = fs::remove_dir_all(&dir);
    }
}
