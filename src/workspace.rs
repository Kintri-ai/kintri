//! What the checkout can tell us about itself.
//!
//! Three facts, all of them metadata: the repository, the branch and a
//! project name. They are read by running `git`, not by reading files -
//! nothing here opens a source file, and there is no code path in this binary
//! that could send one.

use std::path::Path;
use std::process::Command;

/// Where an agent is working.
#[derive(Debug, Clone, Default)]
pub struct Checkout {
    /// `owner/name`, when the remote says so.
    pub repository: Option<String>,
    /// The current branch.
    pub branch: Option<String>,
    /// A project name. The repository's own name, unless told otherwise.
    pub project: Option<String>,
    /// Email from the local git config, for identity resolution.
    pub email: Option<String>,
}

/// Inspect the checkout at `dir`.
///
/// Every field is optional and a failure is silence: an agent working outside
/// a repository is a normal case, and losing presence over it would be absurd.
pub fn inspect(dir: &Path) -> Checkout {
    let branch = git(dir, &["rev-parse", "--abbrev-ref", "HEAD"]).filter(|b| b != "HEAD");
    let repository = git(dir, &["remote", "get-url", "origin"])
        .as_deref()
        .and_then(parse_remote);
    let project = repository
        .as_deref()
        .and_then(|r| r.split('/').nth(1))
        .map(str::to_owned);
    let email = git(dir, &["config", "--get", "user.email"]);

    Checkout {
        repository,
        branch,
        project,
        email,
    }
}

fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let value = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// `owner/name` from any of the shapes a git remote comes in.
///
/// `git@github.com:klik/nova-api.git`, `https://github.com/klik/nova-api`,
/// `ssh://git@github.com/klik/nova-api.git`. Lower-cased, because
/// `github_repositories.full_name` is normalised that way and a repository
/// that matched only when somebody typed the right capitals would be worse
/// than one that never matched at all.
pub fn parse_remote(remote: &str) -> Option<String> {
    let trimmed = remote.trim().trim_end_matches('/');
    let without_git = trimmed.strip_suffix(".git").unwrap_or(trimmed);
    let tail = match without_git.rsplit_once(':') {
        // scp-style: git@host:owner/name
        Some((head, tail)) if !head.contains('/') && !tail.starts_with("//") => tail,
        _ => {
            let after_scheme = without_git.split("://").last()?;
            after_scheme.split_once('/').map(|(_, rest)| rest)?
        }
    };
    let mut parts = tail.rsplit('/');
    let name = parts.next()?;
    let owner = parts.next()?;
    if owner.is_empty() || name.is_empty() {
        return None;
    }
    Some(format!("{}/{}", owner.to_lowercase(), name.to_lowercase()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_remote_shape_yields_owner_and_name() {
        for remote in [
            "git@github.com:Klik/Nova-API.git",
            "https://github.com/klik/nova-api",
            "https://github.com/klik/nova-api.git",
            "ssh://git@github.com/klik/nova-api.git",
            "git@github.com:klik/nova-api",
        ] {
            assert_eq!(
                parse_remote(remote).as_deref(),
                Some("klik/nova-api"),
                "{remote}"
            );
        }
    }

    #[test]
    fn a_remote_that_names_no_owner_is_no_answer() {
        assert_eq!(parse_remote(""), None);
        assert_eq!(parse_remote("nova-api"), None);
        assert_eq!(parse_remote("https://example.invalid/"), None);
    }
}
