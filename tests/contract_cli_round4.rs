#![allow(clippy::doc_lazy_continuation)]
//! Round 4 integration tests for the stryke-k8s-helper CLI:
//!   - `--help` exits 0 with stdout containing "Usage"
//!   - `--version` exits 0 with semver-shaped output
//!   - Unknown subcommand exits with code 2 (clap convention)
//!   - Empty argv (no subcommand) exits non-zero with usage on stderr
//!   - `--help` mentions at least one canonical subcommand string
//!   - Exit code on parse failure is 2 (not 1), per clap convention
//!
//! Earlier rounds pinned subcommand routing and required-flag validation
//! via internal `#[cfg(test)] mod tests` blocks inside `src/main.rs`.
//!
//! These tests target the OUTER process-level contracts (exit codes,
//! stderr/stdout framing, --help / --version conventions) that the
//! pure-function CLI tests inside src/ can't reach.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_stryke-k8s-helper"))
}

/// `--help` exits 0 and writes "Usage" to stdout.
#[test]
fn test_help_flag_exits_zero_and_includes_usage() {
    let out = bin().arg("--help").output().expect("spawn --help");
    assert!(
        out.status.success(),
        "--help must exit 0; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.to_lowercase().contains("usage"),
        "--help stdout must include 'Usage:'; got {stdout:?}"
    );
}

/// `--version` exits 0 and prints semver-shaped output (X.Y.Z).
#[test]
fn test_version_flag_exits_zero_and_prints_semver_string() {
    let out = bin().arg("--version").output().expect("spawn --version");
    assert!(out.status.success(), "--version must exit 0");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let has_semver = stdout.split_whitespace().any(|tok| {
        let parts: Vec<&str> = tok.split('.').collect();
        parts.len() >= 3 && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_digit()))
    });
    assert!(
        has_semver,
        "--version must include a semver-shaped X.Y.Z token; got {stdout:?}"
    );
}

/// Unknown subcommand exits with code 2 (clap convention for parse errors).
#[test]
fn test_unknown_subcommand_exits_with_code_two() {
    let out = bin()
        .arg("definitely-not-a-real-subcommand-xyz123")
        .output()
        .expect("spawn unknown");
    assert!(
        !out.status.success(),
        "unknown subcommand must NOT exit 0; got {:?}",
        out.status
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "clap convention: unknown subcommand → exit 2; got {:?}",
        out.status.code()
    );
}

/// Empty argv (no subcommand) is rejected with non-zero exit and usage hint
/// on stderr. Pins the "subcommand required" contract.
#[test]
fn test_no_subcommand_exits_nonzero_with_stderr_usage_hint() {
    let out = bin().output().expect("spawn (no args)");
    assert!(
        !out.status.success(),
        "no subcommand must exit non-zero; got {:?}",
        out.status
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.to_lowercase().contains("usage") || stderr.contains("--help"),
        "no-subcommand error must mention usage or --help; got {stderr:?}"
    );
}

/// `--help` mentions "Commands:" — clap's standard subcommand listing
/// header. Pins the help-shape contract so a refactor to a non-clap parser
/// (or a clap version that renames the header) is caught.
#[test]
fn test_help_lists_commands_section() {
    let out = bin().arg("--help").output().expect("spawn --help");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("Commands:") || stdout.contains("commands:"),
        "--help must include a 'Commands:' section header; got {stdout:?}"
    );
}

/// Parse error (malformed flag) exits with code 2, not 1. Pins the
/// distinction between USER error (2 — bad invocation) and RUNTIME error
/// (1 — operation failed) per Unix / clap convention.
#[test]
fn test_malformed_long_flag_exits_with_code_two_not_one() {
    let out = bin()
        .arg("--this-flag-definitely-does-not-exist-xyz")
        .output()
        .expect("spawn bad flag");
    assert!(!out.status.success(), "bad flag must exit non-zero");
    assert_eq!(
        out.status.code(),
        Some(2),
        "bad --flag must yield clap exit code 2 (parse error), not 1 (runtime); got {:?}",
        out.status.code()
    );
}
