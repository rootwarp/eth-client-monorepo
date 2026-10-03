//! S0-A-25 / P0-03 — `CoreConfig::default()` ships `VerifyIndividual`.
//!
//! `NoVerification` is reserved for durable-seed replay (`seed_from_durable` →
//! `on_block`). No other production construction site assigns it to
//! `CoreConfig.verify`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::path::{Path, PathBuf};

use cc_chain::core::CoreConfig;
use cc_state_transition::BlockSignatureStrategy;

/// Walk `*.rs` under `root` without a walkdir dependency.
fn walkdir_rs(root: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| {
            panic!("read_dir {}: {e}", dir.display());
        }) {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    out
}

/// Drop `//` / `/* */` comments. String contents are kept (no `NoVerification` lives there).
fn strip_rust_comments(src: &str) -> String {
    let bytes = src.as_bytes();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    let mut in_str = false;
    let mut in_line = false;
    let mut in_block = false;
    while i < bytes.len() {
        let c = bytes[i] as char;
        let next = bytes.get(i + 1).copied().map(|b| b as char);
        if in_line {
            if c == '\n' {
                in_line = false;
                out.push('\n');
            }
            i += 1;
            continue;
        }
        if in_block {
            if c == '*' && next == Some('/') {
                in_block = false;
                i += 2;
            } else {
                i += 1;
            }
            continue;
        }
        if in_str {
            out.push(c);
            if c == '\\'
                && let Some(n) = next
            {
                out.push(n);
                i += 2;
                continue;
            }
            if c == '"' {
                in_str = false;
            }
            i += 1;
            continue;
        }
        if c == '/' && next == Some('/') {
            in_line = true;
            i += 2;
            continue;
        }
        if c == '/' && next == Some('*') {
            in_block = true;
            i += 2;
            continue;
        }
        if c == '"' {
            in_str = true;
            out.push(c);
            i += 1;
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

/// Remove `#[cfg(test)]` items (attribute + following `{...}` item).
fn strip_cfg_test_items(src: &str) -> String {
    let mut out = String::new();
    let mut rest = src;
    while let Some(idx) = rest.find("#[cfg(test)]") {
        out.push_str(&rest[..idx]);
        let mut s = rest[idx + "#[cfg(test)]".len()..].trim_start();
        while s.starts_with("#[") {
            match s.find(']') {
                Some(end) => s = s[end + 1..].trim_start(),
                None => break,
            }
        }
        match s.find('{') {
            Some(brace) => {
                let after_open = &s[brace + 1..];
                let (depth_end, _) = match_braces(after_open);
                rest = &after_open[depth_end..];
            }
            None => {
                rest = s.split_once('\n').map(|(_, r)| r).unwrap_or("");
            }
        }
    }
    out.push_str(rest);
    out
}

fn match_braces(after_open: &str) -> (usize, usize) {
    let mut depth = 1usize;
    for (i, c) in after_open.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return (i + 1, depth);
                }
            }
            _ => {}
        }
    }
    (after_open.len(), depth)
}

fn compact(src: &str) -> String {
    src.chars().filter(|c| !c.is_whitespace()).collect()
}

/// Argument list of each `name(` call, with balanced parens.
fn call_arg_lists<'a>(compacted: &'a str, name: &str) -> Vec<&'a str> {
    let needle = format!("{name}(");
    let mut out = Vec::new();
    let mut search = compacted;
    while let Some(idx) = search.find(&needle) {
        let after = &search[idx + needle.len()..];
        let mut depth = 1usize;
        let mut end = after.len();
        for (i, c) in after.char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = i;
                        break;
                    }
                }
                _ => {}
            }
        }
        out.push(&after[..end]);
        search = &after[end.min(after.len())..];
    }
    out
}

fn production_src(text: &str) -> String {
    strip_rust_comments(&strip_cfg_test_items(text))
}

#[test]
fn core_config_default_is_verify_individual() {
    assert_eq!(
        CoreConfig::default().verify,
        BlockSignatureStrategy::VerifyIndividual
    );
}

/// Production `src/` never assigns `CoreConfig.verify = NoVerification`.
/// Restore-replay still raises `NoVerification` as an `on_block` **argument**
/// (module docs / comments do not count).
#[test]
fn no_verification_only_raised_on_restore_replay() {
    let src_roots = [
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src"),
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../crates/chain-core/src"),
    ];
    let mut assignments = Vec::new();
    let mut restore_on_block_override = false;
    let mut spawn_assigns_no_verification = false;
    let mut import_on_block_no_verification = false;

    for src in &src_roots {
        for path in walkdir_rs(src) {
            let rel = path
                .strip_prefix(src)
                .unwrap_or(&path)
                .display()
                .to_string();
            let text = std::fs::read_to_string(&path).unwrap_or_else(|e| {
                panic!("read {}: {e}", path.display());
            });
            let production = production_src(&text);
            let compacted = compact(&production);

            if compacted.contains("verify:BlockSignatureStrategy::NoVerification")
                || compacted.contains(".verify=BlockSignatureStrategy::NoVerification")
                || compacted.contains("verify=BlockSignatureStrategy::NoVerification")
            {
                assignments.push(rel.clone());
            }

            if rel == "seed.rs" {
                restore_on_block_override = call_arg_lists(&compacted, "on_block_with_context")
                    .iter()
                    .any(|args| args.contains("BlockSignatureStrategy::NoVerification"));
                // Live core after replay must keep the caller's strategy.
                if let Some(idx) = compacted.find("fnspawn_core_from_seed") {
                    let fn_src = &compacted[idx..];
                    spawn_assigns_no_verification = fn_src
                        .contains("verify=BlockSignatureStrategy::NoVerification")
                        || fn_src.contains("verify:BlockSignatureStrategy::NoVerification");
                }
            }

            if rel == "import.rs" {
                import_on_block_no_verification = call_arg_lists(&compacted, "on_block")
                    .iter()
                    .any(|args| args.contains("BlockSignatureStrategy::NoVerification"));
            }
        }
    }

    assert!(
        assignments.is_empty(),
        "CoreConfig.verify must not be constructed as NoVerification in production src: \
         {assignments:?}"
    );
    assert!(
        restore_on_block_override,
        "seed.rs must pass BlockSignatureStrategy::NoVerification \
         as an on_block argument (comments/docs do not count)"
    );
    assert!(
        !spawn_assigns_no_verification,
        "spawn_core_from_seed must not assign CoreConfig.verify = NoVerification"
    );
    assert!(
        !import_on_block_no_verification,
        "import.rs must pass the configured strategy into on_block, not hardcode NoVerification"
    );
}

#[test]
fn comment_strip_does_not_treat_docs_as_on_block_args() {
    let docs_only = r#"
        //! Uses [`BlockSignatureStrategy::NoVerification`] exclusively
        /// on_block replay is privileged
        fn seed_from_durable_docs() {
            // on_block(..., BlockSignatureStrategy::NoVerification)
            let _ = 1;
        }
    "#;
    let production = production_src(docs_only);
    let compacted = compact(&production);
    assert!(
        !call_arg_lists(&compacted, "on_block")
            .iter()
            .any(|args| args.contains("BlockSignatureStrategy::NoVerification")),
        "commented on_block must not count"
    );
}
