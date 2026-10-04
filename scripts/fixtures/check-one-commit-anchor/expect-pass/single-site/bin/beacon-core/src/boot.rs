fn boot() {
    // One call expression. Definitions and test calls do not count.
    // archive.commit_anchor(not_a_call);
    let _note = ".commit_anchor(";
    archive.commit_anchor(trusted);
}

async fn commit_anchor(anchor: Anchor) {
    let _ = anchor;
}

#[cfg(test)] fn hidden() { archive.commit_anchor(hidden); }
