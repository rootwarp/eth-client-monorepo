fn boot() {
    archive.commit_anchor(trusted);
}

#[cfg(test)] fn hidden() { let _ = 1; } archive.commit_anchor(sneak);
