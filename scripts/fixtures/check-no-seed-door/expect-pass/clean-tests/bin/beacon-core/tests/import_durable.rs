fn import_on_block_then_commit_writes_durable_rows() {
    archive.commit_import(import);
    // A prefixed name is not the door.
    let _ = not_submit_p0_committed;
}
