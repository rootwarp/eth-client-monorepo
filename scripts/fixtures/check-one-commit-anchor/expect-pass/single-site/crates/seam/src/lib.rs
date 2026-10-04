async fn commit_anchor(&self, anchor: TrustedAnchor) {
    let _ = anchor;
}

#[cfg(test)]
mod tests {
    fn calls_from_a_test() {
        archive.commit_anchor(anchor);
        ArchiveWrite::commit_anchor(archive, anchor);
    }
}
