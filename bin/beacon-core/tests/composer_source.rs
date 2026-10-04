//! The composer matches the restart tri-state, not a two-valued durable option.

#[test]
fn composer_source_has_no_two_valued_durable_match() {
    let src = include_str!("../src/boot.rs");
    assert!(
        !src.contains("match durable"),
        "two-valued durable match must be gone"
    );
    assert!(
        !src.contains("durable.is_none()"),
        "Incomplete must not share the empty-store arm"
    );
    assert!(
        src.contains("[ARCH]") && src.contains("4.6.1"),
        "the Complete call site must cite [ARCH] §4.6.1"
    );
}
