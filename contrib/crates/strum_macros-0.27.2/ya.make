# Native Rust build with dependencies shared through contrib/crates.

CRATE_PROC_MACRO(strum_macros)

CRATE_VERSION(0.27.2)
CRATE_EDITION(2021)
CRATE_ROOT(src/lib.rs)
CRATE_MANIFEST(Cargo.toml)

CRATE_SOURCES(
    src/helpers/case_style.rs
    src/helpers/inner_variant_props.rs
    src/helpers/metadata.rs
    src/helpers/mod.rs
    src/helpers/type_props.rs
    src/helpers/variant_props.rs
    src/macros/enum_count.rs
    src/macros/enum_discriminants.rs
    src/macros/enum_is.rs
    src/macros/enum_iter.rs
    src/macros/enum_messages.rs
    src/macros/enum_properties.rs
    src/macros/enum_table.rs
    src/macros/enum_try_as.rs
    src/macros/enum_variant_array.rs
    src/macros/enum_variant_names.rs
    src/macros/from_repr.rs
    src/macros/mod.rs
    src/macros/strings/as_ref_str.rs
    src/macros/strings/display.rs
    src/macros/strings/from_string.rs
    src/macros/strings/mod.rs
    src/macros/strings/to_string.rs
)

CRATE_INPUTS(
    Cargo.lock
    LICENSE
    README.md
)

CRATE_FLAGS(--cap-lints=allow)

PEERDIR(
    contrib/crates/heck-0.5.0
    contrib/crates/proc-macro2-1.0.107
    contrib/crates/quote-1.0.47
    contrib/crates/syn-2.0.119
)

LICENSE("MIT")
VERSION(0.27.2)
ORIGINAL_SOURCE(https://static.crates.io/crates/strum_macros/strum_macros-0.27.2.crate)

END()
