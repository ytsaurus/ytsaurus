fn main() {
    #[cfg(feature = "python")]
    pyo3_build_config::use_pyo3_cfgs();
}
