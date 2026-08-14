PY3TEST()

SIZE(SMALL)

PEERDIR(
    yt/yt/tools/pod_size_actualization/optimization
)

TEST_SRCS(
    test_assignment_rows.py
    test_bundle_groups.py
    test_config_validity.py
    test_cross_dc.py
    test_instance_sizes.py
    test_method_merge.py
    test_n_min.py
    test_resource_prices.py
)

END()
