from original_tests.yt.yt.tests.integration.misc.test_get_supported_features \
    import TestGetFeatures as BaseTestGetFeatures


class TestGetFeaturesNewProxy(BaseTestGetFeatures):
    ARTIFACT_COMPONENTS = {
        "22_4": ["master", "node", "job-proxy", "exec", "tools", "scheduler", "controller-agent"],
        "trunk": ["proxy", "http-proxy"],
    }
    SKIP_STATISTICS_DESCRIPTIONS = True

    # COMPAT(gepardo): Remove this after 22.4.
    USE_NATIVE_AUTH = False
