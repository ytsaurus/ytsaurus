from original_tests.yt.yt.tests.integration.misc.test_get_supported_features \
    import TestGetFeatures as BaseTestGetFeatures


class TestGetFeaturesNewProxy(BaseTestGetFeatures):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "job-proxy", "exec", "tools", "scheduler", "controller-agent"],
        "trunk": ["proxy", "http-proxy"],
    }
    SKIP_STATISTICS_DESCRIPTIONS = True
