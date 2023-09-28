from original_tests.yt.yt.tests.integration.node.test_disk_quota \
    import TestDiskMediumAccounting as BaseTestDiskMediumAccounting


class TestDiskMediumAccountingtUpToCA(BaseTestDiskMediumAccounting):
    ARTIFACT_COMPONENTS = {
        "23_2": ["master", "node", "job-proxy", "exec", "tools"],
        "trunk": ["scheduler", "controller-agent", "proxy", "http-proxy"],
    }
