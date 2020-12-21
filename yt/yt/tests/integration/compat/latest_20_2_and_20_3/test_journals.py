# from yt.yt.tests.integration.tests.test_journals import TestJournals as BaseTestJournals
from original_tests.yt.yt.tests.integration.tests.test_journals import TestJournals as BaseTestJournals


class TestJournalsCompat(BaseTestJournals):
    ARTIFACT_COMPONENTS = {
        "20_2": ["master"],
        "20_3": ["node", "job-proxy", "exec", "tools", "scheduler", "controller-agent", "proxy", "http-proxy"],
    }
