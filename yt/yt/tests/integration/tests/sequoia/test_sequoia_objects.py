from yt_env_setup import YTEnvSetup

from yt_commands import authors, get

##################################################################


class TestSequoiaObjects(YTEnvSetup):
    USE_SEQUOIA = True

    @authors("gritukan")
    def test_estimated_creation_time(self):
        object_id = "543507cc-00000000-12345678-abcdef01"
        creation_time = {'min': '2012-12-21T08:34:56.000000Z', 'max': '2012-12-21T08:34:57.000000Z'}
        assert get("//sys/estimated_creation_time/{}".format(object_id)) == creation_time
