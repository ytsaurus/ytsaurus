import yt.wrapper as yt

from helpers import TEST_DIR

import os
import pytest
import tempfile

@pytest.mark.usefixtures("yt_env")
class TestFileCommands(object):
    def test_file_commands(self):
        with pytest.raises(yt.YtError):
            yt.upload_file("", TEST_DIR + "/dir/file")

        file_path = TEST_DIR + "/file"
        yt.upload_file("", file_path)
        assert yt.download_file(file_path).read() == ""

        yt.upload_file("0" * 1000, file_path)
        assert yt.download_file(file_path).read() == "0" * 1000

        _, filename = tempfile.mkstemp()
        with open(filename, "w") as fout:
            fout.write("some content")

        destinationA = yt.smart_upload_file(filename, placement_strategy="hash")
        assert destinationA.startswith(yt.config["remote_temp_files_directory"])

        destinationB = yt.smart_upload_file(filename, placement_strategy="hash")
        assert destinationA == destinationB

        # Lets break link
        yt.remove(yt.get_attribute(destinationB + "&", "target_path"), force=True)
        assert yt.smart_upload_file(filename, placement_strategy="hash") == destinationA

        destination = yt.smart_upload_file(filename, placement_strategy="random")
        path = os.path.join(os.path.basename(filename), yt.config["remote_temp_files_directory"])
        assert destination.startswith(path)

        destination = TEST_DIR + "/file_dir/some_file"
        yt.smart_upload_file(filename, destination=destination, placement_strategy="ignore")
        assert yt.get_attribute(destination, "file_name") == "some_file"

        with pytest.raises(yt.YtError):
            yt.smart_upload_file(filename, destination=destination, placement_strategy="random")
        with pytest.raises(yt.YtError):
            yt.smart_upload_file(filename, destination=destination, placement_strategy="hash")

        assert yt.download_file(destination, length=4).read() == "some"
        assert yt.download_file(destination, offset=5).read() == "content"

        destination = yt.smart_upload_file(filename, placement_strategy="ignore")
        yt.smart_upload_file(filename, placement_strategy="ignore")
        assert yt.download_file(destination).read() == "some content"

