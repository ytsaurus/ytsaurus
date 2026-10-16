import os

import urllib3


def upload_yt_files(yt, yt_files):
    for alias, file_path in yt_files.items():
        parsed_url = urllib3.util.parse_url(alias)
        assert parsed_url.scheme == 'yt'
        assert parsed_url.netloc == 'plato'

        yt_path = '/{}'.format(parsed_url.path)
        yt_dirname = os.path.dirname(yt_path)

        yt.yt_client.create(
            'map_node', yt_dirname,
            recursive=True,
            ignore_existing=True,
        )

        with open(file_path, 'rb') as file_:
            yt.yt_client.write_file(yt_path, file_.read())
