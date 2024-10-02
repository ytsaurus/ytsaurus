#!/usr/bin/env python3

from yt.yt.orm.example.python.admin.db_operations import init_yt_cluster
from yt.yt.orm.example.python.admin.data_model_traits import EXAMPLE_DATA_MODEL_TRAITS

from yt.yt.orm.example.python.client.client import find_token

from yt.orm.cli.admin.admin import AdminCli


class ExampleAdminCli(AdminCli):
    def __init__(self):
        super(ExampleAdminCli, self).__init__(EXAMPLE_DATA_MODEL_TRAITS)

    def init_yt_cluster(self, yt_client, path, **kwargs):
        return init_yt_cluster(yt_client, path, **kwargs)

    def find_token(self):
        return find_token()


def main():
    ExampleAdminCli().main()


if __name__ == "__main__":
    main()
