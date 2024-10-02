#!/usr/bin/env python3

from yt.yt.orm.example.python.local.local import ExampleInstance

from yt.orm.cli.local.local import LocalCli


class ExampleLocalCli(LocalCli):
    def __init__(self):
        super(ExampleLocalCli, self).__init__(name="Example")

    def make_instance(
        self,
        path,
        example_master_config=None,
        local_yt_options=None,
        port_locks_path=None,
        **kwargs
    ):
        return ExampleInstance(
            path,
            options=dict(
                example_master_config=example_master_config,
                enable_ssl=True,
                local_yt_options=local_yt_options,
                port_locks_path=port_locks_path,
                **kwargs
            ),
        )


def main():
    ExampleLocalCli().main()


if __name__ == "__main__":
    main()
