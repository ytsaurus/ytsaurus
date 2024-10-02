#!/usr/bin/env python3

from yt.yt.orm.example.python.client.client import (
    ExampleClient,
    find_token,
)
import yt.yt.orm.example.python.client.data_model as data_model

from yt.orm.cli.client.client import ClientCli


class ExampleClientCli(ClientCli):
    def get_human_readable_orm_name(self):
        return "Example"

    def get_event_type(self):
        return data_model.EEventType

    def make_orm_client(self, address, protocol, config):
        return ExampleClient(address=address, transport=protocol, config=config)

    def find_token(self, allow_receive_token_by_ssh_session):
        return find_token(allow_receive_token_by_ssh_session)


def main():
    ExampleClientCli().main()


if __name__ == "__main__":
    main()
