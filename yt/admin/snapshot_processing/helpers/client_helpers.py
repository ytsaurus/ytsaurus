#!usr/bin/env python

from yt.wrapper import YtClient

import yt.logger as logger

import os


def get_proxy(proxy):
    proxy = proxy if proxy else os.environ.get("YT_PROXY")
    if proxy:
        return proxy
    raise RuntimeError("Proxy is not specified")


def create_client(proxy, token_env_variable):
    client = YtClient(proxy=get_proxy(proxy))

    if token_env_variable is None:
        logger.info("'token_env_variable' is None, using 'YT_TOKEN'")
        token_env_variable = "YT_TOKEN"

    if token_env_variable in os.environ:
        client.config["token"] = os.environ[token_env_variable]
        logger.info("Using token from environment variable {}".format(token_env_variable))
        return client

    logger.info("'{}' should be defined".format(token_env_variable))
    return client
