#!/usr/bin/python

import yt.logger as logger
import yt.wrapper as yt
from yt.wrapper.cli_helpers import die
from yt.wrapper.batch_helpers import create_batch_client

from yt.packages.six.moves import zip as izip

import argparse
import sys
import traceback

def main():
    parser = argparse.ArgumentParser(description='Add user.')
    parser.add_argument('dir')
    parser.add_argument('account')
    parser.add_argument('--proxy')
    args = parser.parse_args()

    if args.proxy is not None:
        yt.config["proxy"]["url"] = args.proxy

    batch_client = create_batch_client()

    processed_nodes = []
    for obj in yt.search(args.dir, attributes=["account", "type"], enable_batch_mode=True):
        if obj.attributes["account"] != args.account:
            if obj.attributes["type"] == "link":
                set_result = batch_client.set(obj + "&/@account", args.account)
            else:
                set_result = batch_client.set(obj + "/@account", args.account)
            processed_nodes.append((obj, set_result))

    batch_client.commit_batch()

    for obj, set_result in processed_nodes:
        if set_result.get_error() is not None:
            error = yt.YtResponseError(set_result.get_error())
            if error.is_concurrent_transaction_lock_conflict():
                logger.warning("Cannot set '%s' account to node '%s'", args.account, obj)
            elif error.is_resolve_error():
                pass
            else:
                raise error
        logger.info("Account '%s' set to node '%s'", args.account, obj)


if __name__ == "__main__":
    try:
        main()
    except yt.YtError as error:
        die(str(error))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()

