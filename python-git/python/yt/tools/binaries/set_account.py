#!/usr/bin/python

import yt.logger as logger
import yt.wrapper as yt
from yt.wrapper.cli_helpers import die

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
        yt.config.set_proxy(args.proxy)

    for obj in yt.search(args.dir, attributes=["account"]):
        if obj.attributes["account"] != args.account:
            try:
                yt.set(obj + "/@account", args.account)
            except yt.YtResponseError as error:
                if error.is_concurrent_transaction_lock_conflict():
                    logger.warning("Cannot set '%s' account to node '%s'", args.account, obj)
                else:
                    raise
            logger.info("Account '%s' set to node '%s'", args.account, obj)


if __name__ == "__main__":
    try:
        main()
    except yt.YtError as error:
        die(str(error))
    except Exception:
        traceback.print_exc(file=sys.stderr)
        die()

