#!/usr/bin/env python

import yt.wrapper as yt

from argparse import ArgumentParser

def main():
    parser = ArgumentParser()
    parser.add_argument("--minimum-number-of-chunks", type=int, default=0)
    args = parser.parse_args()

    for op_id in yt.list("//sys/operations", attributes=["output_transaction_id"]):
        if "output_transaction_id" not in op_id.attributes:
            continue
        tx_id = op_id.attributes["output_transaction_id"]
        #try:
        #    object_count = len(yt.get("#{}/@staged_object_ids".format(tx_id)))
        #except yt.YtResponseError as err:
        #    if err.is_resolve_error():
        #        continue
        #    else:
        #        raise

        if object_count > args.minimum_number_of_chunks:
            print op_id, object_count

if __name__ == "__main__":
    main()
