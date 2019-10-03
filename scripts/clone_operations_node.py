#!/usr/bin/python
import argparse
import logging

import yt.wrapper as yt


def main(src, dst):
    if not yt.exists(src):
        raise Exception("Source path %s does not exists.", src)

    if not yt.exists(dst):
        yt.create("map_node", dst)

    logging.info("Listing operations")
    batch_client = yt.create_batch_client(raise_errors=True)
    list_reqs = []
    for byte in yt.list(src):
        list_reqs.append({
            "src_bucket": src + '/' + byte,
            "dst_bucket": dst + '/' + byte,
            "req": batch_client.list(src + '/' + byte)
        })
    batch_client.commit_batch()

    logging.info("Reading operations")
    batch_client = yt.create_batch_client()
    attributes = None
    read_reqs = []
    for list_req in list_reqs:
        for operation in list_req["req"].get_result():
            src_op = list_req["src_bucket"] + '/' + operation
            dst_op = list_req["dst_bucket"] + '/' + operation
            if not attributes:
                attributes = yt.get(src_op + '/@user_attribute_keys')
            read_op_req = {
                "src_op": src_op,
                "dst_op": dst_op,
                "attributes": batch_client.get(src_op, attributes=attributes),
                "exists_secure_vault": batch_client.exists(src_op + "/secure_vault"),
                "get_secure_vault": batch_client.get(src_op + "/secure_vault")
            }
            read_reqs.append(read_op_req)
    batch_client.commit_batch()

    logging.info("Creating bucket structure")
    batch_client = yt.create_batch_client(raise_errors=True)
    for byte in yt.list(src):
        batch_client.create('map_node', dst + '/' + byte)
    batch_client.commit_batch()

    logging.info("Creating operation nodes")
    batch_client = yt.create_batch_client(raise_errors=True)
    for read_op_req in read_reqs:
        if not read_op_req["attributes"].is_ok():
            logging.error("Get attributes failed")
            raise yt.YtResponseError(read_op_req["attributes"].get_error())

        batch_client.create("map_node", read_op_req["dst_op"], attributes=read_op_req["attributes"].get_result())
    batch_client.commit_batch()

    logging.info("Creating empty secure vaults")
    batch_client = yt.create_batch_client(raise_errors=True)
    for read_op_req in read_reqs:
        if not read_op_req["exists_secure_vault"].is_ok():
            logging.error("Exists secure vault failed")
            raise yt.YtResponseError(read_op_req["exists_secure_vault"].get_error())

        if read_op_req["exists_secure_vault"].get_result():
            batch_client.create("document", read_op_req["dst_op"] + "/secure_vault")
    batch_client.commit_batch()

    logging.info("Filling secure vaults")
    batch_client = yt.create_batch_client(raise_errors=True)
    for read_op_req in read_reqs:
        if read_op_req["exists_secure_vault"].get_result():
            if not read_op_req["get_secure_vault"].is_ok():
                logging.error("Secure vault exists but get secure vault failed")
                raise yt.YtResponseError(read_op_req["get_secure_vault"].get_error())

            batch_client.set(read_op_req["dst_op"] + "/secure_vault", read_op_req["get_secure_vault"].get_result())
    batch_client.commit_batch()

    logging.info("Done")


if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s", level=logging.INFO)

    parser = argparse.ArgumentParser(description='Clone operations root node to new location.')
    parser.add_argument('--src', type=str, required=True)
    parser.add_argument('--dst', type=str, required=True)
    args = parser.parse_args()

    main(args.src, args.dst)
