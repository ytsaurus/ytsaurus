#!/usr/bin/env python
import sys
import os
import argparse
import json
import tarfile

def read_range():
    input_range = json.loads(sys.stdin.readline())
    return input_range["offset"], input_range["limit"]

def set_file_codec(yt_client, dst, codec, codec_type):
    attribute_name = codec_type + "_codec"
    if codec is not None:
        if not yt_client.exists(dst):
            yt_client.create("file", dst, attributes={attribute_name: codec})
        yt_client.set_attribute(dst, attribute_name, codec)

def read(source_proxy, destination_proxy, token, config, source_transaction,
         destination_transaction, file, tmp_dir, erasure_codec, compression_codec):
    from yt.wrapper import YtClient, ypath_join
    import yt.wrapper as yt

    source_client = YtClient(source_proxy, token=token, config=config)
    destination_client = YtClient(destination_proxy, token=token, config=config)

    with source_client.Transaction(ping=False, transaction_id=source_transaction), \
         destination_client.Transaction(ping=False, transaction_id=destination_transaction):

        read_stream = None
        try:
            file_path = yt.FilePath(file, client=source_client)
            offset, limit = read_range()
            read_stream = source_client.read_file(file_path, offset=offset, length=limit)
            tmp_file_path = ypath_join(tmp_dir, "part_" + str(offset))

            set_file_codec(destination_client, tmp_file_path, erasure_codec, "erasure")
            set_file_codec(destination_client, tmp_file_path, compression_codec, "compression")

            destination_client.write_file(tmp_file_path, read_stream)
        finally:
            if read_stream is not None:
                read_stream.close()

def main():
    parser = argparse.ArgumentParser(description="Command to read file from yt cluster")
    parser.add_argument("--source-proxy", required=True)
    parser.add_argument("--destination-proxy", required=True)
    parser.add_argument("--file", required=True)
    parser.add_argument("--tmp-dir", required=True)
    parser.add_argument("--src-tx")
    parser.add_argument("--dst-tx")
    parser.add_argument("--config-file", help="File with client config in JSON format")
    parser.add_argument("--package-file", action="append")
    parser.add_argument("--erasure-codec")
    parser.add_argument("--compression-codec")
    args = parser.parse_args()

    if args.package_file:
        for package_file in args.package_file:
            assert package_file.endswith(".tar"), "Package must have .tar extension"
            tar = tarfile.open(package_file, "r:")
            tar.extractall()
            tar.close()
        sys.path.insert(0, ".")

    read(source_proxy=args.source_proxy,
         destination_proxy=args.destination_proxy,
         token=os.environ.get("YT_SECURE_VAULT_TOKEN"),
         config=json.load(open(args.config_file)),
         file=args.file,
         source_transaction=args.src_tx,
         destination_transaction=args.dst_tx,
         tmp_dir=args.tmp_dir,
         erasure_codec=args.erasure_codec,
         compression_codec=args.compression_codec)

if __name__ == "__main__":
    main()
