#!/usr/bin/env python

from yt.wrapper.common import die
import yt.wrapper as yt

import os
import sys
import hashlib
import itertools
import simplejson as json
from argparse import ArgumentParser

def md5(obj):
    h = hashlib.md5()

    if isinstance(obj, dict):
        for key, value in sorted(obj.items()):
            h.update(key)
            h.update(md5(value))
    elif isinstance(obj, list):
        for value in obj:
            h.update(md5(value))
    elif isinstance(obj, unicode):
        h.update(obj.encode("utf-8"))
    else:
        h.update(str(obj))

    return h.digest()

def from_hex(str):
    return "".join(chr(int(str[i * 2: i * 2 + 2], 16)) for i in xrange(len(str) / 2))

def parse_stream(stream, input_type):
    for line in stream:
        if input_type == "structured":
            parsed = json.loads(line)
            if parsed.get("$value", "") is None:
                continue
            yield parsed
        else:
            yield line

def extract_key(obj, input_type, reduce_by):
    if input_type == "yamr":
        return obj.split("\t", 1)[0]
    if input_type == "structured":
        return json.dumps([obj.get(key, "") for key in reduce_by])
    assert False, "Incorrect input_type '%s'" % input_type

def extract_md5(stream, input_type):
    for row in stream:
        if input_type == "md5":
            yield from_hex(row.split("\t", 1)[1].strip())
        else:
            yield md5(row)

class Hash(object):
    def __init__(self, md5=None):
        if md5 is None:
            self.array = [0] * 16
        else:
            assert len(md5) == 16
            self.array = map(ord, md5)

    def update(self, other):
        assert len(self.array) == len(other.array)
        for i in xrange(len(self.array)):
            self.array[i] = self.array[i] ^ other.array[i]

    def md5(self):
        return "".join(map(chr, self.array))

    def hex(self):
        return "".join(map(lambda num: "%02x" % num, self.array))

class SortedHash(object):
    prime = 4093082899
    def __init__(self, md5=None):
        if md5 is None:
            self.matrix = [[1, 0],
                           [0, 1]]
        else:
            assert len(md5) == 16
            self._number_len = len(md5) / 4
            numbers = [self._str_to_num(md5[self._number_len * i: self._number_len * (i + 1)]) for i in xrange(4)]
            self.matrix = [[numbers[i * 2 + j] for j in xrange(2)] for i in xrange(2)]

    def _str_to_num(self, bytes):
        result = 0
        for byte in bytes:
            result = result * 256 + ord(byte)
        return result

    def _num_to_str(self, num):
        result = ""
        for i in xrange(self._number_len):
            result = result + chr(num % 256)
            num /= 256
        return result

    def update(self, other):
        product = [[0 for j in xrange(2)] for i in xrange(2)]
        for i in xrange(2):
            for j in xrange(2):
                for k in xrange(2):
                    product[i][j] += self.matrix[i][k] * other.matrix[k][j]
                    product[i][j] %= SortedHash.prime
        self.matrix = product

    def md5(self):
        return "".join(self._num_to_str(self.matrix[i][j]) for i in xrange(2) for j in xrange(2))

    def hex(self):
        return "".join("%08x" % self.matrix[i][j] for i in xrange(2) for j in xrange(2))

def calculate_hash(md5_stream, hash_type):
    total_hash = hash_type()
    for md5_str in md5_stream:
        total_hash.update(hash_type(md5_str))
    return total_hash

def calculate_hash_of_groups(stream, input_type, reduce_by):
    for key, group in itertools.groupby(stream, lambda obj: extract_key(obj, input_type, reduce_by)):
        yield calculate_hash(extract_md5(group, input_type), Hash)

def process_stream(stream, input_type, sorted, reduce_by):
    stream = parse_stream(stream, input_type)
    if sorted:
        key = ""
        hash = None
        if input_type == "md5":
            hash = calculate_hash(extract_md5(stream, input_type), SortedHash)
        else:
            stream, another_stream = itertools.tee(stream)
            try:
                key = extract_key(another_stream.next(), input_type, reduce_by)
                del another_stream
            except StopIteration:
                # Empty input case
                return ""

            group_hashes = calculate_hash_of_groups(stream, input_type, reduce_by)
            hash = calculate_hash(itertools.imap(lambda hash: hash.md5(), group_hashes), SortedHash)

        return "{0}\t{1}\n".format(key, hash.hex())
    else:
        hash = calculate_hash(extract_md5(stream, input_type), Hash)
        return "\t{0}\n".format(hash.hex())


def main():
    parser = ArgumentParser()
    parser.add_argument("--input-type", help="Possible options: yamr, structured or md5.", required=True)
    parser.add_argument("--sorted", action="store_true", default=False)
    parser.add_argument("--table", help="Path to the table. If --table is not set, binary uses stdin.")
    parser.add_argument("--has-subkey", action="store_true", default=False)
    parser.add_argument("--reduce-by", action="append")
    parser.add_argument("--proxy")

    args = parser.parse_args()

    if args.input_type not in ["yamr", "structured", "md5"]:
        raise yt.YtError("Incorrect input_type '{0}'".format(args.input_type))

    if args.table is not None:
        if args.proxy is not None:
            yt.config.set_proxy(args.proxy)

        script = os.path.realpath(__file__)
        cmd = "./{0} --input-type {1} {2}".format(os.path.basename(script), args.input_type, "--sorted" if args.sorted else "")

        reduce_by = None
        if args.sorted:
            if not yt.exists(args.table + "/@sorted_by"):
                die("Cannot calculate sorted checksum if table is not sorted")
            reduce_by = yt.get(args.table + "/@sorted_by")
            cmd += " " + " ".join("--reduce-by " + key for key in reduce_by)

        dst = yt.create_temp_table(prefix="checksum")

        if args.input_type == "yamr":
            input_format = yt.YamrFormat(has_subkey=args.has_subkey, lenval=False)
        if args.input_type == "structured":
            input_format = yt.JsonFormat()
        if args.input_type == "md5":
            input_format = yt.YamrFormat(has_subkey=False, lenval=False)

        output_format = yt.YamrFormat(lenval=False, has_subkey=False)

        if args.sorted:
            yt.run_reduce(cmd, args.table, dst, input_format=input_format, output_format=output_format, local_files=script, reduce_by=reduce_by)
            yt.run_sort(dst, dst, sort_by=["key"])
        else:
            yt.run_map(cmd, args.table, dst, input_format=input_format, output_format=output_format, local_files=script)

        sys.stdout.write(process_stream(yt.read_table(dst, format=output_format), "md5", args.sorted, reduce_by))

    else:
        sys.stdout.write(process_stream(sys.stdin, args.input_type, args.sorted, args.reduce_by))


if __name__ == "__main__":
    main()
