#!/usr/bin/env python

import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("cell_id", type=str)
    parser.add_argument("cell_tag", type=int)
    args = parser.parse_args()

    cell_id_parts = map(lambda part: int(part, 16), args.cell_id.split("-"))
    cell_id_parts = list(reversed(cell_id_parts))
    object_cell_tag = (cell_id_parts[1] >> 16) & 0xffff
    object_type = (cell_id_parts[1]) & 0xffff

    print "Old Cell Tag: %d ; Old Type: %d" % (object_cell_tag, object_type)
    cell_id_parts[1] = (args.cell_tag << 16) | 601
    print "New Cell Id: %x-%x-%x-%x" % tuple(reversed(cell_id_parts))

if __name__ == "__main__":
    main()
