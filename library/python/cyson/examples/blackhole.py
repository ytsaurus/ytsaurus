#!/usr/bin/env python

import cyson


def main():
    input = cyson.list_fragments(
        cyson.InputStream.from_fd(0),
        process_table_index=True,
    )
    for obj in input:
        pass


if __name__ == '__main__':
    main()
