#!/usr/bin/python

REVISION = 347745
SCENARIO = """
cmake
generated
util

dict/
    json
    dictutil

library/
    unittest

quality/
    Misc
    NetLiba
    mapreducelib
    random
    util

contrib/
    tools/
        byacc
        gperf
        protoc
        ragel5
        ragel6
        yasm
    libs/
        fastlz
        libbz2
        libiconv
        lzmasdk
        minilzo
        pcre
        protobuf
        quicklz
        yajl
        zlib
"""

def play(iterable):
    stack = []
    level = 0

    for line in iterable:
        level = (len(line) - len(line.lstrip())) / 4
        line = line.strip()

        if not line:
            continue

        if line.endswith("/"):
            stack = stack[:level]
            stack.append(line.rstrip("/"))
        else:
            yield "/".join(stack + [ line ])

def arrange(scenario, revision):
    for item in play(scenario.split("\n")):
        print "{module:32s} -r {revision} svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/{module}".format(
            module = item,
            revision = revision
        )

if __name__ == "__main__":
    arrange(SCENARIO, REVISION)

