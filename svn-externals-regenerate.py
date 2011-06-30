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
    current_stack = []
    current_level = 0

    for line in iterable:
        line = line.rstrip("\r\n")

        if not line:
            continue

        current_level = (len(line) - len(line.lstrip())) / 4

        if line.endswith("/"):
            current_stack = current_stack[0: current_level]
            current_stack.append(line.strip().rstrip("/"))
        else:
            yield "/".join(current_stack + [ line.strip() ])

def arrange(scenario, revision):
    for item in play(scenario.split("\n")):
        print "{module:32s} -r {revision} svn+ssh://arcadia.yandex.ru/arc/trunk/arcadia/{module}".format(
            module = item,
            revision = revision
        )

if __name__ == "__main__":
    arrange(SCENARIO, REVISION)



