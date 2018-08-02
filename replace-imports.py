#!/usr/bin/env python

import sys


def main():
    protobuf_interop_fields = (
        "NYT.NYson.NProto.attribute_dictionary",
        "NYT.NYson.NProto.enum_value_name",
        "NYT.NYson.NProto.yson_string",
        "NYT.NYson.NProto.yson_map",
        "NYT.NYson.NProto.derive_underscore_case_names",
    )
    for line in sys.stdin:
        if line.strip().startswith("import") and "protobuf_interop.proto" not in line:
            line = line.replace("\"yt/", "\"yt_proto/yt/")
            line = line.replace("\"yp/", "\"yp_proto/yp/")
        if "NYT." in line and all([item not in line for item in protobuf_interop_fields]):
            line = line.replace("NYT.", "NYtPython.")
        sys.stdout.write(line)


if __name__ == "__main__":
    main()
