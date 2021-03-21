import yt.wrapper as yt

src_path = "//sys/clickhouse/tests/performance/reference"
dst_path = "//sys/clickhouse/tests/testsets/performance"

rows = list(yt.read_table(src_path, format=yt.YsonFormat(encoding=None)))
new_rows = []
for i, row in enumerate(rows):
    new_row = {
        b"tags": [],
        b"reference": None,
        b"queries": [row[b"query"]],
        b"name": "{:03d}".format(i).encode("utf-8")
    }
    new_rows.append(new_row)

schema = yt.get("//sys/clickhouse/tests/testsets/correctness/@schema")
schema[2]["type_v3"] = {"type_name": "optional", "item": "string"}
del schema[2]["type_v2"]
del schema[2]["type"]
del schema[2]["required"]

yt.remove(dst_path, force=True)
yt.create("table", dst_path, attributes={"schema": schema})
yt.write_table(dst_path, new_rows, format=yt.YsonFormat(encoding=None))
