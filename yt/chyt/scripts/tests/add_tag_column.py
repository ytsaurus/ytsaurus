import yt.wrapper as yt

path = "//sys/clickhouse/tests/testsets/correctness"

rows = list(yt.read_table(path, format=yt.YsonFormat(encoding=None)))
for row in rows:
    row[b"tags"] = []

schema = yt.get(path + "/@schema")
schema.append({"name": "tags", "type_v3": {"type_name": "list", "item": "string"}})

yt.remove(path)
yt.create("table", path, attributes={"schema": schema})
yt.write_table(path, rows, format=yt.YsonFormat(encoding=None))

