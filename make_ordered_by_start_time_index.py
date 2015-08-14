import yt.wrapper as yt
import yt.yson as yson

yt.config.VERSION = "v3"

archive = yson.load(yt.select_rows("* from [//sys/operations_archive/ordered_by_id]",format=yt.YsonFormat()), "list_fragment")

for row in archive:
	yt.insert_rows("//sys/operations_archive/ordered_by_start_time", yson.dumps({"id": row["id"], "start_time": row["start_time"], "dummy": "null"}), format=yt.YsonFormat())


