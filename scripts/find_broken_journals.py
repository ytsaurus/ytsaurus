import yt.wrapper as yt

cell_ids = yt.list("//sys/tablet_cells", attributes=["health"])
nodes = set()
for x in cell_ids:
	if x.attributes["health"] != "good":
		cell_id = str(x)
		logs = sorted(yt.list("//sys/tablet_cells/{0}/changelogs".format(cell_id)))
		last_log = logs[-1]
		chunk_ids = yt.get("//sys/tablet_cells/{0}/changelogs/{1}/@chunk_ids".format(cell_id, last_log))
		chunk_id = chunk_ids[-1]
		replicas = yt.get("//sys/chunks/{0}/@stored_replicas".format(chunk_id))
		for replica in replicas:
			if replica.attributes["medium"] == "ssd_blobs":
				nodes.add(str(replica))
for node in nodes:
	print node


