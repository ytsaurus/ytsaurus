import yt.wrapper as yt

import sys

def fix_object_type_for_erasure_chunk(chunk_id):
    parts = map(lambda s: int(s, 16), chunk_id.split("-"))
    if parts[2] % (2 ** 16) > 100:
        parts[2] = (parts[2] / 2 ** 16) * (2 ** 16) + 102
    return "-".join(map(lambda i: hex(i)[2:], parts))

def erase_chunk(path, chunk_id):
    chunk_ids = yt.get(path + "/@chunk_ids")
    index = chunk_ids.index(chunk_id)
    yt.copy(path, path + ".original")
    yt.run_merge(yt.TablePath(path, ranges=[{"upper_limit": {"chunk_index": index}}, {"lower_limit": {"chunk_index": index + 1}}]), path + ".fixed")
    yt.run_merge(path + ".fixed", path)
    print >>sys.stderr, "Chunk", chunk_id, "erased from table", path
    print >>sys.stderr, "Write owners of the table", path, "about that and then remove .fixed and .original tables"

def main():
    chunk_id = sys.argv[1]
    original_chunk_id = fix_object_type_for_erasure_chunk(chunk_id)
    paths = yt.get("#{}/@owning_nodes".format(original_chunk_id))
    for path in paths:
        erase_chunk(path, original_chunk_id)

if __name__ == "__main__":
    main()
