import yt.wrapper as yt

import sys


def main():
    client = yt.YtClient(sys.argv[1])

    rows = list(
        client.select_rows('[meta.id] from [//yp/db/pod_sets] where [spec.node_segment_id] = ""')
    )

    updates = []
    for row in rows:
        updates.append({"meta.id": row["meta.id"], "spec.node_segment_id": "default"})
    print updates
    client.insert_rows("//yp/db/pod_sets", updates, update=True)


if __name__ == "__main__":
    main()
