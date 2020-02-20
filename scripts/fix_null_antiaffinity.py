import yt.wrapper as yt

import sys


def main():
    client = yt.YtClient(sys.argv[1])

    rows = list(
        client.select_rows(
            "[meta.id] from [//yp/db/pod_sets] where is_null([spec.antiaffinity_constraints])"
        )
    )

    updates = []
    for row in rows:
        updates.append({"meta.id": row["meta.id"], "spec.antiaffinity_constraints": []})
    print updates
    client.insert_rows("//yp/db/pod_sets", updates, update=True)


if __name__ == "__main__":
    main()
