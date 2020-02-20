import yt.wrapper as yt

import sys


def main():
    client = yt.YtClient(sys.argv[1])

    rows = list(
        client.select_rows(
            "[meta.id], [meta.pod_set_id] from [//yp/db/pods] where is_null([spec.secrets])"
        )
    )

    updates = []
    for row in rows:
        updates.append(
            {
                "meta.id": row["meta.id"],
                "meta.pod_set_id": row["meta.pod_set_id"],
                "spec.secrets": {},
            }
        )
    print updates
    client.insert_rows("//yp/db/pods", updates, update=True)


if __name__ == "__main__":
    main()
