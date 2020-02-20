import yt.wrapper as yt
import sys


def main():
    client = yt.YtClient(sys.argv[1])

    rows = list(client.select_rows("[meta.id], [meta.acl] from [//yp/db/pod_sets]"))

    updates = []
    for row in rows:
        acl = row["meta.acl"]
        if len(acl) != 1:
            continue
        permissions = acl[0]["permissions"]
        new_permissions = filter(lambda p: p != "read_secrets", permissions)
        if permissions != new_permissions:
            row["meta.acl"][0]["permissions"] = new_permissions
            updates.append(row)
    print updates
    client.insert_rows("//yp/db/pod_sets", updates, update=True)


if __name__ == "__main__":
    main()
