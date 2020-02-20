import yt.wrapper as yt

import sys


def main():
    client = yt.YtClient(sys.argv[1])

    schema = client.get("//yp/db/nodes/@schema")

    print "Old schema:", schema

    for x in schema:
        if x["name"] == "status.etc":
            x["lock"] = "heartbeat"

    print "New schema:", schema

    client.alter_table("//yp/db/nodes", schema=schema)


if __name__ == "__main__":
    main()
