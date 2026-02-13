import os

import yt.wrapper


def main():
    # Creating an RPC client.
    # You need to set up cluster address in YT_PROXY environment variable.
    cluster = os.getenv("YT_PROXY")
    if cluster is None or cluster == "":
        raise RuntimeError("Environment variable YT_PROXY is empty")
    client = yt.wrapper.YtClient(cluster, config={"backend": "rpc"})

    # Working the same way when using standard client.
    print(client.list("/"))


if __name__ == "__main__":
    main()
