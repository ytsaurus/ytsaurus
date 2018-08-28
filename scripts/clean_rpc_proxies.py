import yt.wrapper as yw

if __name__ == "__main__":
    proxies = yw.get("//sys/rpc_proxies")
    for proxy, childs in proxies.items():
        if "alive" not in childs:
            yw.remove("//sys/rpc_proxies/" + proxy, recursive=True)
            print proxy, "is down, removing"
