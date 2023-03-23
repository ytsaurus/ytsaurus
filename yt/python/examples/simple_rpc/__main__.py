import yt.wrapper as yt

if __name__ == "__main__":
    # Создаём RPC-клиента.
    client = yt.YtClient("freud", config={"backend": "rpc"})

    # Работаем как с обычным клиентом.
    print(client.list("/"))
