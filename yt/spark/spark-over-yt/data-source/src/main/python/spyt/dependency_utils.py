def require_yt_client():
    try:
        import yt.wrapper
        from yt.wrapper import YtClient
    except Exception as e:
        raise ImportError(
            "Please install ytsaurus-client (or yandex-yt for internal Yandex users). "
            "These libraries cannot be installed at the same time"
        ) from e
