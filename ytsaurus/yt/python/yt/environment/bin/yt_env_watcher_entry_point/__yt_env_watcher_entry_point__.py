try:
    import __res  # noqa
except ImportError:
    pass  # Not Arcadia.
else:
    import yt_env_watcher

    yt_env_watcher.main()
