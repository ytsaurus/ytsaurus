def install_hint(package: str) -> str:
    from importlib.metadata import packages_distributions

    dists = packages_distributions().get("yt", [])
    for name in ("ytsaurus-client", "yandex-yt"):
        if name in dists:
            return f"pip install '{name}[admin]'"
    return f"pip install {package}"
