def confirm(message: str, *, assume_yes: bool = False) -> bool:
    if assume_yes:
        return True
    try:
        return input(f"{message} [y/N] ").strip().lower() in ("y", "yes")
    except EOFError:
        return False


def install_hint(package: str) -> str:
    from importlib.metadata import packages_distributions

    dists = packages_distributions().get("yt", [])
    for name in ("ytsaurus-client", "yandex-yt"):
        if name in dists:
            return f"pip install '{name}[admin]'"
    return f"pip install {package}"
