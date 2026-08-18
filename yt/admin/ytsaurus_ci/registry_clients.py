from typing import Dict

from yt.admin.ytsaurus_ci import ghcr
from yt.admin.ytsaurus_ci import yandex_cr


def build_clients(auths: Dict[str, object]) -> Dict[str, object]:
    factories = {
        "ghcr": ghcr.GitHubPackagesClient,
        "yandex_cr": yandex_cr.YandexContainerRegistryClient,
    }
    return {registry: factories[registry](auths[registry]) for registry in factories if auths.get(registry)}
