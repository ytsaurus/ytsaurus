import logging
from dataclasses import dataclass

import frozendict

from yt.admin.ytsaurus_ci.base_client import RegistryClientBase

logger = logging.getLogger(__name__)


@dataclass
class YandexCRAuth:
    token: str
    base_url: str = "https://container-registry.api.cloud.yandex.net"


class YandexContainerRegistryClient(RegistryClientBase):
    def __init__(self, auth: YandexCRAuth, max_retries: int = 3, backoff_factor: float = 1.0):
        self.config = auth
        super().__init__(auth.base_url, {"Authorization": f"Bearer {auth.token}"}, max_retries, backoff_factor)

    def get_image_tags(self, registry_id, image_name):
        endpoint = "/container-registry/v1/images"
        params = {"repositoryName": f"{registry_id}/{image_name}", "pageSize": 100}

        images = []
        while True:
            response = self._make_request("GET", endpoint, params=frozendict.frozendict(params))
            data = response.json()

            images.extend(data.get("images") or [])

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

            params["pageToken"] = next_page_token

        logger.info("yandex_cr: %s/%s: got %s image(s)", registry_id, image_name, len(images))
        return sorted(images, key=lambda image: image.get("createdAt", ""), reverse=True)
