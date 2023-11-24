from typing import Optional, NamedTuple, NoReturn, List

from yt.wrapper import YtClient

from .local_manager import Versions
from .utils import configure_logger

logger = configure_logger("Remote manager")


class PublishConfig(NamedTuple):
    skip_spark_fork: bool = False
    specific_global_file: Optional[str] = None
    ignore_existing: bool = False
    snapshot_ttl: int = 14 * 24 * 60 * 60 * 1000
    include_livy: bool = False


class ClientBuilder(NamedTuple):
    root_path: str = "//home/spark"
    yt_client: Optional[YtClient] = None


class Client:
    def __init__(self, client_builder: ClientBuilder):
        from yt.wrapper.default_config import get_config_from_env
        self.yt_client = client_builder.yt_client or YtClient(config=get_config_from_env())
        self.root_path = client_builder.root_path

    def resolve_from_root(self, path):
        return f"{self.root_path}/{path}"

    def mkdir(self, target_path: str, ttl: Optional[int] = None,
              recursive: bool = True, ignore_existing: bool = True) -> NoReturn:
        full_target_path = self.resolve_from_root(target_path)
        logger.debug(f"Creation map node {full_target_path}")
        attributes = {}
        if ttl is not None:
            attributes["expiration_timeout"] = ttl
        self.yt_client.create("map_node", path=full_target_path, ignore_existing=ignore_existing,
                              recursive=recursive, attributes=attributes)

    def link(self, target_path: str, link_path: str) -> NoReturn:
        full_target_path = self.resolve_from_root(target_path)
        full_link_path = self.resolve_from_root(link_path)
        logger.debug(f"Creation link {full_target_path} -> {full_link_path}")
        self.yt_client.link(full_target_path, full_link_path, ignore_existing=True)

    def resolve_link(self, link_path: str):
        full_link_path = self.resolve_from_root(link_path)
        logger.debug(f"Resolving link {full_link_path}")
        return self.yt_client.get(f"{full_link_path}&/@target_path")

    def read_file(self, target_path: str, local_file_path: str) -> NoReturn:
        full_target_path = self.resolve_from_root(target_path)
        logger.debug(f"Reading file {full_target_path} to {local_file_path}")
        with open(local_file_path, 'wb') as file:
            stream = self.yt_client.read_file(full_target_path)
            file.write(stream)

    def write_file(self, local_file_path: str, target_path: str, executable: bool = False) -> NoReturn:
        full_target_path = self.resolve_from_root(target_path)
        logger.debug(f"Writing {'executable' if executable else ''} file {local_file_path} to {full_target_path}")
        with open(local_file_path, 'rb') as file:
            self.yt_client.write_file(full_target_path, file)
        if executable:
            self.yt_client.set(full_target_path + "/@executable", True)

    def write_document(self, local_file_path: str, target_path: str) -> NoReturn:
        full_target_path = self.resolve_from_root(target_path)
        logger.debug(f"Writing document {local_file_path} to {full_target_path}")
        self.yt_client.create("document", full_target_path, ignore_existing=True)
        with open(local_file_path, 'r') as file:
            document = file.read()
        self.yt_client.set(full_target_path, document, format="json")

    def read_document(self, target_path: str, local_file_path: str) -> NoReturn:
        full_target_path = self.resolve_from_root(target_path)
        logger.debug(f"Reading document {full_target_path} to {local_file_path}")
        with open(local_file_path, 'w') as file:
            document = self.yt_client.get(full_target_path, format="json")
            file.write(document)

    def list_dir(self, target_path: str) -> List[str]:
        full_target_path = self.resolve_from_root(target_path)
        logger.debug(f"Listing directory {full_target_path}")
        result = self.yt_client.list(full_target_path)
        return result


def bin_remote_dir(versions: Versions):
    return f"bin/{versions.cluster_version.get_version_directory()}"


def conf_remote_dir(versions: Optional[Versions] = None):
    if versions is None:
        return "conf"
    return f"conf/{versions.cluster_version.get_version_directory()}"


def spark_remote_dir(versions: Versions):
    return f"spark/{versions.spark_version.get_version_directory()}"


def spyt_remote_dir(versions: Versions):
    return f"spyt/{versions.client_version.get_version_directory()}"
