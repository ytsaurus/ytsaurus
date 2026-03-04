import yt.wrapper as yt

from . import descriptors


def get_sequoia_table_descriptors(
    group_names: list[str],
    version: int,
) -> dict[str, descriptors.TableDescriptor]:
    """Return table descriptors from the parsed versioned registry."""
    tds = descriptors.get_table_descriptors(version).as_dict()
    return {k: v for k, v in tds.items() if v.group in group_names}


def list_master_cell_tags(yt_client: yt.YtClient) -> list[str]:
    return ([
        str(
            yt_client.get(
                "//@native_cell_tag",
                suppress_transaction_coordinator_sync=True,
                suppress_upstream_sync=True))
    ] +
        list(
            yt_client.list(
                "//sys/secondary_masters",
                suppress_transaction_coordinator_sync=True,
                suppress_upstream_sync=True)))


def make_ground_reign_path(root_path: str) -> str:
    return f"{root_path}/@ground_reign"
