import yt.type_info as ti
import yt.wrapper as yt


def init_id_to_path_updater(client: yt.YtClient, path: str, tablet_cell_bundle: str, primary_medium: str) -> None:
    client.create(
        "table",
        path,
        attributes={
            "dynamic": True,
            "schema": yt.schema.TableSchema([
                yt.schema.ColumnSchema("cluster", ti.Optional[ti.String], sort_order="ascending"),
                yt.schema.ColumnSchema("node_id", ti.Optional[ti.String], sort_order="ascending"),
                yt.schema.ColumnSchema("path", ti.Optional[ti.String]),
            ]),
            "tablet_cell_bundle": tablet_cell_bundle,
            "primary_medium": primary_medium,
        },
        recursive=True,
        ignore_existing=True,
    )
    client.mount_table(path, sync=True)
