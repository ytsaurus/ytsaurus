"""Add cell_tag and shard_index fields to chunk_replicas table."""

from .. import action_builder as builder, actions, app as sequoia_app, config as cfg


CHUNK_SHARD_COUNT = 60


def parse_guid(guid):
    id_parts = guid.split("-")
    return [int(part, 16) for part in reversed(id_parts)]


def get_chunk_shard_index(id_parts):
    return id_parts[0] % CHUNK_SHARD_COUNT


def get_chunk_cell_tag(id_parts):
    return id_parts[1] >> 16


def chunk_replicas_table_mapper(row):
    result = dict([(key, row.get(key)) for key in row])
    id_parts = parse_guid(result["chunk_id"])
    result["cell_tag"] = get_chunk_cell_tag(id_parts)
    result["shard_index"] = get_chunk_shard_index(id_parts)
    yield result


def alter_chunk_replicas_table(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="alter-chunk-replicas-table", version=3)
        .for_table(
            scope=cfg.Scope.REPLICAS,
            table_name="chunk_replicas")
        .with_table_factory(
            lambda ctx: [
                builder.ConversionAction(
                    ctx,
                    source="chunk_replicas",
                    operation="map",
                    operation_args={
                        "binary": chunk_replicas_table_mapper,
                    })
            ])
        .then()
        .promote_reign()
        .build())
