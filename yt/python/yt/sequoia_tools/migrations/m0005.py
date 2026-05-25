"""Add small_chunk_id_hash to chunk_replicas table."""

from .. import action_builder as builder, actions, app as sequoia_app, config as cfg


def chunk_replicas_table_mapper(row):
    result = dict([(key, row.get(key)) for key in row])
    result["stored_replicas"] = [result["stored_replicas"], []]
    yield result


def add_small_chunk_id_hash_to_chunk_replicas_table(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="add-small-chunk-id-hash-to-chunk-replicas-table", version=5)
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
        .build())
