"""Add small_chunk_id_hash to chunk_replicas table."""

from .. import action_builder as builder, actions, app as sequoia_app


def add_small_chunk_id_hash_to_chunk_replicas_table(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="add-small-chunk-id-hash-to-chunk-replicas-table", version=5)
        .build())
