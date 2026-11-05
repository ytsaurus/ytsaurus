"""Add node_id_hash field to location_replicas table."""

from .. import action_builder as builder, actions, app as sequoia_app


def add_node_id_hash_to_location_replicas_table(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="add-node-id-hash-to-location-replicas-table", version=6)
        .build())
