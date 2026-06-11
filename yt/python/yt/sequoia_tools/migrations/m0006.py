"""Add node_id_hash field to location_replicas table."""

from .. import action_builder as builder, actions, app as sequoia_app, config as cfg


def add_node_id_hash_to_location_replicas_table(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="add-node-id-hash-to-location-replicas-table", version=6)
        .for_table(
            scope=cfg.Scope.REPLICAS,
            table_name="location_replicas")
        .with_table_factory(
            lambda ctx: [
                builder.ConversionAction(
                    ctx,
                    source="location_replicas",
                    use_default_mapper=True)
            ])
        .then()
        .build())
