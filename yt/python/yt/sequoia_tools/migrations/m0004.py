"""Add replica state field to location_replicas table."""

from .. import action_builder as builder, actions, app as sequoia_app, config as cfg


def location_replicas_table_mapper(row):
    result = dict([(key, row.get(key)) for key in row])
    result["replica_state"] = 0
    result.pop("fake")
    yield result


def alter_location_replicas_table(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="add-replica-state-to-location-replicas-table", version=4)
        .for_table(
            scope=cfg.Scope.REPLICAS,
            table_name="location_replicas")
        .with_table_factory(
            lambda ctx: [
                builder.ConversionAction(
                    ctx,
                    source="location_replicas",
                    operation="map",
                    operation_args={
                        "binary": location_replicas_table_mapper,
                    })
            ])
        .then()
        .promote_reign()
        .build())
