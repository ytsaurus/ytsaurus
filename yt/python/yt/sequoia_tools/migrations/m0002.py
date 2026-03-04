"""Rename the child node table."""

from .. import action_builder as builder, actions, app as sequoia_app, config as cfg


def alter_child_node_table(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="alter-child-node-table", version=2)
        .for_table(
            scope=cfg.Scope.SEQUOIA,
            table_name="child_nodes")
        .with_table_factory(
            lambda ctx: [
                builder.ConversionAction(
                    ctx,
                    source="child_node",
                    use_default_mapper=True)
            ])
        .then()
        .promote_reign()
        .build())
