"""Add hash field to node_id_to_path and response_keeper tables."""

from .. import action_builder as builder, actions, app as sequoia_app, config as cfg


def add_hash_to_remaining_tables(app: sequoia_app.SequoiaTool) -> actions.ActionPlan:
    return (
        builder.ActionBuilder(app, name="add-hash-to-remaining-tables", version=7)
        .for_table(
            scope=cfg.Scope.SEQUOIA,
            table_name="node_id_to_path")
        .with_table_factory(
            lambda ctx: [
                builder.ConversionAction(
                    ctx,
                    source="node_id_to_path",
                    use_default_mapper=True)
            ])
        .then()
        .for_table(
            scope=cfg.Scope.SEQUOIA,
            table_name="response_keeper")
        .with_table_factory(
            lambda ctx: [
                builder.ConversionAction(
                    ctx,
                    source="response_keeper",
                    use_default_mapper=True)
            ])
        .then()
        .build())
