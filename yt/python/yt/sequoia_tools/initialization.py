"""Sequoia ground cluster initialization logic."""

from . import action_builder as builder, actions, app as sequoia_app, config as cfg


def initialize_ground(app: sequoia_app.SequoiaTool, target_reign: int) -> None:
    """Initialize Sequoia ground cluster.

    Set up the ground cluster for Sequoia for the first time or validate the
    current configuration and metadata.
    """
    def setup_environment(app: sequoia_app.SequoiaTool):
        ground_config = app.config_provider.get_ground_config()
        dynamic_attributes = (
            cfg.flatten(
                ground_config.master_dynamic_config,
                "//sys/@config") +
            cfg.flatten(
                ground_config.account_resource_limits,
                f"//sys/accounts/{ground_config.account}/@resource_limits"))
        return [
            actions.SetAttributeAction(path, value)
            for path, value in dynamic_attributes
        ]

    plan = (
        builder.ActionBuilder(app, "ground-cluster-initialization", version=target_reign)
        .with_factory(setup_environment)
        .with_component_factory(
            lambda ctx: [
                actions.CreateTabletCellBundleAction(
                    ctx.config.tablet_cell_bundle,
                    ctx.config.tablet_cell_bundle_config),
                actions.CreateTabletCellsAction(
                    ctx.config.tablet_cell_bundle,
                    ctx.config.tablet_cell_count),
            ])
        .for_each_table()
        .with_table_factory(
            lambda ctx: [
                actions.CreateTableAction(
                    ctx.name,
                    ctx.parent_path,
                    ctx.descriptor.schema,
                    ctx.attributes)
            ])
        .then()
        .promote_reign(initialize=True)
        .build())

    actions.run_action_plan(plan, app)


def _make_mount_action_plan(
    app: sequoia_app.SequoiaTool,
    ground_reign: int,
    unmount: bool,
    **kwargs,
) -> actions.ActionPlan:
    plan_name = "unmount-tables" if unmount else "mount-tables"
    return (
        builder.ActionBuilder(app, plan_name, version=ground_reign)
        .for_each_table()
        .with_table_factory(
            lambda ctx: [
                actions.MountTabletAction(ctx.path, unmount, **kwargs)
            ])
        .then()
        .build())


def mount_tables(app: sequoia_app.SequoiaTool, ground_reign: int, sync: bool = False) -> None:
    """Mount Sequoia ground dynamic tables."""
    plan = _make_mount_action_plan(app, ground_reign, unmount=False, sync=sync)
    actions.run_action_plan(plan, app)


def unmount_tables(app: sequoia_app.SequoiaTool, ground_reign: int, sync: bool = False) -> None:
    """Unmount Sequoia ground dynamic tables."""
    plan = _make_mount_action_plan(app, ground_reign, unmount=True, sync=sync)
    actions.run_action_plan(plan, app)
