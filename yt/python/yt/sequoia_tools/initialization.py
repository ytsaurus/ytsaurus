"""Sequoia ground cluster initialization logic."""

from . import action_builder as builder, actions, app as sequoia_app, config as cfg, helpers


def initialize_ground(app: sequoia_app.SequoiaTool, target_reign: int) -> None:
    """Initialize Sequoia ground cluster.

    Set up the ground cluster for Sequoia for the first time or validate the
    current configuration and metadata.
    """
    def setup_environment(app: sequoia_app.SequoiaTool):
        ground_config = app.config_provider.get_ground_config()

        result: list[actions.Action] = []

        if ground_config.account_resource_limits:
            result.extend(
                actions.SetAttributeAction(path, value)
                for path, value in cfg.flatten(
                    ground_config.account_resource_limits,
                    f"//sys/accounts/{ground_config.account}/@resource_limits"))

        if ground_config.master_dynamic_config:
            result.append(actions.ValidateConfigAction(
                get_config=lambda app: app.ground_client.get("//sys/@config"),
                description="Validate master dynamic config",
                expected=ground_config.master_dynamic_config))

        if ground_config.master_static_config:
            def get_master_static_config(app: sequoia_app.SequoiaTool):
                master = app.ground_client.list("//sys/primary_masters")[0]
                return app.ground_client.get(
                    f"//sys/primary_masters/{master}/orchid/config")

            result.append(actions.ValidateConfigAction(
                get_config=get_master_static_config,
                description="Validate master static config",
                expected=ground_config.master_static_config))

        return result

    def setup_component(ctx: builder.ComponentContext):
        result: list[actions.Action] = []

        if ctx.config.tablet_cell_count > 0:
            bundle = ctx.config.tablet_cell_bundle

            if ctx.ground_config.tablet_node_static_config:
                def get_tablet_node_static_config(app: sequoia_app.SequoiaTool):
                    peer = helpers.get_tablet_cell_peer(app, bundle)
                    return app.ground_client.get(
                        "//sys/tablet_nodes/{}/orchid/config".format(peer["address"]))

                result.append(actions.ValidateConfigAction(
                    get_config=get_tablet_node_static_config,
                    description=f'Validate tablet node static config for bundle "{bundle}"',
                    expected=ctx.ground_config.tablet_node_static_config))

            if ctx.ground_config.tablet_node_dynamic_config:
                def get_tablet_node_dynamic_config(app: sequoia_app.SequoiaTool):
                    peer = helpers.get_tablet_cell_peer(app, bundle)
                    return app.ground_client.get(
                        "//sys/tablet_nodes/{}/orchid/dynamic_config_manager/effective_config"
                        .format(peer["address"]))

                result.append(actions.ValidateConfigAction(
                    get_config=get_tablet_node_dynamic_config,
                    description=f'Validate tablet node dynamic config for bundle "{bundle}"',
                    expected=ctx.ground_config.tablet_node_dynamic_config))

        return result

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
        .with_component_factory(setup_component)
        .for_each_table()
        .with_table_factory(
            lambda ctx: [
                actions.CreateTableAction(
                    ctx.name,
                    ctx.parent_path,
                    ctx.descriptor.schema,
                    ctx.attributes,
                    ctx.pivot_keys)
            ])
        .then()
        .build())

    actions.run_action_plan(plan, app)
    helpers.promote_reign(app, target_reign)


def _make_mount_action_plan(
    app: sequoia_app.SequoiaTool,
    ground_reign: int,
    unmount: bool,
) -> actions.ActionPlan:
    plan_name = "unmount-tables" if unmount else "mount-tables"
    return (
        builder.ActionBuilder(app, plan_name, version=ground_reign)
        .for_each_table()
        .with_table_factory(
            lambda ctx: [
                actions.MountTabletAction(ctx.path, unmount)
            ])
        .then()
        .build())


def mount_tables(app: sequoia_app.SequoiaTool, ground_reign: int) -> None:
    """Mount Sequoia ground dynamic tables."""
    plan = _make_mount_action_plan(app, ground_reign, unmount=False)
    actions.run_action_plan(plan, app)


def unmount_tables(app: sequoia_app.SequoiaTool, ground_reign: int) -> None:
    """Unmount Sequoia ground dynamic tables."""
    plan = _make_mount_action_plan(app, ground_reign, unmount=True)
    actions.run_action_plan(plan, app)
