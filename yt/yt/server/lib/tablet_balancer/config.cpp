#include "config.h"

namespace NYT::NTabletBalancer {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TComponentFactorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cell_factor", &TThis::Cell)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("node_factor", &TThis::Node)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("table_cell_factor", &TThis::TableCell)
        .Optional()
        .GreaterThanOrEqual(0);
    registrar.Parameter("table_node_factor", &TThis::TableNode)
        .Optional()
        .GreaterThanOrEqual(0);
}

TComponentFactorConfigPtr TComponentFactorConfig::MakeDefaultIdentity()
{
    auto config = New<TComponentFactorConfig>();

    config->Cell = 1;
    config->Node = 1;
    config->TableCell = 0;
    config->TableNode = 0;

    return config;
}

TComponentFactorConfigPtr TComponentFactorConfig::MakeUniformIdentity()
{
    auto config = New<TComponentFactorConfig>();

    config->Cell = 1;
    config->Node = 1;
    config->TableCell = 1;
    config->TableNode = 1;

    return config;
}

TComponentFactorConfigPtr TComponentFactorConfig::MergeWith(
    const TComponentFactorConfigPtr& otherConfig) const
{
    auto config = New<TComponentFactorConfig>();

    auto coalesce = [] (auto lhs, auto rhs) {
        return lhs ? lhs : rhs;
    };

    config->Cell = coalesce(otherConfig->Cell, Cell);
    config->Node = coalesce(otherConfig->Node, Node);
    config->TableCell = coalesce(otherConfig->TableCell, TableCell);
    config->TableNode = coalesce(otherConfig->TableNode, TableNode);

    return config;
}

////////////////////////////////////////////////////////////////////////////////

void TParameterizedBalancingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_reshard", &TThis::EnableReshard)
        .Default();
    registrar.Parameter("enable_uniform", &TThis::EnableUniform)
        .Default();
    registrar.Parameter("metric", &TThis::Metric)
        .Default();
    registrar.Parameter("max_action_count", &TThis::MaxActionCount)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("bounded_priority_queue_size", &TThis::BoundedPriorityQueueSize)
        .Default(10'000)
        .GreaterThan(0);
    registrar.Parameter("node_deviation_threshold", &TThis::NodeDeviationThreshold)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("cell_deviation_threshold", &TThis::CellDeviationThreshold)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("min_relative_metric_improvement", &TThis::MinRelativeMetricImprovement)
        .Default()
        .GreaterThanOrEqual(0);
    registrar.Parameter("factors", &TThis::Factors)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->EnableUniform.value_or(false) && !config->Factors) {
            config->Factors = TComponentFactorConfig::MakeUniformIdentity();
        } else if (!config->Factors) {
            config->Factors = New<TComponentFactorConfig>();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTabletBalancingGroupConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_move", &TThis::EnableMove)
        .Default(true);
    registrar.Parameter("enable_reshard", &TThis::EnableReshard)
        .Default(true);
    registrar.Parameter("type", &TThis::Type)
        .Default(EBalancingType::Parameterized);
    registrar.Parameter("parameterized", &TThis::Parameterized)
        .DefaultNew();
    registrar.Parameter("schedule", &TThis::Schedule)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMasterBundleTabletBalancerConfig::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("enable_in_memory_cell_balancer", &TThis::EnableInMemoryCellBalancer)
        .Default(true)
        .Alias("enable_in_memory_balancer");

    registrar.Parameter("enable_cell_balancer", &TThis::EnableCellBalancer)
        .Default(false);

    registrar.Parameter("enable_tablet_size_balancer", &TThis::EnableTabletSizeBalancer)
        .Default(true);

    registrar.Parameter("enable_standalone_tablet_balancer", &TThis::EnableStandaloneTabletBalancer)
        .Default(false)
        .DontSerializeDefault();

    // COMPAT(savrus) Only for compatibility purpose.
    registrar.Parameter("compat_enable_tablet_cell_smoothing", &TThis::EnableTabletCellSmoothing)
        .Default(true)
        .Alias("enable_tablet_cell_smoothing");

    registrar.Parameter("soft_in_memory_cell_balance_threshold", &TThis::SoftInMemoryCellBalanceThreshold)
        .Default(0.05)
        .Alias("cell_balance_factor");

    registrar.Parameter("hard_in_memory_cell_balance_threshold", &TThis::HardInMemoryCellBalanceThreshold)
        .Default(0.15);

    registrar.Parameter("min_tablet_size", &TThis::MinTabletSize)
        .Default(128_MB);

    registrar.Parameter("max_tablet_size", &TThis::MaxTabletSize)
        .Default(20_GB);

    registrar.Parameter("desired_tablet_size", &TThis::DesiredTabletSize)
        .Default(10_GB);

    registrar.Parameter("min_in_memory_tablet_size", &TThis::MinInMemoryTabletSize)
        .Default(512_MB);

    registrar.Parameter("max_in_memory_tablet_size", &TThis::MaxInMemoryTabletSize)
        .Default(2_GB);

    registrar.Parameter("desired_in_memory_tablet_size", &TThis::DesiredInMemoryTabletSize)
        .Default(1_GB);

    registrar.Parameter("tablet_to_cell_ratio", &TThis::TabletToCellRatio)
        .GreaterThan(0)
        .Default(5.0);

    registrar.Parameter("tablet_balancer_schedule", &TThis::TabletBalancerSchedule)
        .Default();

    registrar.Parameter("enable_verbose_logging", &TThis::EnableVerboseLogging)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        if (config->MinTabletSize > config->DesiredTabletSize) {
            THROW_ERROR_EXCEPTION("\"min_tablet_size\" must be less than or equal to \"desired_tablet_size\"");
        }
        if (config->DesiredTabletSize > config->MaxTabletSize) {
            THROW_ERROR_EXCEPTION("\"desired_tablet_size\" must be less than or equal to \"max_tablet_size\"");
        }
        if (config->MinInMemoryTabletSize >= config->DesiredInMemoryTabletSize) {
            THROW_ERROR_EXCEPTION("\"min_in_memory_tablet_size\" must be less than \"desired_in_memory_tablet_size\"");
        }
        if (config->DesiredInMemoryTabletSize >= config->MaxInMemoryTabletSize) {
            THROW_ERROR_EXCEPTION("\"desired_in_memory_tablet_size\" must be less than \"max_in_memory_tablet_size\"");
        }
        if (config->SoftInMemoryCellBalanceThreshold > config->HardInMemoryCellBalanceThreshold) {
            THROW_ERROR_EXCEPTION("\"soft_in_memory_cell_balance_threshold\" must less than or equal to "
                "\"hard_in_memory_cell_balance_threshold\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TBundleTabletBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_parameterized_by_default", &TThis::EnableParameterizedByDefault)
        .Default(false);
    registrar.Parameter("default_in_memory_group", &TThis::DefaultInMemoryGroup)
        .Default();
    registrar.Parameter("groups", &TThis::Groups)
        .Default();
    registrar.Parameter("enable_pick_pivot_keys", &TThis::EnablePickPivotKeys)
        .Default(true);
    registrar.Parameter("safe_used_tablet_static_ratio", &TThis::SafeUsedTabletStaticRatio)
        .Default(1.)
        .InRange(0., 1.);

    registrar.Postprocessor([] (TThis* config) {
        auto [it, inserted] = config->Groups.emplace(DefaultGroupName, New<TTabletBalancingGroupConfig>());
        if (inserted) {
            it->second->Postprocess();
        }

        if (auto it = config->Groups.emplace(LegacyGroupName, New<TTabletBalancingGroupConfig>()); it.second) {
            it.first->second->Type = EBalancingType::Legacy;
        }

        if (auto it = config->Groups.emplace(LegacyInMemoryGroupName, New<TTabletBalancingGroupConfig>()); it.second) {
            it.first->second->Type = EBalancingType::Legacy;
        }

        for (const auto& [groupName, groupConfig] : config->Groups) {
            if (groupConfig->Type == EBalancingType::Legacy) {
                THROW_ERROR_EXCEPTION_IF(
                    groupName != LegacyGroupName && groupName != LegacyInMemoryGroupName,
                    "Group %Qv is not builtin but has legacy type",
                    groupName);
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TMasterTableTabletBalancerConfig::Register(TRegistrar registrar)
{
    registrar.UnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);

    registrar.Parameter("enable_auto_reshard", &TThis::EnableAutoReshard)
        .Default(true);

    registrar.Parameter("enable_auto_tablet_move", &TThis::EnableAutoTabletMove)
        .Default(true);

    registrar.Parameter("min_tablet_size", &TThis::MinTabletSize)
        .Default();

    registrar.Parameter("max_tablet_size", &TThis::MaxTabletSize)
        .Default();

    registrar.Parameter("desired_tablet_size", &TThis::DesiredTabletSize)
        .Default();

    registrar.Parameter("desired_tablet_count", &TThis::DesiredTabletCount)
        .Default();

    registrar.Parameter("min_tablet_count", &TThis::MinTabletCount)
        .Default()
        .GreaterThan(0);

    registrar.Parameter("enable_verbose_logging", &TThis::EnableVerboseLogging)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        config->CheckTabletSizeInequalities();
    });
}

void TMasterTableTabletBalancerConfig::SetMinTabletSize(std::optional<i64> value)
{
    SetTabletSizeConstraint(&MinTabletSize, value);
}

void TMasterTableTabletBalancerConfig::SetDesiredTabletSize(std::optional<i64> value)
{
    SetTabletSizeConstraint(&DesiredTabletSize, value);
}

void TMasterTableTabletBalancerConfig::SetMaxTabletSize(std::optional<i64> value)
{
    SetTabletSizeConstraint(&MaxTabletSize, value);
}

void TMasterTableTabletBalancerConfig::CheckTabletSizeInequalities() const
{
    if (MinTabletSize && DesiredTabletSize && *MinTabletSize > *DesiredTabletSize) {
        THROW_ERROR_EXCEPTION("\"min_tablet_size\" must be less than or equal to \"desired_tablet_size\"");
    }
    if (DesiredTabletSize && MaxTabletSize && *DesiredTabletSize > *MaxTabletSize) {
        THROW_ERROR_EXCEPTION("\"desired_tablet_size\" must be less than or equal to \"max_tablet_size\"");
    }
    if (DesiredTabletCount && MinTabletCount && *MinTabletCount > *DesiredTabletCount) {
        THROW_ERROR_EXCEPTION("\"min_tablet_count\" must be less than or equal to \"desired_tablet_count\"");
    }
}

void TMasterTableTabletBalancerConfig::SetTabletSizeConstraint(std::optional<i64>* member, std::optional<i64> value)
{
    auto oldValue = *member;
    try {
        *member = value;
        CheckTabletSizeInequalities();
    } catch (const std::exception& ex) {
        *member = oldValue;
        throw;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TTableTabletBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_parameterized", &TThis::EnableParameterized)
        .Default();
    registrar.Parameter("group", &TThis::Group)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
