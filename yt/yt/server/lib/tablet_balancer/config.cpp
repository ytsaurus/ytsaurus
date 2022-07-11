#include "config.h"

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

void TBundleTabletBalancerConfig::Register(TRegistrar registrar)
{
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

void TTableTabletBalancerConfig::Register(TRegistrar registrar)
{
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

void TTableTabletBalancerConfig::SetMinTabletSize(std::optional<i64> value)
{
    SetTabletSizeConstraint(&MinTabletSize, value);
}

void TTableTabletBalancerConfig::SetDesiredTabletSize(std::optional<i64> value)
{
    SetTabletSizeConstraint(&DesiredTabletSize, value);
}

void TTableTabletBalancerConfig::SetMaxTabletSize(std::optional<i64> value)
{
    SetTabletSizeConstraint(&MaxTabletSize, value);
}

void TTableTabletBalancerConfig::CheckTabletSizeInequalities() const
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

void TTableTabletBalancerConfig::SetTabletSizeConstraint(std::optional<i64>* member, std::optional<i64> value)
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

} // namespace NYT::NTabletBalancer
