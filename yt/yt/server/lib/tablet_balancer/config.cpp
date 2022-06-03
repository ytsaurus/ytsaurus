#include "config.h"

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

TBundleTabletBalancerConfig::TBundleTabletBalancerConfig()
{
    RegisterParameter("enable_in_memory_cell_balancer", EnableInMemoryCellBalancer)
        .Default(true)
        .Alias("enable_in_memory_balancer");

    RegisterParameter("enable_cell_balancer", EnableCellBalancer)
        .Default(false);

    RegisterParameter("enable_tablet_size_balancer", EnableTabletSizeBalancer)
        .Default(true);

    // COMPAT(savrus) Only for compatibility purpose.
    RegisterParameter("compat_enable_tablet_cell_smoothing", EnableTabletCellSmoothing)
        .Default(true)
        .Alias("enable_tablet_cell_smoothing");

    RegisterParameter("soft_in_memory_cell_balance_threshold", SoftInMemoryCellBalanceThreshold)
        .Default(0.05)
        .Alias("cell_balance_factor");

    RegisterParameter("hard_in_memory_cell_balance_threshold", HardInMemoryCellBalanceThreshold)
        .Default(0.15);

    RegisterParameter("min_tablet_size", MinTabletSize)
        .Default(128_MB);

    RegisterParameter("max_tablet_size", MaxTabletSize)
        .Default(20_GB);

    RegisterParameter("desired_tablet_size", DesiredTabletSize)
        .Default(10_GB);

    RegisterParameter("min_in_memory_tablet_size", MinInMemoryTabletSize)
        .Default(512_MB);

    RegisterParameter("max_in_memory_tablet_size", MaxInMemoryTabletSize)
        .Default(2_GB);

    RegisterParameter("desired_in_memory_tablet_size", DesiredInMemoryTabletSize)
        .Default(1_GB);

    RegisterParameter("tablet_to_cell_ratio", TabletToCellRatio)
        .GreaterThan(0)
        .Default(5.0);

    RegisterParameter("tablet_balancer_schedule", TabletBalancerSchedule)
        .Default();

    RegisterParameter("enable_verbose_logging", EnableVerboseLogging)
        .Default(false);

    RegisterPostprocessor([&] () {
        if (MinTabletSize > DesiredTabletSize) {
            THROW_ERROR_EXCEPTION("\"min_tablet_size\" must be less than or equal to \"desired_tablet_size\"");
        }
        if (DesiredTabletSize > MaxTabletSize) {
            THROW_ERROR_EXCEPTION("\"desired_tablet_size\" must be less than or equal to \"max_tablet_size\"");
        }
        if (MinInMemoryTabletSize >= DesiredInMemoryTabletSize) {
            THROW_ERROR_EXCEPTION("\"min_in_memory_tablet_size\" must be less than \"desired_in_memory_tablet_size\"");
        }
        if (DesiredInMemoryTabletSize >= MaxInMemoryTabletSize) {
            THROW_ERROR_EXCEPTION("\"desired_in_memory_tablet_size\" must be less than \"max_in_memory_tablet_size\"");
        }
        if (SoftInMemoryCellBalanceThreshold > HardInMemoryCellBalanceThreshold) {
            THROW_ERROR_EXCEPTION("\"soft_in_memory_cell_balance_threshold\" must less than or equal to "
                "\"hard_in_memory_cell_balance_threshold\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TTableTabletBalancerConfig::TTableTabletBalancerConfig()
{
    RegisterParameter("enable_auto_reshard", EnableAutoReshard)
        .Default(true);

    RegisterParameter("enable_auto_tablet_move", EnableAutoTabletMove)
        .Default(true);

    RegisterParameter("min_tablet_size", MinTabletSize)
        .Default();

    RegisterParameter("max_tablet_size", MaxTabletSize)
        .Default();

    RegisterParameter("desired_tablet_size", DesiredTabletSize)
        .Default();

    RegisterParameter("desired_tablet_count", DesiredTabletCount)
        .Default();

    RegisterParameter("min_tablet_count", MinTabletCount)
        .Default()
        .GreaterThan(0);

    RegisterParameter("enable_verbose_logging", EnableVerboseLogging)
        .Default(false);

    RegisterPostprocessor([&] {
        CheckTabletSizeInequalities();
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
