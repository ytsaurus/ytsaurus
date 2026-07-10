#include "spec.h"

#include <util/generic/hash_set.h>

#include <yt/yt/flow/library/cpp/common/yt_path_option.h>

#include <yt/yt/library/re2/re2.h>

namespace NYT::NFlow::NStaticTableConnectorV2 {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TTableTimestampLocatorSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("attribute", &TThis::Attribute);
    registrar.Parameter("format", &TThis::Format)
        .Default(ETimestampFormat::Iso8601);
}

////////////////////////////////////////////////////////////////////////////////

void TTableSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("tables", &TThis::Tables)
        .Default()
        .AddOption(EYTPathOwnership::ReadOnly);
    registrar.Parameter("tables_path", &TThis::TablesPath)
        .Default()
        .AddOption(EYTPathOwnership::ReadOnly);

    registrar.Parameter("table_name_filter", &TThis::TableNameFilter)
        .Default();

    registrar.Parameter("event_timestamp_locator", &TThis::EventTimestampLocator)
        .DefaultNew();
    registrar.Parameter("system_timestamp_locator", &TThis::SystemTimestampLocator)
        .DefaultNew();

    registrar.Parameter("ignore_symlinks", &TThis::IgnoreSymlinks)
        .Default(false);

    registrar.Parameter("skip_non_table_nodes", &TThis::SkipNonTableNodes)
        .Default(false);

    registrar.Parameter("watermark_delay", &TThis::WatermarkDelay)
        .Default(TDuration::Hours(1));

    registrar.Parameter("failover_delay", &TThis::FailoverDelay)
        .Default(TDuration::Minutes(5));

    registrar.Preprocessor([] (TThis* config) {
        config->EventTimestampLocator->Attribute = "key";
        config->SystemTimestampLocator->Attribute = "creation_time";
    });

    registrar.Postprocessor([] (TThis* spec) {
        if (spec->Tables.has_value()) {
            THROW_ERROR_EXCEPTION_IF(spec->TablesPath.has_value(), "`TablesPath` cannot be set with `Tables`");
            for (const auto& table : *spec->Tables) {
                THROW_ERROR_EXCEPTION_UNLESS(table.GetCluster().has_value(), "`Tables[i]` should have cluster (Table: %v)", table);
                THROW_ERROR_EXCEPTION_IF(table.HasNontrivialRanges(), "`Tables[i]` should not have ranges (Table: %v)", table);
            }
        } else {
            THROW_ERROR_EXCEPTION_UNLESS(spec->TablesPath.has_value(), "`TablesPath` or `Tables` must be set");

            THROW_ERROR_EXCEPTION_IF(spec->TablesPath->GetPath().empty(), "`TablesPath` must not be empty (TablesPath: %v)", spec->TablesPath);
            THROW_ERROR_EXCEPTION_IF(spec->TablesPath->HasNontrivialRanges(), "`TablesPath` should not have ranges (TablesPath: %v)", spec->TablesPath);

            const bool hasCluster = spec->TablesPath->GetCluster().has_value();
            const auto clusters = spec->TablesPath->GetClusters();
            THROW_ERROR_EXCEPTION_IF(hasCluster && clusters.has_value(),
                "`TablesPath` must specify either `cluster` or `clusters`, not both (TablesPath: %v)",
                spec->TablesPath);
            THROW_ERROR_EXCEPTION_UNLESS(hasCluster || clusters.has_value(),
                "`TablesPath` should have `cluster` or `clusters` (TablesPath: %v)",
                spec->TablesPath);
            if (clusters.has_value()) {
                THROW_ERROR_EXCEPTION_IF(clusters->empty(),
                    "`TablesPath` `clusters` must not be empty (TablesPath: %v)",
                    spec->TablesPath);
                THashSet<std::string> seen;
                for (const auto& cluster : *clusters) {
                    THROW_ERROR_EXCEPTION_UNLESS(seen.insert(cluster).second,
                        "`TablesPath` `clusters` must not contain duplicates (Cluster: %v)",
                        cluster);
                }
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableSourceParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("desired_table_process_time", &TThis::DesiredTableProcessTime)
        .Default(TDuration::Hours(1));

    registrar.Parameter("max_rows_per_second", &TThis::MaxRowsPerSecond)
        .Default(1e12);
    registrar.Parameter("max_bytes_per_second", &TThis::MaxBytesPerSecond)
        .Default(1e12);

    registrar.Parameter("min_event_timestamp", &TThis::MinEventTimestamp)
        .Default();

    registrar.Parameter("restart_instant", &TThis::RestartInstant)
        .Default(TInstant::Zero());

    registrar.Parameter("max_partition_count", &TThis::MaxPartitionCount)
        .Default(TSize::FromString("10K"));
    registrar.Parameter("throttler_period", &TThis::ThrottlerPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("desired_partition_process_time", &TThis::DesiredPartitionProcessTime)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("desired_partition_rows_per_second", &TThis::DesiredPartitionRowsPerSecond)
        .Default(1e3);
    registrar.Parameter("desired_partition_bytes_per_second", &TThis::DesiredPartitionBytesPerSecond)
        .Default(1e6);

    registrar.Parameter("read_timeout", &TThis::ReadTimeout)
        .Default(TDuration::Minutes(5));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableSourcePartitionSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("table", &TThis::Table)
        .Default();

    registrar.Parameter("event_timestamp", &TThis::EventTimestamp)
        .Default();
    registrar.Parameter("system_timestamp", &TThis::SystemTimestamp)
        .Default();

    registrar.Parameter("rows_per_second", &TThis::RowsPerSecond)
        .Default(1.0);

    registrar.Postprocessor([] (TThis* spec) {
        THROW_ERROR_EXCEPTION_UNLESS(spec->Table.GetCluster().has_value(), "`Table` should have cluster (Table: %v)", spec->Table);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnectorV2
