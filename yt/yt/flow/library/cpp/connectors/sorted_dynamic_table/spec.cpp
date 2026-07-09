#include "spec.h"

#include <yt/yt/flow/library/cpp/common/yt_path_option.h>

namespace NYT::NFlow::NSortedDynamicTable {

////////////////////////////////////////////////////////////////////////////////

void TInfoSpec::Register(TRegistrar registrar)
{
    registrar.Parameter("table_path", &TThis::TablePath)
        .AddOption(EYTPathOwnership::SharedWrite);

    registrar.Parameter("update_partition_count_period", &TThis::UpdatePartitionCountPeriod)
        .Default(TDuration::Seconds(60));
}

DEFINE_REFCOUNTED_TYPE(TInfoSpec);

////////////////////////////////////////////////////////////////////////////////

void TSyncSinkParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("column_filter", &TThis::ColumnFilter)
        .Default();
    registrar.Parameter("aggregate_columns", &TThis::AggregateColumns)
        .Default();
    registrar.Parameter("delete_rows", &TThis::DeleteRows)
        .Default(false);
    registrar.Parameter("require_sync_replica", &TThis::RequireSyncReplica)
        .Default(true);
}

DEFINE_REFCOUNTED_TYPE(TSyncSinkParameters);

////////////////////////////////////////////////////////////////////////////////

void TDynamicSyncSinkParameters::Register(TRegistrar /*registrar*/)
{ }

DEFINE_REFCOUNTED_TYPE(TDynamicSyncSinkParameters);

////////////////////////////////////////////////////////////////////////////////

void TSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

DEFINE_REFCOUNTED_TYPE(TSinkControllerParameters);

////////////////////////////////////////////////////////////////////////////////

void TDynamicSinkControllerParameters::Register(TRegistrar /*registrar*/)
{ }

DEFINE_REFCOUNTED_TYPE(TDynamicSinkControllerParameters);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NSortedDynamicTable
