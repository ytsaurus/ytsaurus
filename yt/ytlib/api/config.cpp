#include "config.h"

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/transaction_client/config.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

TMasterConnectionConfig::TMasterConnectionConfig()
{
    RegisterParameter("cell_tag", CellTag);
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("network_name", NetworkName)
        .Default(NNodeTrackerClient::InterconnectNetworkName);
    RegisterParameter("master", Master);
    RegisterParameter("master_cache", MasterCache)
        .Default();
    RegisterParameter("enable_read_from_followers", EnableReadFromFollowers)
        .Default(false);
    RegisterParameter("timestamp_provider", TimestampProvider)
        .Default();
    RegisterParameter("cell_directory", CellDirectory)
        .DefaultNew();
    RegisterParameter("scheduler", Scheduler)
        .DefaultNew();
    RegisterParameter("transaction_manager", TransactionManager)
        .DefaultNew();
    RegisterParameter("block_cache", BlockCache)
        .DefaultNew();
    RegisterParameter("table_mount_cache", TableMountCache)
        .DefaultNew();

    RegisterParameter("query_evaluator", QueryEvaluator)
        .DefaultNew();
    RegisterParameter("query_timeout", QueryTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("query_response_codec", QueryResponseCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("default_input_row_limit", DefaultInputRowLimit)
        .GreaterThan(0)
        .Default(100000000);
    RegisterParameter("default_output_row_limit", DefaultOutputRowLimit)
        .GreaterThan(0)
        .Default(1000000);

    RegisterParameter("column_evaluator_cache", ColumnEvaluatorCache)
        .DefaultNew();

    RegisterParameter("write_timeout", WriteTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("write_request_codec", WriteRequestCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("max_rows_per_write_request", MaxRowsPerWriteRequest)
        .GreaterThan(0)
        .Default(1000);
    RegisterParameter("max_rows_per_transaction", MaxRowsPerTransaction)
        .GreaterThan(0)
        .Default(100000);

    RegisterParameter("lookup_timeout", LookupTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("lookup_request_codec", LookupRequestCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("lookup_response_codec", LookupResponseCodec)
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("max_rows_per_read_request", MaxRowsPerReadRequest)
        .GreaterThan(0)
        .Default(1000);

    RegisterParameter("enable_udf", EnableUdf)
        .Default(false);
    RegisterParameter("udf_registry_path", UdfRegistryPath)
        .Default("//tmp/udfs");

    RegisterParameter("table_mount_info_update_retry_count", TableMountInfoUpdateRetryCount)
        .GreaterThanOrEqual(0)
        .Default(5);
    RegisterParameter("table_mount_info_update_retry_time", TableMountInfoUpdateRetryPeriod)
        .GreaterThan(TDuration::MicroSeconds(0))
        .Default(TDuration::Seconds(1));

    RegisterParameter("light_pool_size", LightInvokerPoolSize)
        .Describe("Number of threads handling light requests")
        .Default(1);
    RegisterParameter("heavy_pool_size", HeavyInvokerPoolSize)
        .Describe("Number of threads handling heavy requests")
        .Default(4);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

