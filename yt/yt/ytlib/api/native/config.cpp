#include "config.h"

#include <yt/ytlib/hive/config.h>

#include <yt/ytlib/node_tracker_client/config.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/table_client/config.h>

#include <yt/ytlib/tablet_client/config.h>

#include <yt/ytlib/transaction_client/config.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/transaction_client/config.h>

namespace NYT::NApi::NNative {

using namespace NObjectClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TMasterConnectionConfig::TMasterConnectionConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(15));

    RegisterParameter("enable_master_cache_discovery", EnableMasterCacheDiscovery)
        .Default(false);

    RegisterParameter("master_cache_discovery_period", MasterCacheDiscoveryPeriod)
        .Default(TDuration::Minutes(1));
}

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("networks", Networks)
        .Default();
    RegisterParameter("timestamp_provider", TimestampProvider)
        .Default();
    RegisterParameter("cell_directory", CellDirectory)
        .DefaultNew();
    RegisterParameter("cell_directory_synchronizer", CellDirectorySynchronizer)
        .DefaultNew();
    RegisterParameter("master_cell_directory_synchronizer", MasterCellDirectorySynchronizer)
        .DefaultNew();
    RegisterParameter("scheduler", Scheduler)
        .DefaultNew();
    RegisterParameter("transaction_manager", TransactionManager)
        .DefaultNew();
    RegisterParameter("block_cache", BlockCache)
        .DefaultNew();
    RegisterParameter("cluster_directory_synchronizer", ClusterDirectorySynchronizer)
        .DefaultNew();
    RegisterParameter("medium_directory_synchronizer", MediumDirectorySynchronizer)
        .DefaultNew();
    RegisterParameter("node_directory_synchronizer", NodeDirectorySynchronizer)
        .DefaultNew();

    RegisterParameter("query_evaluator", QueryEvaluator)
        .DefaultNew();
    RegisterParameter("default_select_rows_timeout", DefaultSelectRowsTimeout)
        // COMPAT(babenko)
        .Alias("query_timeout")
        .Default(TDuration::Seconds(60));
    RegisterParameter("select_rows_response_codec", SelectRowsResponseCodec)
        // COMPAT(babenko)
        .Alias("query_response_codec")
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("default_input_row_limit", DefaultInputRowLimit)
        .GreaterThan(0)
        .Default(1000000);
    RegisterParameter("default_output_row_limit", DefaultOutputRowLimit)
        .GreaterThan(0)
        .Default(1000000);

    RegisterParameter("column_evaluator_cache", ColumnEvaluatorCache)
        .DefaultNew();

    RegisterParameter("write_rows_timeout", WriteRowsTimeout)
        // COMPAT(babenko)
        .Alias("write_timeout")
        .Default(TDuration::Seconds(60));
    RegisterParameter("write_rows_request_codec", WriteRowsRequestCodec)
        // COMPAT(babenko)
        .Alias("write_request_codec")
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("max_rows_per_write_request", MaxRowsPerWriteRequest)
        .GreaterThan(0)
        .Default(1000);
    RegisterParameter("max_data_weight_per_write_request", MaxDataWeightPerWriteRequest)
        .GreaterThan(0)
        .Default(64_MB);
    RegisterParameter("max_rows_per_transaction", MaxRowsPerTransaction)
        .GreaterThan(0)
        .Default(100000);

    RegisterParameter("default_lookup_rows_timeout", DefaultLookupRowsTimeout)
        // COMPAT(babenko)
        .Alias("lookup_timeout")
        .Default(TDuration::Seconds(60));
    RegisterParameter("lookup_rows_request_codec", LookupRowsRequestCodec)
        .Alias("lookup_request_codec")
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("lookup_rows_response_codec", LookupRowsResponseCodec)
        .Alias("lookup_response_codec")
        .Default(NCompression::ECodec::Lz4);
    RegisterParameter("max_rows_per_lookup_request", MaxRowsPerLookupRequest)
        .Alias("max_rows_per_read_request")
        .GreaterThan(0)
        .Default(1000);
    RegisterParameter("enable_lookup_multiread", EnableLookupMultiread)
        .Default(true);

    RegisterParameter("udf_registry_path", UdfRegistryPath)
        .Default("//tmp/udfs");
    RegisterParameter("function_registry_cache", FunctionRegistryCache)
        .DefaultNew();
    RegisterParameter("function_impl_cache", FunctionImplCache)
        .DefaultNew();

    RegisterParameter("thread_pool_size", ThreadPoolSize)
        .Default(4);

    RegisterParameter("max_concurrent_requests", MaxConcurrentRequests)
        .GreaterThan(0)
        .Default(1000);

    RegisterParameter("bus_client", BusClient)
        .DefaultNew();
    RegisterParameter("idle_channel_ttl", IdleChannelTtl)
        .Default(TDuration::Minutes(5));

    RegisterParameter("default_get_in_sync_replicas_timeout", DefaultGetInSyncReplicasTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("default_get_tablet_infos_timeout", DefaultGetTabletInfosTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("default_trim_table_timeout", DefaultTrimTableTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("default_get_operation_timeout", DefaultGetOperationTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("default_list_jobs_timeout", DefaultListJobsTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("default_get_job_timeout", DefaultGetJobTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("default_list_operations_timeout", DefaultListOperationsTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("cache_sticky_group_size_override", CacheStickyGroupSizeOverride)
        .Default(1);

    RegisterParameter("max_request_window_size", MaxRequestWindowSize)
        .GreaterThan(0)
        .Default(65536);

    RegisterParameter("upload_transaction_timeout", UploadTransactionTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("hive_sync_rpc_timeout", HiveSyncRpcTimeout)
        .Default(TDuration::Seconds(30));

    RegisterParameter("name", Name)
        .Default("default");

    RegisterParameter("permission_cache", PermissionCache)
        .DefaultNew();

    RegisterParameter("job_node_descriptor_cache", JobNodeDescriptorCache)
        .DefaultNew();

    RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
        .Default(100'000)
        .GreaterThan(0);

    RegisterParameter("max_chunks_per_locate_request", MaxChunksPerLocateRequest)
        .Default(10'000)
        .GreaterThan(0);

    RegisterParameter("nested_input_transaction_timeout", NestedInputTransactionTimeout)
        .Default(TDuration::Minutes(10));
    RegisterParameter("nested_input_transaction_ping_period", NestedInputTransactionPingPeriod)
        .Default(TDuration::Minutes(1));

    RegisterPreprocessor([&] () {
        FunctionImplCache->Capacity = 100;

        JobNodeDescriptorCache->ExpireAfterAccessTime = TDuration::Minutes(5);
        JobNodeDescriptorCache->ExpireAfterSuccessfulUpdateTime = TDuration::Minutes(5);
        JobNodeDescriptorCache->RefreshTime = TDuration::Minutes(1);
    });
}

////////////////////////////////////////////////////////////////////////////////

void InitTimestampProviderConfig(
    TRemoteTimestampProviderConfigPtr timestampProviderConfig,
    TMasterConnectionConfigPtr config)
{
    // Use masters for timestamp generation.
    timestampProviderConfig->Addresses = config->Addresses;
    timestampProviderConfig->RpcTimeout = config->RpcTimeout;

    // TRetryingChannelConfig
    timestampProviderConfig->RetryBackoffTime = config->RetryBackoffTime;
    timestampProviderConfig->RetryAttempts = config->RetryAttempts;
    timestampProviderConfig->RetryTimeout = config->RetryTimeout;
}

TRemoteTimestampProviderWithDiscoveryConfigPtr CreateRemoteTimestampProviderWithDiscoveryConfig(TMasterConnectionConfigPtr config)
{
    auto timestampProviderConfig = New<TRemoteTimestampProviderWithDiscoveryConfig>();
    InitTimestampProviderConfig(timestampProviderConfig, config);
    return timestampProviderConfig;
}

TBatchingRemoteTimestampProviderConfigPtr CreateBatchingRemoteTimestampProviderConfig(TMasterConnectionConfigPtr config)
{
    auto timestampProviderConfig = New<TBatchingRemoteTimestampProviderConfig>();
    InitTimestampProviderConfig(timestampProviderConfig, config);
    return timestampProviderConfig;
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

