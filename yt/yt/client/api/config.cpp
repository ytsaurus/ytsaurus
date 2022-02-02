#include "config.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TTableMountCacheConfig::TTableMountCacheConfig()
{
    RegisterParameter("on_error_retry_count", OnErrorRetryCount)
        .GreaterThanOrEqual(0)
        .Default(5);
    RegisterParameter("on_error_retry_slack_period", OnErrorSlackPeriod)
        .GreaterThan(TDuration::Zero())
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

TConnectionConfig::TConnectionConfig()
{
    RegisterParameter("connection_type", ConnectionType)
        .Default(EConnectionType::Native);
    RegisterParameter("cluster_name", ClusterName)
        .Default();
    RegisterParameter("table_mount_cache", TableMountCache)
        .DefaultNew();
    RegisterParameter("replication_card_cache", ReplicationCardCache)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TConnectionDynamicConfig::TConnectionDynamicConfig()
{
    RegisterParameter("table_mount_cache", TableMountCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TPersistentQueuePollerConfig::TPersistentQueuePollerConfig()
{
    RegisterParameter("max_prefetch_row_count", MaxPrefetchRowCount)
        .GreaterThan(0)
        .Default(1024);
    RegisterParameter("max_prefetch_data_weight", MaxPrefetchDataWeight)
        .GreaterThan(0)
        .Default((i64) 16 * 1024 * 1024);
    RegisterParameter("max_rows_per_fetch", MaxRowsPerFetch)
        .GreaterThan(0)
        .Default(512);
    RegisterParameter("max_rows_per_poll", MaxRowsPerPoll)
        .GreaterThan(0)
        .Default(1);
    RegisterParameter("max_fetched_untrimmed_row_count", MaxFetchedUntrimmedRowCount)
        .GreaterThan(0)
        .Default(40000);
    RegisterParameter("untrimmed_data_rows_low", UntrimmedDataRowsLow)
        .Default(0);
    RegisterParameter("untrimmed_data_rows_high", UntrimmedDataRowsHigh)
        .Default(std::numeric_limits<i64>::max());
    RegisterParameter("data_poll_period", DataPollPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("state_trim_period", StateTrimPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("backoff_time", BackoffTime)
        .Default(TDuration::Seconds(5));

    RegisterPostprocessor([&] {
        if (UntrimmedDataRowsLow > UntrimmedDataRowsHigh) {
            THROW_ERROR_EXCEPTION("\"untrimmed_data_rows_low\" must not exceed \"untrimmed_data_rows_high\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJournalWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_batch_row_count", &TThis::MaxBatchRowCount)
        .Default(10'000);
    registrar.Parameter("max_batch_data_size", &TThis::MaxBatchDataSize)
        .Default(16_MB);
    registrar.Parameter("max_batch_delay", &TThis::MaxBatchDelay)
        .Default(TDuration::MilliSeconds(5));

    registrar.Parameter("max_flush_row_count", &TThis::MaxFlushRowCount)
        .Default(100'000);
    registrar.Parameter("max_flush_data_size", &TThis::MaxFlushDataSize)
        .Default(100_MB);

    registrar.Parameter("max_chunk_row_count", &TThis::MaxChunkRowCount)
        .GreaterThan(0)
        .Default(1'000'000);
    registrar.Parameter("max_chunk_data_size", &TThis::MaxChunkDataSize)
        .GreaterThan(0)
        .Default(10_GB);
    registrar.Parameter("max_chunk_session_duration", &TThis::MaxChunkSessionDuration)
        .Default(TDuration::Hours(60));

    registrar.Parameter("prefer_local_host", &TThis::PreferLocalHost)
        .Default(true);

    registrar.Parameter("node_rpc_timeout", &TThis::NodeRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("node_ping_period", &TThis::NodePingPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("node_ban_timeout", &TThis::NodeBanTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("open_session_backoff_time", &TThis::OpenSessionBackoffTime)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("node_channel", &TThis::NodeChannel)
        .DefaultNew();

    registrar.Parameter("prerequisite_transaction_probe_period", &TThis::PrerequisiteTransactionProbePeriod)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("dont_close", &TThis::DontClose)
        .Default(false);
    registrar.Parameter("dont_seal", &TThis::DontSeal)
        .Default(false);
    registrar.Parameter("dont_preallocate", &TThis::DontPreallocate)
        .Default(false);
    registrar.Parameter("replica_failure_probability", &TThis::ReplicaFailureProbability)
        .Default(0.0)
        .InRange(0.0, 1.0);
    registrar.Parameter("replica_row_limits", &TThis::ReplicaRowLimits)
        .Default();
    registrar.Parameter("replica_fake_timeout_delay", &TThis::ReplicaFakeTimeoutDelay)
        .Default();
    registrar.Parameter("open_delay", &TThis::OpenDelay)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        if (config->MaxBatchRowCount > config->MaxFlushRowCount) {
            THROW_ERROR_EXCEPTION("\"max_batch_row_count\" cannot be greater than \"max_flush_row_count\"")
                << TErrorAttribute("max_batch_row_count", config->MaxBatchRowCount)
                << TErrorAttribute("max_flush_row_count", config->MaxFlushRowCount);
        }
        if (config->MaxBatchDataSize > config->MaxFlushDataSize) {
            THROW_ERROR_EXCEPTION("\"max_batch_data_size\" cannot be greater than \"max_flush_data_size\"")
                << TErrorAttribute("max_batch_data_size", config->MaxBatchDataSize)
                << TErrorAttribute("max_flush_data_size", config->MaxFlushDataSize);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

