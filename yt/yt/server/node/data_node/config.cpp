#include "config.h"

#include <yt/yt/library/containers/config.h>
#include <yt/yt/library/containers/disk_manager/config.h>

namespace NYT::NDataNode {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TP2PConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(true);

    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();

    registrar.Parameter("block_cache_override", &TThis::BlockCacheOverride)
        .DefaultNew();

    registrar.Parameter("tick_period", &TThis::TickPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("node_refresh_period", &TThis::NodeRefreshPeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("request_timeout", &TThis::RequestTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("node_staleness_timeout", &TThis::NodeStalenessTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("iteration_wait_timeout", &TThis::IterationWaitTimeout)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("max_waiting_requests", &TThis::MaxWaitingRequests)
        .Default(128);

    registrar.Parameter("session_cleaup_period", &TThis::SessionCleaupPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("session_ttl", &TThis::SessionTTL)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("snooper_cache", &TThis::RequestCache)
        .DefaultNew();
    registrar.Parameter("snooper_cache_override", &TThis::RequestCacheOverride)
        .DefaultNew();

    registrar.Parameter("chunk_cooldown_timeout", &TThis::ChunkCooldownTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("max_distributed_bytes", &TThis::MaxDistributedBytes)
        .Default(128_MB);
    registrar.Parameter("max_block_size", &TThis::MaxBlockSize)
        .Default(128_MB);
    registrar.Parameter("block_counter_reset_ticks", &TThis::BlockCounterResetTicks)
        .GreaterThan(0)
        .Default(150);
    registrar.Parameter("hot_block_threshold", &TThis::HotBlockThreshold)
        .Default(10);
    registrar.Parameter("second_hot_block_threshold", &TThis::SecondHotBlockThreshold)
        .Default(5);
    registrar.Parameter("hot_block_replica_count", &TThis::HotBlockReplicaCount)
        .Default(3);
    registrar.Parameter("block_redistribution_ticks", &TThis::BlockRedistributionTicks)
        .Default(3000);
    registrar.Parameter("track_memory_of_chunk_blocks_buffer", &TThis::TrackMemoryOfChunkBlocksBuffer)
        .Default(false);

    registrar.Parameter("node_tag_filter", &TThis::NodeTagFilter)
        .Default(MakeBooleanFormula("!CLOUD"));

    registrar.Preprocessor([] (TThis* config) {
        // Low default to prevent OOMs in yt-local.
        config->BlockCache->Capacity = 1_MB;

        // Block cache won't accept blocks larger than Capacity / ShardCount * YoungerSizeFraction.
        //
        // With Capacity = 2G and default ShardCount/YoungerSizeFraction,
        // max block size is equal to 32MB, which is too low.
        //
        // With adjusted defaults, max block size is equal to 256MB.
        config->BlockCache->ShardCount = 4;
        config->BlockCache->YoungerSizeFraction = 0.5;

        // Should be good enough.
        config->RequestCache->Capacity = 16 * 1024;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TChunkLocationConfig::ApplyDynamicInplace(const TChunkLocationDynamicConfig& dynamicConfig)
{
    TDiskLocationConfig::ApplyDynamicInplace(dynamicConfig);

    UpdateYsonStructField(IOEngineType, dynamicConfig.IOEngineType);
    UpdateYsonStructField(IOConfig, dynamicConfig.IOConfig);

    for (auto kind : TEnumTraits<EChunkLocationThrottlerKind>::GetDomainValues()) {
        UpdateYsonStructField(Throttlers[kind], dynamicConfig.Throttlers[kind]);
    }
    UpdateYsonStructField(ThrottleDuration, dynamicConfig.ThrottleDuration);

    UpdateYsonStructField(CoalescedReadMaxGapSize, dynamicConfig.CoalescedReadMaxGapSize);
}

void TChunkLocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("quota", &TThis::Quota)
        .GreaterThanOrEqual(0)
        .Default();

    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Default();

    registrar.Parameter("io_engine_type", &TThis::IOEngineType)
        .Default(NIO::EIOEngineType::ThreadPool);
    registrar.Parameter("io_config", &TThis::IOConfig)
        .Optional();

    registrar.Parameter("throttle_duration", &TThis::ThrottleDuration)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("coalesced_read_max_gap_size", &TThis::CoalescedReadMaxGapSize)
        .GreaterThanOrEqual(0)
        .Default(0);

    registrar.Parameter("io_weight", &TThis::IOWeight)
        .GreaterThanOrEqual(0)
        .Default(1.0);

    registrar.Parameter("max_write_rate_by_dwpd", &TThis::MaxWriteRateByDwpd)
        .GreaterThanOrEqual(0)
        .Default(0);

    registrar.Parameter("reset_uuid", &TThis::ResetUuid)
        .Default(false);

    registrar.Postprocessor([] (TThis* config) {
        for (auto kind : TEnumTraits<EChunkLocationThrottlerKind>::GetDomainValues()) {
            if (!config->Throttlers[kind]) {
                config->Throttlers[kind] = New<TRelativeThroughputThrottlerConfig>();
            }
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TChunkLocationDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("io_engine_type", &TThis::IOEngineType)
        .Optional();
    registrar.Parameter("io_config", &TThis::IOConfig)
        .Optional();
    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Optional();
    registrar.Parameter("throttle_duration", &TThis::ThrottleDuration)
        .Optional();
    registrar.Parameter("coalesced_read_max_gap_size", &TThis::CoalescedReadMaxGapSize)
        .GreaterThanOrEqual(0)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

TStoreLocationConfigPtr TStoreLocationConfig::ApplyDynamic
    (const TStoreLocationDynamicConfigPtr& dynamicConfig) const
{
    auto config = CloneYsonStruct(MakeStrong(this));
    config->ApplyDynamicInplace(*dynamicConfig);
    config->Postprocess();
    return config;
}

void TStoreLocationConfig::ApplyDynamicInplace(
    const TStoreLocationDynamicConfig& dynamicConfig)
{
    TChunkLocationConfig::ApplyDynamicInplace(dynamicConfig);

    UpdateYsonStructField(LowWatermark, dynamicConfig.LowWatermark);
    UpdateYsonStructField(HighWatermark, dynamicConfig.HighWatermark);
    UpdateYsonStructField(DisableWritesWatermark, dynamicConfig.DisableWritesWatermark);

    UpdateYsonStructField(MaxTrashTtl, dynamicConfig.MaxTrashTtl);
    UpdateYsonStructField(TrashCleanupWatermark, dynamicConfig.TrashCleanupWatermark);
    UpdateYsonStructField(TrashCheckPeriod, dynamicConfig.TrashCheckPeriod);
}

void TStoreLocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("low_watermark", &TThis::LowWatermark)
        .GreaterThanOrEqual(0)
        .Default(5_GB);
    registrar.Parameter("high_watermark", &TThis::HighWatermark)
        .GreaterThanOrEqual(0)
        .Default(2_GB);
    registrar.Parameter("disable_writes_watermark", &TThis::DisableWritesWatermark)
        .GreaterThanOrEqual(0)
        .Default(1_GB);

    registrar.Parameter("max_trash_ttl", &TThis::MaxTrashTtl)
        .Default(TDuration::Hours(1));
    registrar.Parameter("trash_cleanup_watermark", &TThis::TrashCleanupWatermark)
        .GreaterThanOrEqual(0)
        .Default(4_GB);
    registrar.Parameter("trash_check_period", &TThis::TrashCheckPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("multiplexed_changelog", &TThis::MultiplexedChangelog)
        .Default();
    registrar.Parameter("high_latency_split_changelog", &TThis::HighLatencySplitChangelog)
        .Default();
    registrar.Parameter("low_latency_split_changelog", &TThis::LowLatencySplitChangelog)
        .Default();

    registrar.BaseClassParameter("medium_name", &TThis::MediumName)
        .Default(NChunkClient::DefaultStoreMediumName);

    registrar.Postprocessor([] (TThis* config) {
        if (config->HighWatermark > config->LowWatermark) {
            THROW_ERROR_EXCEPTION("\"high_full_watermark\" must be less than or equal to \"low_watermark\"");
        }
        if (config->DisableWritesWatermark > config->HighWatermark) {
            THROW_ERROR_EXCEPTION("\"write_disable_watermark\" must be less than or equal to \"high_watermark\"");
        }
        if (config->DisableWritesWatermark > config->TrashCleanupWatermark) {
            THROW_ERROR_EXCEPTION("\"disable_writes_watermark\" must be less than or equal to \"trash_cleanup_watermark\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TStoreLocationDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("low_watermark", &TThis::LowWatermark)
        .GreaterThanOrEqual(0)
        .Optional();
    registrar.Parameter("high_watermark", &TThis::HighWatermark)
        .GreaterThanOrEqual(0)
        .Optional();
    registrar.Parameter("disable_writes_watermark", &TThis::DisableWritesWatermark)
        .GreaterThanOrEqual(0)
        .Optional();

    registrar.Parameter("max_trash_ttl", &TThis::MaxTrashTtl)
        .Optional();
    registrar.Parameter("trash_cleanup_watermark", &TThis::TrashCleanupWatermark)
        .GreaterThanOrEqual(0)
        .Optional();
    registrar.Parameter("trash_check_period", &TThis::TrashCheckPeriod)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TCacheLocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("in_throttler", &TThis::InThrottler)
        .DefaultNew();

    registrar.BaseClassParameter("medium_name", &TThis::MediumName)
        .Default(NChunkClient::DefaultCacheMediumName);
}

////////////////////////////////////////////////////////////////////////////////

void TMultiplexedChangelogConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_record_count", &TThis::MaxRecordCount)
        .Default(1000000)
        .GreaterThan(0);
    registrar.Parameter("max_data_size", &TThis::MaxDataSize)
        .Default(256_MB)
        .GreaterThan(0);
    registrar.Parameter("auto_rotation_period", &TThis::AutoRotationPeriod)
        .Default(TDuration::Minutes(15));
    registrar.Parameter("replay_buffer_size", &TThis::ReplayBufferSize)
        .GreaterThan(0)
        .Default(256_MB);
    registrar.Parameter("max_clean_changelogs_to_keep", &TThis::MaxCleanChangelogsToKeep)
        .GreaterThanOrEqual(0)
        .Default(3);
    registrar.Parameter("clean_delay", &TThis::CleanDelay)
        .Default(TDuration::Minutes(1));
    registrar.Parameter("big_record_threshold", &TThis::BigRecordThreshold)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TArtifactCacheReaderConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemArtifactCacheDownload);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TLayerLocationConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("low_watermark", &TThis::LowWatermark)
        .Default(1_GB)
        .GreaterThanOrEqual(0);

    registrar.Parameter("quota", &TThis::Quota)
        .Default();

    registrar.Parameter("location_is_absolute", &TThis::LocationIsAbsolute)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TTmpfsLayerCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("capacity", &TThis::Capacity)
        .Default(10 * 1_GB)
        .GreaterThan(0);

    registrar.Parameter("layers_directory_path", &TThis::LayersDirectoryPath)
        .Default(std::nullopt);

    registrar.Parameter("layers_update_period", &TThis::LayersUpdatePeriod)
        .Default(TDuration::Minutes(3))
        .GreaterThan(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

void TTableSchemaCacheConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("table_schema_cache_request_timeout", &TThis::TableSchemaCacheRequestTimeout)
        .Default(TDuration::Seconds(1));

    registrar.Preprocessor([] (TThis* config) {
        config->Capacity = 100_MB;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TTableSchemaCacheDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("table_schema_cache_request_timeout", &TThis::TableSchemaCacheRequestTimeout)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("porto_executor", &TThis::PortoExecutor)
        .DefaultNew();

    registrar.Parameter("layer_locations", &TThis::LayerLocations);

    registrar.Parameter("enable_layers_cache", &TThis::EnableLayersCache)
        .Default(true);

    registrar.Parameter("cache_capacity_fraction", &TThis::CacheCapacityFraction)
        .Default(0.8)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1);

    registrar.Parameter("layer_import_concurrency", &TThis::LayerImportConcurrency)
        .Default(2)
        .GreaterThan(0)
        .LessThanOrEqual(10);

    registrar.Parameter("enable_disk_quota", &TThis::EnableDiskQuota)
        .Default(true);

    registrar.Parameter("regular_tmpfs_layer_cache", &TThis::RegularTmpfsLayerCache)
        .Alias("tmpfs_layer_cache")
        .DefaultNew();

    registrar.Parameter("nirvana_tmpfs_layer_cache", &TThis::NirvanaTmpfsLayerCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("incremental_heartbeat_period", &TThis::IncrementalHeartbeatPeriod)
        .Default();
    registrar.Parameter("incremental_heartbeat_period_splay", &TThis::IncrementalHeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("job_heartbeat_period", &TThis::JobHeartbeatPeriod)
        .Default();
    registrar.Parameter("job_heartbeat_period_splay", &TThis::JobHeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

void TMasterConnectorDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("incremental_heartbeat_period", &TThis::IncrementalHeartbeatPeriod)
        .Default();
    registrar.Parameter("incremental_heartbeat_period_splay", &TThis::IncrementalHeartbeatPeriodSplay)
        .Default();
    registrar.Parameter("incremental_heartbeat_timeout", &TThis::IncrementalHeartbeatTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("full_heartbeat_timeout", &TThis::FullHeartbeatTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("job_heartbeat_period", &TThis::JobHeartbeatPeriod)
        .Default();
    registrar.Parameter("job_heartbeat_period_splay", &TThis::JobHeartbeatPeriodSplay)
        .Default();
    registrar.Parameter("job_heartbeat_timeout", &TThis::JobHeartbeatTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("max_chunk_events_per_incremental_heartbeat", &TThis::MaxChunkEventsPerIncrementalHeartbeat)
        .Default(1000000);
    registrar.Parameter("enable_profiling", &TThis::EnableProfiling)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TAllyReplicaManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("announcement_backoff_time", &TThis::AnnouncementBackoffTime)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_chunks_per_announcement_request", &TThis::MaxChunksPerAnnouncementRequest)
        .Default(5'000);
    registrar.Parameter("announcement_request_timeout", &TThis::AnnouncementRequestTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

void TDataNodeTestingOptions::Register(TRegistrar registrar)
{
    registrar.Parameter("columnar_statistics_chunk_meta_fetch_max_delay", &TThis::ColumnarStatisticsChunkMetaFetchMaxDelay)
        .Default();

    registrar.Parameter("simulate_network_throttling_for_get_block_set", &TThis::SimulateNetworkThrottlingForGetBlockSet)
        .Default(false);

    registrar.Parameter("fail_reincarnation_jobs", &TThis::FailReincarnationJobs)
        .Default(false);

    registrar.Parameter("block_read_timeout_fraction", &TThis::BlockReadTimeoutFraction)
        .Default(0.75);

    registrar.Parameter("delay_before_blob_session_block_free", &TThis::DelayBeforeBlobSessionBlockFree)
        .Default();

    registrar.Parameter("columnar_statistics_read_timeout_fraction", &TThis::ColumnarStatisticsReadTimeoutFraction)
        .Default(0.75);
}

////////////////////////////////////////////////////////////////////////////////

void TMediumThroughputMeterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("medium_name", &TThis::MediumName)
        .NonEmpty();

    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("verification_initial_window_factor", &TThis::VerificationInitialWindowFactor)
        .Default(0.75);

    registrar.Parameter("verification_segment_size_factor", &TThis::VerificationSegmentSizeFactor)
        .Default(0.05);

    registrar.Parameter("verification_window_period", &TThis::VerificationWindowPeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("dwpd_factor", &TThis::DWPDFactor)
        .Default(1);

    registrar.Parameter("use_workload_model", &TThis::UseWorkloadModel)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TIOThroughputMeterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("media", &TThis::Media)
        .Alias("mediums");

    registrar.Parameter("time_between_tests", &TThis::TimeBetweenTests)
        .Default(TDuration::Hours(12));

    registrar.Parameter("estimate_time_limit", &TThis::EstimateTimeLimit)
        .Default(TDuration::Minutes(20));

    registrar.Parameter("max_estimate_congestions", &TThis::MaxEstimateCongestions)
        .Default(20);

    registrar.Parameter("testing_time_hard_limit", &TThis::TestingTimeHardLimit)
        .Default(TDuration::Minutes(120));
}

////////////////////////////////////////////////////////////////////////////////

void TLocationHealthCheckerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enabled", &TThis::Enabled)
        .Default(false);

    registrar.Parameter("enable_manual_disk_failures", &TThis::EnableManualDiskFailures)
        .Default(false);

    registrar.Parameter("new_disk_checker_enabled", &TThis::EnableNewDiskChecker)
        .Default(false);

    registrar.Parameter("health_check_period", &TThis::HealthCheckPeriod)
        .Default(TDuration::Seconds(60));
}

////////////////////////////////////////////////////////////////////////////////

void TReplicateChunkJobDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("writer", &TThis::Writer)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        // Disable target allocation from master.
        config->Writer->UploadReplicationFactor = 1;

        // Use proper workload descriptors.
        // TODO(babenko): avoid passing workload descriptor in config
        config->Writer->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemReplication);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TRemoveChunkJobDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("wait_for_incremental_heartbeat_barrier", &TThis::WaitForIncrementalHeartbeatBarrier)
        .Default(true);

    registrar.Parameter("delay_before_start_remove_chunk", &TThis::DelayBeforeStartRemoveChunk)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TMergeChunksJobDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TThis::Reader)
        .DefaultNew();
    registrar.Parameter("writer", &TThis::Writer)
        .DefaultNew();

    registrar.Parameter("fail_shallow_merge_validation", &TThis::FailShallowMergeValidation)
        .Default(false);

    registrar.Parameter("read_memory_limit", &TThis::ReadMemoryLimit)
        .Default(1_GB);

    registrar.Preprocessor([] (TThis* config) {
        // Use proper workload descriptors.
        // TODO(babenko): avoid passing workload descriptor in config
        config->Writer->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemMerge);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TRepairChunkJobDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TThis::Reader)
        .DefaultNew();
    registrar.Parameter("writer", &TThis::Writer)
        .DefaultNew();

    registrar.Parameter("window_size", &TThis::WindowSize)
        .Default(256_MBs);

    registrar.Preprocessor([] (TThis* config) {
        // Disable target allocation from master.
        config->Writer->UploadReplicationFactor = 1;

        // Use proper workload descriptors.
        // TODO(babenko): avoid passing workload descriptor in config
        config->Writer->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemRepair);

        // Don't populate caches in chunk jobs.
        config->Reader->PopulateCache = false;
        config->Reader->RetryTimeout = TDuration::Minutes(15);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TAutotomizeChunkJobDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TThis::Reader)
        .DefaultNew();
    registrar.Parameter("writer", &TThis::Writer)
        .DefaultNew();

    registrar.Parameter("rpc_timeout", &TThis::RpcTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("fail_jobs", &TThis::FailJobs)
        .Default(false);
    registrar.Parameter("sleep_in_jobs", &TThis::SleepInJobs)
        .Default(false);

    registrar.Preprocessor([] (TThis* config) {
        // Use proper workload descriptors.
        // SystemTabletLogging seems to be a good fit since we what writes to have high priority.
        // TODO(babenko): avoid passing workload descriptor in config
        config->Writer->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemTabletLogging);

        // Don't populate caches in chunk jobs.
        config->Reader->PopulateCache = false;
        config->Reader->RetryTimeout = TDuration::Minutes(15);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TReincarnateChunkJobDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TThis::Reader)
        .DefaultNew();
    registrar.Parameter("writer", &TThis::Writer)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TSealChunkJobDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("reader", &TThis::Reader)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        // Don't populate caches in chunk jobs.
        config->Reader->PopulateCache = false;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJournalManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("multiplexed_changelog", &TThis::MultiplexedChangelog)
        .DefaultNew();
    registrar.Parameter("high_latency_split_changelog", &TThis::HighLatencySplitChangelog)
        .DefaultNew();
    registrar.Parameter("low_latency_split_changelog", &TThis::LowLatencySplitChangelog)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        // Expect many splits -- adjust configuration.
        config->HighLatencySplitChangelog->FlushPeriod = TDuration::Seconds(15);

        // Turn off batching for non-multiplexed split changelogs.
        config->LowLatencySplitChangelog->FlushPeriod = TDuration::Zero();
    });
}

////////////////////////////////////////////////////////////////////////////////

void TJobControllerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("waiting_jobs_timeout", &TThis::WaitingJobsTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("account_master_memory_request", &TThis::AccountMasterMemoryRequest)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TDataNodeConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("lease_transaction_timeout", &TThis::LeaseTransactionTimeout)
        .Default(TDuration::Seconds(120));
    registrar.Parameter("lease_transaction_ping_period", &TThis::LeaseTransactionPingPeriod)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("incremental_heartbeat_period", &TThis::IncrementalHeartbeatPeriod)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("incremental_heartbeat_period_splay", &TThis::IncrementalHeartbeatPeriodSplay)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("register_retry_period", &TThis::RegisterRetryPeriod)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("register_retry_splay", &TThis::RegisterRetrySplay)
        .Default(TDuration::Seconds(3));
    registrar.Parameter("register_timeout", &TThis::RegisterTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("incremental_heartbeat_timeout", &TThis::IncrementalHeartbeatTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("incremental_heartbeat_throttler", &TThis::IncrementalHeartbeatThrottler)
        .DefaultCtor([] {
            auto result = TThroughputThrottlerConfig::Create(1);
            result->Period = TDuration::Minutes(10);
            return result;
        });

    registrar.Parameter("full_heartbeat_timeout", &TThis::FullHeartbeatTimeout)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("job_heartbeat_timeout", &TThis::JobHeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("chunk_meta_cache", &TThis::ChunkMetaCache)
        .DefaultNew();
    registrar.Parameter("blocks_ext_cache", &TThis::BlocksExtCache)
        .DefaultNew();
    registrar.Parameter("block_meta_cache", &TThis::BlockMetaCache)
        .DefaultNew();
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();
    registrar.Parameter("blob_reader_cache", &TThis::BlobReaderCache)
        .DefaultNew();
    registrar.Parameter("changelog_reader_cache", &TThis::ChangelogReaderCache)
        .DefaultNew();
    registrar.Parameter("table_schema_cache", &TThis::TableSchemaCache)
        .DefaultNew();

    registrar.Parameter("session_timeout", &TThis::SessionTimeout)
        .Default(TDuration::Seconds(120));
    registrar.Parameter("session_block_reorder_timeout", &TThis::SessionBlockReorderTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("node_rpc_timeout", &TThis::NodeRpcTimeout)
        .Default(TDuration::Seconds(120));
    registrar.Parameter("peer_update_period", &TThis::PeerUpdatePeriod)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("peer_update_expiration_time", &TThis::PeerUpdateExpirationTime)
        .Default(TDuration::Seconds(40));

    registrar.Parameter("net_out_throttling_limit", &TThis::NetOutThrottlingLimit)
        .GreaterThan(0)
        .Default(512_MB);
    registrar.Parameter("net_out_throttling_duration", &TThis::NetOutThrottlingDuration)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("disk_write_throttling_limit", &TThis::DiskWriteThrottlingLimit)
        .GreaterThan(0)
        .Default(1_GB);
    registrar.Parameter("disk_read_throttling_limit", &TThis::DiskReadThrottlingLimit)
        .GreaterThan(0)
        .Default(512_MB);

    registrar.Parameter("store_locations", &TThis::StoreLocations)
        .Default();

    registrar.Parameter("cache_locations", &TThis::CacheLocations)
        .Default();

    registrar.Parameter("volume_manager", &TThis::VolumeManager)
        .DefaultNew();

    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Default();

    registrar.Parameter("read_rps_out_throttler", &TThis::ReadRpsOutThrottler)
        .DefaultNew();
    registrar.Parameter("announce_chunk_replica_rps_out_throttler", &TThis::AnnounceChunkReplicaRpsOutThrottler)
        .DefaultNew();

    registrar.Parameter("disk_health_checker", &TThis::DiskHealthChecker)
        .DefaultNew();

    registrar.Parameter("publish_disabled_locations", &TThis::PublishDisabledLocations)
        .Default(true);

    registrar.Parameter("max_write_sessions", &TThis::MaxWriteSessions)
        .Default(1000)
        .GreaterThanOrEqual(1);

    registrar.Parameter("max_blocks_per_read", &TThis::MaxBlocksPerRead)
        .GreaterThan(0)
        .Default(100000);
    registrar.Parameter("max_bytes_per_read", &TThis::MaxBytesPerRead)
        .GreaterThan(0)
        .Default(64_MB);
    registrar.Parameter("bytes_per_write", &TThis::BytesPerWrite)
        .GreaterThan(0)
        .Default(16_MB);

    registrar.Parameter("validate_block_checksums", &TThis::ValidateBlockChecksums)
        .Default(true);

    registrar.Parameter("placement_expiration_time", &TThis::PlacementExpirationTime)
        .Default(TDuration::Hours(1));

    registrar.Parameter("sync_directories_on_connect", &TThis::SyncDirectoriesOnConnect)
        .Default(false);

    registrar.Parameter("storage_heavy_thread_count", &TThis::StorageHeavyThreadCount)
        .GreaterThan(0)
        .Default(2);
    registrar.Parameter("storage_light_thread_count", &TThis::StorageLightThreadCount)
        .GreaterThan(0)
        .Default(2);
    registrar.Parameter("storage_lookup_thread_count", &TThis::StorageLookupThreadCount)
        .GreaterThan(0)
        .Default(2);

    registrar.Parameter("max_replication_errors_in_heartbeat", &TThis::MaxReplicationErrorsInHeartbeat)
        .GreaterThan(0)
        .Default(3);
    registrar.Parameter("max_tablet_errors_in_heartbeat", &TThis::MaxTabletErrorsInHeartbeat)
        .GreaterThan(0)
        .Default(10);

    registrar.Parameter("background_artifact_validation_delay", &TThis::BackgroundArtifactValidationDelay)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("p2p", &TThis::P2P)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->ChunkMetaCache->Capacity = 1_GB;
        config->BlocksExtCache->Capacity = 1_GB;
        config->BlockMetaCache->Capacity = 1_GB;
        config->BlockCache->CompressedData->Capacity = 1_GB;
        config->BlockCache->UncompressedData->Capacity = 1_GB;

        config->BlobReaderCache->Capacity = 256;

        config->ChangelogReaderCache->Capacity = 256;
    });

    registrar.Postprocessor([] (TThis* config) {
        for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
            if (!config->Throttlers[kind]) {
                config->Throttlers[kind] = New<TRelativeThroughputThrottlerConfig>();
            }
        }

        // COMPAT(gritukan)
        if (!config->MasterConnector->IncrementalHeartbeatPeriod) {
            config->MasterConnector->IncrementalHeartbeatPeriod = config->IncrementalHeartbeatPeriod;
        }
        if (!config->MasterConnector->JobHeartbeatPeriod) {
            // This is not a mistake!
            config->MasterConnector->JobHeartbeatPeriod = config->IncrementalHeartbeatPeriod;
        }
    });
}

i64 TDataNodeConfig::GetCacheCapacity() const
{
    i64 capacity = 0;
    for (const auto& config : CacheLocations) {
        if (config->Quota) {
            capacity += *config->Quota;
        } else {
            return std::numeric_limits<i64>::max();
        }
    }
    return capacity;
}

////////////////////////////////////////////////////////////////////////////////

void TDataNodeDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("storage_heavy_thread_count", &TThis::StorageHeavyThreadCount)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("storage_light_thread_count", &TThis::StorageLightThreadCount)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("storage_lookup_thread_count", &TThis::StorageLookupThreadCount)
        .GreaterThan(0)
        .Optional();
    registrar.Parameter("master_job_thread_count", &TThis::MasterJobThreadCount)
        .GreaterThan(0)
        .Default(4);

    registrar.Parameter("throttlers", &TThis::Throttlers)
        .Optional();
    registrar.Parameter("read_rps_out_throttler", &TThis::ReadRpsOutThrottler)
        .Optional();
    registrar.Parameter("announce_chunk_replica_rps_out_throttler", &TThis::AnnounceChunkReplicaRpsOutThrottler)
        .Optional();

    registrar.Parameter("chunk_meta_cache", &TThis::ChunkMetaCache)
        .DefaultNew();
    registrar.Parameter("blocks_ext_cache", &TThis::BlocksExtCache)
        .DefaultNew();
    registrar.Parameter("block_meta_cache", &TThis::BlockMetaCache)
        .DefaultNew();
    registrar.Parameter("block_cache", &TThis::BlockCache)
        .DefaultNew();
    registrar.Parameter("blob_reader_cache", &TThis::BlobReaderCache)
        .DefaultNew();
    registrar.Parameter("changelog_reader_cache", &TThis::ChangelogReaderCache)
        .DefaultNew();
    registrar.Parameter("table_schema_cache", &TThis::TableSchemaCache)
        .DefaultNew();
    registrar.Parameter("location_health_checker", &TThis::LocationHealthChecker)
        .DefaultNew();

    registrar.Parameter("master_connector", &TThis::MasterConnector)
        .DefaultNew();

    registrar.Parameter("ally_replica_manager", &TThis::AllyReplicaManager)
        .DefaultNew();

    registrar.Parameter("chunk_reader_retention_timeout", &TThis::ChunkReaderRetentionTimeout)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("artifact_cache_reader", &TThis::ArtifactCacheReader)
        .DefaultNew();

    registrar.Parameter("abort_on_location_disabled", &TThis::AbortOnLocationDisabled)
        .Default(false);

    registrar.Parameter("track_memory_after_session_completion", &TThis::TrackMemoryAfterSessionCompletion)
        .Default(true);

    registrar.Parameter("track_system_jobs_memory", &TThis::TrackSystemJobsMemory)
        .Default(true);

    registrar.Parameter("publish_disabled_locations", &TThis::PublishDisabledLocations)
        .Default();

    registrar.Parameter("use_disable_send_blocks", &TThis::UseDisableSendBlocks)
        .Default(false);

    registrar.Parameter("p2p", &TThis::P2P)
        .Optional();

    registrar.Parameter("io_statistics_update_timeout", &TThis::IOStatisticsUpdateTimeout)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("io_throughput_meter", &TThis::IOThroughputMeter)
        .DefaultNew();

    registrar.Parameter("remove_chunk_job", &TThis::RemoveChunkJob)
        .DefaultNew();
    registrar.Parameter("replicate_chunk_job", &TThis::ReplicateChunkJob)
        .DefaultNew();
    registrar.Parameter("merge_chunks_job", &TThis::MergeChunksJob)
        .DefaultNew();
    registrar.Parameter("repair_chunk_job", &TThis::RepairChunkJob)
        .DefaultNew();
    registrar.Parameter("autotomize_chunk_job", &TThis::AutotomizeChunkJob)
        .DefaultNew();
    registrar.Parameter("reincarnate_chunk_job", &TThis::ReincarnateChunkJob)
        .DefaultNew();
    registrar.Parameter("seal_chunk_job", &TThis::SealChunkJob)
        .DefaultNew();

    registrar.Parameter("store_location_config_per_medium", &TThis::StoreLocationConfigPerMedium)
        .Default();

    registrar.Parameter("net_out_throttling_limit", &TThis::NetOutThrottlingLimit)
        .Default();

    registrar.Parameter("disk_write_throttling_limit", &TThis::DiskWriteThrottlingLimit)
        .Default();
    registrar.Parameter("disk_read_throttling_limit", &TThis::DiskReadThrottlingLimit)
        .Default();

    registrar.Parameter("disable_location_writes_pending_read_size_high_limit", &TThis::DisableLocationWritesPendingReadSizeHighLimit)
        .Default();
    registrar.Parameter("disable_location_writes_pending_read_size_low_limit", &TThis::DisableLocationWritesPendingReadSizeLowLimit)
        .Default();

    registrar.Parameter("testing_options", &TThis::TestingOptions)
        .DefaultNew();

    registrar.Parameter("job_controller", &TThis::JobController)
        .DefaultNew();

    registrar.Postprocessor([] (TThis* config) {
        if (config->DisableLocationWritesPendingReadSizeHighLimit.has_value() != config->DisableLocationWritesPendingReadSizeLowLimit.has_value()) {
            THROW_ERROR_EXCEPTION(
                "\"disable_location_writes_pending_read_size_high_limit\" and "
                "\"disable_location_writes_pending_read_size_low_limit\" must either both be present or absent");
        }

        if (config->DisableLocationWritesPendingReadSizeHighLimit &&
            *config->DisableLocationWritesPendingReadSizeHighLimit < *config->DisableLocationWritesPendingReadSizeLowLimit)
        {
            THROW_ERROR_EXCEPTION(
                "\"disable_location_writes_pending_read_size_high_limit\" must not be less than "
                "\"disable_location_writes_pending_read_size_low_limit\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
