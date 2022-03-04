#include "config.h"

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

TP2PConfig::TP2PConfig()
{
    RegisterParameter("enabled", Enabled)
        .Default(true);

    RegisterParameter("block_cache", BlockCache)
        .DefaultNew();

    RegisterParameter("block_cache_override", BlockCacheOverride)
        .DefaultNew();

    RegisterParameter("tick_period", TickPeriod)
        .Default(TDuration::MilliSeconds(100));
    RegisterParameter("node_refresh_period", NodeRefreshPeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("request_timeout", RequestTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("node_staleness_timeout", NodeStalenessTimeout)
        .Default(TDuration::Minutes(5));

    RegisterParameter("iteration_wait_timeout", IterationWaitTimeout)
        .Default(TDuration::Seconds(1));
    RegisterParameter("max_waiting_requests", MaxWaitingRequests)
        .Default(128);

    RegisterParameter("session_cleaup_period", SessionCleaupPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("session_ttl", SessionTTL)
        .Default(TDuration::Minutes(5));

    RegisterParameter("request_cache", RequestCache)
        .DefaultNew();
    RegisterParameter("request_cache_override", RequestCacheOverride)
        .DefaultNew();

    RegisterParameter("chunk_cooldown_timeout", ChunkCooldownTimeout)
        .Default(TDuration::Minutes(5));
    RegisterParameter("max_distributed_bytes", MaxDistributedBytes)
        .Default(128_MB);
    RegisterParameter("max_block_size", MaxBlockSize)
        .Default(128_MB);
    RegisterParameter("block_counter_reset_ticks", BlockCounterResetTicks)
        .GreaterThan(0)
        .Default(150);
    RegisterParameter("hot_block_threshold", HotBlockThreshold)
        .Default(10);
    RegisterParameter("second_hot_block_threshold", SecondHotBlockThreshold)
        .Default(5);
    RegisterParameter("hot_block_replica_count", HotBlockReplicaCount)
        .Default(3);
    RegisterParameter("block_redistribution_ticks", BlockRedistributionTicks)
        .Default(3000);

    RegisterParameter("node_tag_filter", NodeTagFilter)
        .Default(MakeBooleanFormula("!CLOUD"));

    RegisterPreprocessor([&] {
        // Low default to prevent OOMs in yt-local.
        BlockCache->Capacity = 1_MB;

        // Block cache won't accept blocks larger than Capacity / ShardCount * YoungerSizeFraction.
        //
        // With Capacity = 2G and default ShardCount/YoungerSizeFraction,
        // max block size is equal to 32MB, which is too low.
        //
        // With adjusted defaults, max block size is equal to 256MB.
        BlockCache->ShardCount = 4;
        BlockCache->YoungerSizeFraction = 0.5;

        // Should be good enough.
        RequestCache->Capacity = 16 * 1024;
    });
}

////////////////////////////////////////////////////////////////////////////////

TStoreLocationConfigBase::TStoreLocationConfigBase()
{
    RegisterParameter("quota", Quota)
        .GreaterThanOrEqual(0)
        .Default(std::optional<i64>());
    RegisterParameter("replication_out_throttler", ReplicationOutThrottler)
        .DefaultNew();
    RegisterParameter("tablet_compaction_and_partitioning_out_throttler", TabletCompactionAndPartitioningOutThrottler)
        .DefaultNew();
    RegisterParameter("tablet_logging_out_throttler", TabletLoggingOutThrottler)
        .DefaultNew();
    RegisterParameter("tablet_preload_out_throttler", TabletPreloadOutThrottler)
        .DefaultNew();
    RegisterParameter("tablet_recovery_out_throttler", TabletRecoveryOutThrottler)
        .DefaultNew();
    RegisterParameter("io_engine_type", IOEngineType)
        .Default(NIO::EIOEngineType::ThreadPool);
    RegisterParameter("io_config", IOConfig)
        .Optional();
    RegisterParameter("use_direct_io_for_reads", UseDirectIOForReads)
        .Default(NIO::EDirectIOPolicy::Never);
    RegisterParameter("throttle_counter_interval", ThrottleDuration)
        .Default(TDuration::Seconds(30));
    RegisterParameter("coalesced_read_max_gap_size", CoalescedReadMaxGapSize)
        .GreaterThanOrEqual(0)
        .Default(0);
    RegisterParameter("disk_family", DiskFamily)
        .Default("UNKNOWN");

    RegisterParameter("device_name", DeviceName)
        .Default("UNKNOWN");
    RegisterParameter("device_model", DeviceModel)
        .Default("UNKNOWN");
}

////////////////////////////////////////////////////////////////////////////////

TStoreLocationConfig::TStoreLocationConfig()
{
    RegisterParameter("low_watermark", LowWatermark)
        .GreaterThanOrEqual(0)
        .Default(5_GB);
    RegisterParameter("high_watermark", HighWatermark)
        .GreaterThanOrEqual(0)
        .Default(2_GB);
    RegisterParameter("disable_writes_watermark", DisableWritesWatermark)
        .GreaterThanOrEqual(0)
        .Default(1_GB);
    RegisterParameter("max_trash_ttl", MaxTrashTtl)
        .Default(TDuration::Hours(1))
        .GreaterThanOrEqual(TDuration::Zero());
    RegisterParameter("trash_cleanup_watermark", TrashCleanupWatermark)
        .GreaterThanOrEqual(0)
        .Default(4_GB);
    RegisterParameter("trash_check_period", TrashCheckPeriod)
        .GreaterThanOrEqual(TDuration::Zero())
        .Default(TDuration::Seconds(10));
    RegisterParameter("repair_in_throttler", RepairInThrottler)
        .DefaultNew();
    RegisterParameter("replication_in_throttler", ReplicationInThrottler)
        .DefaultNew();
    RegisterParameter("tablet_comaction_and_partitoning_in_throttler", TabletCompactionAndPartitioningInThrottler)
        .DefaultNew();
    RegisterParameter("tablet_logging_in_throttler", TabletLoggingInThrottler)
        .DefaultNew();
    RegisterParameter("tablet_snapshot_in_throttler", TabletSnapshotInThrottler)
        .DefaultNew();
    RegisterParameter("tablet_store_flush_in_throttler", TabletStoreFlushInThrottler)
        .DefaultNew();

    RegisterParameter("multiplexed_changelog", MultiplexedChangelog)
        .Default();
    RegisterParameter("high_latency_split_changelog", HighLatencySplitChangelog)
        .Default();
    RegisterParameter("low_latency_split_changelog", LowLatencySplitChangelog)
        .Default();

    // NB: base class's field.
    RegisterParameter("medium_name", MediumName)
        .Default(NChunkClient::DefaultStoreMediumName);

    RegisterPostprocessor([&] () {
        if (HighWatermark > LowWatermark) {
            THROW_ERROR_EXCEPTION("\"high_full_watermark\" must be less than or equal to \"low_watermark\"");
        }
        if (DisableWritesWatermark > HighWatermark) {
            THROW_ERROR_EXCEPTION("\"write_disable_watermark\" must be less than or equal to \"high_watermark\"");
        }
        if (DisableWritesWatermark > TrashCleanupWatermark) {
            THROW_ERROR_EXCEPTION("\"disable_writes_watermark\" must be less than or equal to \"trash_cleanup_watermark\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TCacheLocationConfig::TCacheLocationConfig()
{
    RegisterParameter("in_throttler", InThrottler)
        .DefaultNew();

    // NB: base class's field.
    RegisterParameter("medium_name", MediumName)
        .Default(NChunkClient::DefaultCacheMediumName);
}

////////////////////////////////////////////////////////////////////////////////

TMultiplexedChangelogConfig::TMultiplexedChangelogConfig()
{
    RegisterParameter("max_record_count", MaxRecordCount)
        .Default(1000000)
        .GreaterThan(0);
    RegisterParameter("max_data_size", MaxDataSize)
        .Default(256_MB)
        .GreaterThan(0);
    RegisterParameter("auto_rotation_period", AutoRotationPeriod)
        .Default(TDuration::Minutes(15));
    RegisterParameter("replay_buffer_size", ReplayBufferSize)
        .GreaterThan(0)
        .Default(256_MB);
    RegisterParameter("max_clean_changelogs_to_keep", MaxCleanChangelogsToKeep)
        .GreaterThanOrEqual(0)
        .Default(3);
    RegisterParameter("clean_delay", CleanDelay)
        .Default(TDuration::Minutes(1));
    RegisterParameter("big_record_threshold", BigRecordThreshold)
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

TLayerLocationConfig::TLayerLocationConfig()
{
    RegisterParameter("low_watermark", LowWatermark)
        .Default(1_GB)
        .GreaterThanOrEqual(0);

    RegisterParameter("quota", Quota)
        .Default();

    RegisterParameter("location_is_absolute", LocationIsAbsolute)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TTmpfsLayerCacheConfig::TTmpfsLayerCacheConfig()
{
    RegisterParameter("capacity", Capacity)
        .Default(10 * 1_GB)
        .GreaterThan(0);

    RegisterParameter("layers_directory_path", LayersDirectoryPath)
        .Default(std::nullopt);

    RegisterParameter("layers_update_period", LayersUpdatePeriod)
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

TVolumeManagerConfig::TVolumeManagerConfig()
{
    RegisterParameter("porto_executor", PortoExecutor)
        .DefaultNew();

    RegisterParameter("layer_locations", LayerLocations);

    RegisterParameter("enable_layers_cache", EnableLayersCache)
        .Default(true);

    RegisterParameter("cache_capacity_fraction", CacheCapacityFraction)
        .Default(0.8)
        .GreaterThanOrEqual(0)
        .LessThanOrEqual(1);

    RegisterParameter("layer_import_concurrency", LayerImportConcurrency)
        .Default(2)
        .GreaterThan(0)
        .LessThanOrEqual(10);

    RegisterParameter("test_disk_quota", TestDiskQuota)
        .Default(false);

    RegisterParameter("regular_tmpfs_layer_cache", RegularTmpfsLayerCache)
        .Alias("tmpfs_layer_cache")
        .DefaultNew();

    RegisterParameter("nirvana_tmpfs_layer_cache", NirvanaTmpfsLayerCache)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

TMasterConnectorConfig::TMasterConnectorConfig()
{
    RegisterParameter("incremental_heartbeat_period", IncrementalHeartbeatPeriod)
        .Default();
    RegisterParameter("incremental_heartbeat_period_splay", IncrementalHeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
    RegisterParameter("job_heartbeat_period", JobHeartbeatPeriod)
        .Default();
    RegisterParameter("job_heartbeat_period_splay", JobHeartbeatPeriodSplay)
        .Default(TDuration::Seconds(1));
    RegisterParameter("incremental_heartbeat_timeout", IncrementalHeartbeatTimeout)
        .Default();
    RegisterParameter("full_heartbeat_timeout", FullHeartbeatTimeout)
        .Default();
    RegisterParameter("job_heartbeat_timeout", JobHeartbeatTimeout)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TMasterConnectorDynamicConfig::TMasterConnectorDynamicConfig()
{
    RegisterParameter("incremental_heartbeat_period", IncrementalHeartbeatPeriod)
        .Default();
    RegisterParameter("incremental_heartbeat_period_splay", IncrementalHeartbeatPeriodSplay)
        .Default();
    RegisterParameter("job_heartbeat_period", JobHeartbeatPeriod)
        .Default();
    RegisterParameter("job_heartbeat_period_splay", JobHeartbeatPeriodSplay)
        .Default();
    RegisterParameter("max_chunk_events_per_incremental_heartbeat", MaxChunkEventsPerIncrementalHeartbeat)
        .Default(1000000);
    RegisterParameter("enable_profiling", EnableProfiling)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TAllyReplicaManagerDynamicConfig::TAllyReplicaManagerDynamicConfig()
{
    RegisterParameter("announcement_backoff_time", AnnouncementBackoffTime)
        .Default(TDuration::Seconds(5));
    RegisterParameter("max_chunks_per_announcement_request", MaxChunksPerAnnouncementRequest)
        .Default(5'000);
    RegisterParameter("announcement_request_timeout", AnnouncementRequestTimeout)
        .Default(TDuration::Seconds(15));
}

////////////////////////////////////////////////////////////////////////////////

TChunkAutotomizerConfig::TChunkAutotomizerConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(5));

    RegisterParameter("fail_jobs", FailJobs)
        .Default(false);
    RegisterParameter("sleep_in_jobs", SleepInJobs)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TDataNodeTestingOptions::TDataNodeTestingOptions()
{
    RegisterParameter(
        "columnar_statistics_chunk_meta_fetch_max_delay",
        ColumnarStatisticsChunkMetaFetchMaxDelay)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TMediumThroughputMeterConfig::TMediumThroughputMeterConfig()
{
    RegisterParameter("medium_name", MediumName)
        .NonEmpty();

    RegisterParameter("enabled", Enabled)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TIOThroughputMeterConfig::TIOThroughputMeterConfig()
{
    RegisterParameter("enabled", Enabled)
        .Default(false);

    RegisterParameter("mediums", Mediums);

    RegisterParameter("time_between_tests", TimeBetweenTests)
        .Default(TDuration::Hours(12));

    RegisterParameter("testing_time_soft_limit", TestingTimeSoftLimit)
        .Default(TDuration::Minutes(20));

    RegisterParameter("testing_time_hard_limit", TestingTimeHardLimit)
        .Default(TDuration::Minutes(60));

    RegisterParameter("max_congestions_per_test", MaxCongestionsPerTest)
        .Default(20);
}

////////////////////////////////////////////////////////////////////////////////

TDataNodeConfig::TDataNodeConfig()
{
    RegisterParameter("lease_transaction_timeout", LeaseTransactionTimeout)
        .Default(TDuration::Seconds(120));
    RegisterParameter("lease_transaction_ping_period", LeaseTransactionPingPeriod)
        .Default(TDuration::Seconds(15));
    RegisterParameter("incremental_heartbeat_period", IncrementalHeartbeatPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("incremental_heartbeat_period_splay", IncrementalHeartbeatPeriodSplay)
        .Default(TDuration::Seconds(5));
    RegisterParameter("register_retry_period", RegisterRetryPeriod)
        .Default(TDuration::Seconds(3));
    RegisterParameter("register_retry_splay", RegisterRetrySplay)
        .Default(TDuration::Seconds(3));
    RegisterParameter("register_timeout", RegisterTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("incremental_heartbeat_timeout", IncrementalHeartbeatTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("incremental_heartbeat_throttler", IncrementalHeartbeatThrottler)
        .DefaultNew(/* limit */1, /* period */TDuration::Minutes(10));

    RegisterParameter("full_heartbeat_timeout", FullHeartbeatTimeout)
        .Default(TDuration::Seconds(60));
    RegisterParameter("job_heartbeat_timeout", JobHeartbeatTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("chunk_meta_cache", ChunkMetaCache)
        .DefaultNew();
    RegisterParameter("blocks_ext_cache", BlocksExtCache)
        .DefaultNew();
    RegisterParameter("block_meta_cache", BlockMetaCache)
        .DefaultNew();
    RegisterParameter("block_cache", BlockCache)
        .DefaultNew();
    RegisterParameter("blob_reader_cache", BlobReaderCache)
        .DefaultNew();
    RegisterParameter("changelog_reader_cache", ChangelogReaderCache)
        .DefaultNew();
    RegisterParameter("table_schema_cache", TableSchemaCache)
        .DefaultNew();

    RegisterParameter("multiplexed_changelog", MultiplexedChangelog)
        .DefaultNew();
    RegisterParameter("high_latency_split_changelog", HighLatencySplitChangelog)
        .DefaultNew();
    RegisterParameter("low_latency_split_changelog", LowLatencySplitChangelog)
        .DefaultNew();

    RegisterParameter("session_timeout", SessionTimeout)
        .Default(TDuration::Seconds(120));
    RegisterParameter("node_rpc_timeout", NodeRpcTimeout)
        .Default(TDuration::Seconds(120));
    RegisterParameter("peer_update_period", PeerUpdatePeriod)
        .Default(TDuration::Seconds(30));
    RegisterParameter("peer_update_expiration_time", PeerUpdateExpirationTime)
        .Default(TDuration::Seconds(40));

    RegisterParameter("net_out_throttling_limit", NetOutThrottlingLimit)
        .GreaterThan(0)
        .Default(512_MB);
    RegisterParameter("net_out_throttling_extra_limit", NetOutThrottlingExtraLimit)
        .GreaterThan(0)
        .Default(512_MB);
    RegisterParameter("net_out_throttle_duration", NetOutThrottleDuration)
        .Default(TDuration::Seconds(30));

    RegisterParameter("disk_write_throttling_limit", DiskWriteThrottlingLimit)
        .GreaterThan(0)
        .Default(1_GB);
    RegisterParameter("disk_read_throttling_limit", DiskReadThrottlingLimit)
        .GreaterThan(0)
        .Default(512_MB);

    RegisterParameter("store_locations", StoreLocations)
        .Default();

    RegisterParameter("cache_locations", CacheLocations)
        .Default();

    RegisterParameter("volume_manager", VolumeManager)
        .DefaultNew();

    RegisterParameter("replication_writer", ReplicationWriter)
        .DefaultNew();
    RegisterParameter("repair_reader", RepairReader)
        .DefaultNew();
    RegisterParameter("repair_writer", RepairWriter)
        .DefaultNew();

    RegisterParameter("seal_reader", SealReader)
        .DefaultNew();

    RegisterParameter("merge_reader", MergeReader)
        .DefaultNew();
    RegisterParameter("merge_writer", MergeWriter)
        .DefaultNew();

    RegisterParameter("autotomy_reader", AutotomyReader)
        .DefaultNew();
    RegisterParameter("autotomy_writer", AutotomyWriter)
        .DefaultNew();

    RegisterParameter("throttlers", Throttlers)
        .Optional();

    // COMPAT(babenko): use /data_node/throttlers instead.
    RegisterParameter("total_in_throttler", Throttlers[EDataNodeThrottlerKind::TotalIn])
        .Optional();
    RegisterParameter("total_out_throttler", Throttlers[EDataNodeThrottlerKind::TotalOut])
        .Optional();
    RegisterParameter("replication_in_throttler", Throttlers[EDataNodeThrottlerKind::ReplicationIn])
        .Optional();
    RegisterParameter("replication_out_throttler", Throttlers[EDataNodeThrottlerKind::ReplicationOut])
        .Optional();
    RegisterParameter("repair_in_throttler", Throttlers[EDataNodeThrottlerKind::RepairIn])
        .Optional();
    RegisterParameter("repair_out_throttler", Throttlers[EDataNodeThrottlerKind::RepairOut])
        .Optional();
    RegisterParameter("artifact_cache_in_throttler", Throttlers[EDataNodeThrottlerKind::ArtifactCacheIn])
        .Optional();
    RegisterParameter("artifact_cache_out_throttler", Throttlers[EDataNodeThrottlerKind::ArtifactCacheOut])
        .Optional();
    RegisterParameter("skynet_out_throttler", Throttlers[EDataNodeThrottlerKind::SkynetOut])
        .Optional();
    RegisterParameter("tablet_comaction_and_partitoning_in_throttler", Throttlers[EDataNodeThrottlerKind::TabletCompactionAndPartitioningIn])
        .Optional();
    RegisterParameter("tablet_comaction_and_partitoning_out_throttler", Throttlers[EDataNodeThrottlerKind::TabletCompactionAndPartitioningOut])
        .Optional();
    RegisterParameter("tablet_logging_in_throttler", Throttlers[EDataNodeThrottlerKind::TabletLoggingIn])
        .Optional();
    RegisterParameter("tablet_preload_out_throttler", Throttlers[EDataNodeThrottlerKind::TabletPreloadOut])
        .Optional();
    RegisterParameter("tablet_snapshot_in_throttler", Throttlers[EDataNodeThrottlerKind::TabletSnapshotIn])
        .Optional();
    RegisterParameter("tablet_store_flush_in_throttler", Throttlers[EDataNodeThrottlerKind::TabletStoreFlushIn])
        .Optional();
    RegisterParameter("tablet_recovery_out_throttler", Throttlers[EDataNodeThrottlerKind::TabletRecoveryOut])
        .Optional();
    RegisterParameter("tablet_replication_out_throttler", Throttlers[EDataNodeThrottlerKind::TabletReplicationOut])
        .Optional();

    RegisterParameter("read_rps_out_throttler", ReadRpsOutThrottler)
        .DefaultNew();
    RegisterParameter("announce_chunk_replica_rps_out_throttler", AnnounceChunkReplicaRpsOutThrottler)
        .DefaultNew();

    RegisterParameter("disk_health_checker", DiskHealthChecker)
        .DefaultNew();

    RegisterParameter("max_write_sessions", MaxWriteSessions)
        .Default(1000)
        .GreaterThanOrEqual(1);

    RegisterParameter("max_blocks_per_read", MaxBlocksPerRead)
        .GreaterThan(0)
        .Default(100000);
    RegisterParameter("max_bytes_per_read", MaxBytesPerRead)
        .GreaterThan(0)
        .Default(64_MB);
    RegisterParameter("bytes_per_write", BytesPerWrite)
        .GreaterThan(0)
        .Default(16_MB);

    RegisterParameter("validate_block_checksums", ValidateBlockChecksums)
        .Default(true);

    RegisterParameter("placement_expiration_time", PlacementExpirationTime)
        .Default(TDuration::Hours(1));

    RegisterParameter("sync_directories_on_connect", SyncDirectoriesOnConnect)
        .Default(false);

    RegisterParameter("storage_heavy_thread_count", StorageHeavyThreadCount)
        .GreaterThan(0)
        .Default(2);
    RegisterParameter("storage_light_thread_count", StorageLightThreadCount)
        .GreaterThan(0)
        .Default(2);
    RegisterParameter("storage_lookup_thread_count", StorageLookupThreadCount)
        .GreaterThan(0)
        .Default(2);

    RegisterParameter("max_replication_errors_in_heartbeat", MaxReplicationErrorsInHeartbeat)
        .GreaterThan(0)
        .Default(3);
    RegisterParameter("max_tablet_errors_in_heartbeat", MaxTabletErrorsInHeartbeat)
        .GreaterThan(0)
        .Default(10);

    RegisterParameter("block_read_timeout_fraction", BlockReadTimeoutFraction)
        .Default(0.75);

    RegisterParameter("columnar_statistics_read_timeout_fraction", ColumnarStatisticsReadTimeoutFraction)
        .Default(0.75);

    RegisterParameter("background_artifact_validation_delay", BackgroundArtifactValidationDelay)
        .Default(TDuration::Minutes(5));

    RegisterParameter("master_connector", MasterConnector)
        .DefaultNew();

    RegisterParameter("p2p", P2P)
        .DefaultNew();

    RegisterParameter("testing_options", TestingOptions)
        .DefaultNew();

    RegisterPreprocessor([&] {
        ChunkMetaCache->Capacity = 1_GB;
        BlocksExtCache->Capacity = 1_GB;
        BlockMetaCache->Capacity = 1_GB;
        BlockCache->CompressedData->Capacity = 1_GB;
        BlockCache->UncompressedData->Capacity = 1_GB;

        BlobReaderCache->Capacity = 256;

        ChangelogReaderCache->Capacity = 256;

        // Expect many splits -- adjust configuration.
        HighLatencySplitChangelog->FlushPeriod = TDuration::Seconds(15);

        // Turn off batching for non-multiplexed split changelogs.
        LowLatencySplitChangelog->FlushPeriod = TDuration::Zero();

        // Disable target allocation from master.
        ReplicationWriter->UploadReplicationFactor = 1;
        RepairWriter->UploadReplicationFactor = 1;

        // Use proper workload descriptors.
        // TODO(babenko): avoid passing workload descriptor in config
        RepairWriter->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemRepair);
        ReplicationWriter->WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::SystemReplication);

        // Don't populate caches in chunk jobs.
        RepairReader->PopulateCache = false;
        RepairReader->RetryTimeout = TDuration::Minutes(15);
        SealReader->PopulateCache = false;
    });

    RegisterPostprocessor([&] {
        // Instantiate default throttler configs.
        for (auto kind : TEnumTraits<EDataNodeThrottlerKind>::GetDomainValues()) {
            if (!Throttlers[kind]) {
                Throttlers[kind] = New<NConcurrency::TRelativeThroughputThrottlerConfig>();
            }
        }

        // COMPAT(gritukan)
        if (!MasterConnector->IncrementalHeartbeatPeriod) {
            MasterConnector->IncrementalHeartbeatPeriod = IncrementalHeartbeatPeriod;
        }
        if (!MasterConnector->JobHeartbeatPeriod) {
            // This is not a mistake!
            MasterConnector->JobHeartbeatPeriod = IncrementalHeartbeatPeriod;
        }
        if (!MasterConnector->FullHeartbeatTimeout) {
            MasterConnector->FullHeartbeatTimeout = FullHeartbeatTimeout;
        }
        if (!MasterConnector->IncrementalHeartbeatTimeout) {
            MasterConnector->IncrementalHeartbeatTimeout = IncrementalHeartbeatTimeout;
        }
        if (!MasterConnector->JobHeartbeatTimeout) {
            MasterConnector->JobHeartbeatTimeout = JobHeartbeatTimeout;
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

i64 TDataNodeConfig::GetNetOutThrottlingHardLimit() const
{
    return NetOutThrottlingLimit + NetOutThrottlingExtraLimit;
}

////////////////////////////////////////////////////////////////////////////////

TDataNodeDynamicConfig::TDataNodeDynamicConfig()
{
    RegisterParameter("storage_heavy_thread_count", StorageHeavyThreadCount)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("storage_light_thread_count", StorageLightThreadCount)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("storage_lookup_thread_count", StorageLookupThreadCount)
        .GreaterThan(0)
        .Optional();
    RegisterParameter("master_job_thread_count", MasterJobThreadCount)
        .GreaterThan(0)
        .Default(4);

    RegisterParameter("throttlers", Throttlers)
        .Optional();
    RegisterParameter("read_rps_out_throttler", ReadRpsOutThrottler)
        .Optional();
    RegisterParameter("announce_chunk_replica_rps_out_throttler", AnnounceChunkReplicaRpsOutThrottler)
        .Optional();

    RegisterParameter("chunk_meta_cache", ChunkMetaCache)
        .DefaultNew();
    RegisterParameter("blocks_ext_cache", BlocksExtCache)
        .DefaultNew();
    RegisterParameter("block_meta_cache", BlockMetaCache)
        .DefaultNew();
    RegisterParameter("block_cache", BlockCache)
        .DefaultNew();
    RegisterParameter("blob_reader_cache", BlobReaderCache)
        .DefaultNew();
    RegisterParameter("changelog_reader_cache", ChangelogReaderCache)
        .DefaultNew();
    RegisterParameter("table_schema_cache", TableSchemaCache)
        .DefaultNew();

    RegisterParameter("master_connector", MasterConnector)
        .DefaultNew();

    RegisterParameter("ally_replica_manager", AllyReplicaManager)
        .DefaultNew();

    RegisterParameter("chunk_reader_retention_timeout", ChunkReaderRetentionTimeout)
        .Default(TDuration::Minutes(1));

    RegisterParameter("artifact_cache_reader", ArtifactCacheReader)
        .DefaultNew();

    RegisterParameter("abort_on_location_disabled", AbortOnLocationDisabled)
        .Default(true);

    RegisterParameter("p2p", P2P)
        .Optional();

    RegisterParameter("chunk_autotomizer", ChunkAutotomizer)
        .DefaultNew();

    RegisterParameter("io_statistics_update_timeout", IOStatisticsUpdateTimeout)
        .Default(TDuration::Seconds(10));

    RegisterParameter("adaptive_chunk_repair_job", AdaptiveChunkRepairJob)
        .Optional();

    RegisterPostprocessor([&] {
        if (!AdaptiveChunkRepairJob) {
            AdaptiveChunkRepairJob = New<NChunkClient::TErasureReaderConfig>();
            AdaptiveChunkRepairJob->EnableAutoRepair = false;
        }
    });

    RegisterParameter("io_throughput_meter", IOThroughputMeter)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
