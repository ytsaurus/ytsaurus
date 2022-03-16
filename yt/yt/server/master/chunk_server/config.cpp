#include "config.h"

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

TChunkManagerConfig::TChunkManagerConfig()
{
    RegisterParameter("max_replication_factor", MaxReplicationFactor)
        .GreaterThanOrEqual(NChunkClient::DefaultReplicationFactor)
        .Default(NChunkClient::MaxReplicationFactor);
    RegisterParameter("max_replicas_per_rack", MaxReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());
    RegisterParameter("max_regular_replicas_per_rack", MaxRegularReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());
    RegisterParameter("max_journal_replicas_per_rack", MaxJournalReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());
    RegisterParameter("max_erasure_replicas_per_rack", MaxErasureReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());

    RegisterParameter("allow_multiple_erasure_parts_per_node", AllowMultipleErasurePartsPerNode)
        .Default(false);

    RegisterParameter("replicator_enabled_check_period", ReplicatorEnabledCheckPeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("repair_queue_balancer_weight_decay_interval", RepairQueueBalancerWeightDecayInterval)
        .Default(TDuration::Seconds(60));
    RegisterParameter("repair_queue_balancer_weight_decay_factor", RepairQueueBalancerWeightDecayFactor)
        .Default(0.5);
}

////////////////////////////////////////////////////////////////////////////////

TMediumConfig::TMediumConfig()
{
    RegisterParameter("max_replication_factor", MaxReplicationFactor)
        .GreaterThanOrEqual(NChunkClient::DefaultReplicationFactor)
        .Default(NChunkClient::MaxReplicationFactor);
    RegisterParameter("max_replicas_per_rack", MaxReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    RegisterParameter("max_regular_replicas_per_rack", MaxRegularReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    RegisterParameter("max_journal_replicas_per_rack", MaxJournalReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    RegisterParameter("max_erasure_replicas_per_rack", MaxErasureReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    RegisterParameter("prefer_local_host_for_dynamic_tables", PreferLocalHostForDynamicTables)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicChunkMergerConfig::TDynamicChunkMergerConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);

    RegisterParameter("max_chunk_count", MaxChunkCount)
        .GreaterThan(1)
        .Default(20);
    RegisterParameter("min_chunk_count", MinChunkCount)
        .GreaterThan(1)
        .Default(2);
    RegisterParameter("min_shallow_merge_chunk_count", MinShallowMergeChunkCount)
        .GreaterThan(1)
        .Default(5);

    RegisterParameter("max_row_count", MaxRowCount)
        .GreaterThan(0)
        .Default(1000000);
    RegisterParameter("max_data_weight", MaxDataWeight)
        .GreaterThan(0)
        .Default(1_GB);
    RegisterParameter("max_uncompressed_data_size", MaxUncompressedDataSize)
        .GreaterThan(0)
        .Default(2_GB);
    RegisterParameter("max_input_chunk_data_weight", MaxInputChunkDataWeight)
        .GreaterThan(0)
        .Default(512_MB);

    RegisterParameter("max_block_count", MaxBlockCount)
        .GreaterThan(0)
        .Default(250);
    RegisterParameter("max_jobs_per_chunk_list", MaxJobsPerChunkList)
        .GreaterThan(0)
        .Default(50);

    RegisterParameter("schedule_period", SchedulePeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("create_chunks_period", CreateChunksPeriod)
        .Default(TDuration::Seconds(1));
    RegisterParameter("transaction_update_period", TransactionUpdatePeriod)
        .Default(TDuration::Minutes(10));
    RegisterParameter("session_finalization_period", SessionFinalizationPeriod)
        .Default(TDuration::Seconds(10));

    RegisterParameter("create_chunks_batch_size", CreateChunksBatchSize)
        .GreaterThan(0)
        .Default(100);
    RegisterParameter("session_finalization_batch_size", SessionFinalizationBatchSize)
        .GreaterThan(0)
        .Default(100);

    RegisterParameter("queue_size_limit", QueueSizeLimit)
        .GreaterThan(0)
        .Default(100000);
    RegisterParameter("max_running_job_count", MaxRunningJobCount)
        .GreaterThan(1)
        .Default(100000);

    RegisterParameter("shallow_merge_validation_probability", ShallowMergeValidationProbability)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicDataNodeTrackerConfig::TDynamicDataNodeTrackerConfig()
{
    RegisterParameter("max_concurrent_full_heartbeats", MaxConcurrentFullHeartbeats)
        .Default(1)
        .GreaterThan(0);
    RegisterParameter("max_concurrent_incremental_heartbeats", MaxConcurrentIncrementalHeartbeats)
        .Default(10)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

using TChunkTreeBalancerSettingsPtr = TIntrusivePtr<TChunkTreeBalancerSettings>;

void TChunkTreeBalancerSettings::RegisterParameters(
    int maxChunkTreeRank,
    int minChunkListSize,
    int maxChunkListSize,
    double minChunkListToChunkRatio)
{
    RegisterParameter("max_chunk_tree_rank", MaxChunkTreeRank)
        .GreaterThan(0)
        .Default(maxChunkTreeRank);
    RegisterParameter("min_chunk_list_size", MinChunkListSize)
        .GreaterThan(0)
        .Default(minChunkListSize);
    RegisterParameter("max_chunk_list_size", MaxChunkListSize)
        .GreaterThan(0)
        .Default(maxChunkListSize);
    RegisterParameter("min_chunk_list_to_chunk_ratio", MinChunkListToChunkRatio)
        .GreaterThan(0)
        .Default(minChunkListToChunkRatio);
}

TChunkTreeBalancerSettingsPtr TChunkTreeBalancerSettings::NewWithStrictDefaults()
{
    auto result = New<TChunkTreeBalancerSettings>();
    result->RegisterParameters(32, 1024, 2048, 0.01);
    return result;
}

TChunkTreeBalancerSettingsPtr TChunkTreeBalancerSettings::NewWithPermissiveDefaults()
{
    auto result = New<TChunkTreeBalancerSettings>();
    result->RegisterParameters(64, 1024, 4096, 0.05);
    return result;
}

TChunkTreeBalancerSettingsPtr TDynamicChunkTreeBalancerConfig::GetSettingsForMode(EChunkTreeBalancerMode mode)
{
    switch (mode) {
        case EChunkTreeBalancerMode::Permissive:
            return PermissiveSettings;
        case EChunkTreeBalancerMode::Strict:
            return StrictSettings;
    }
}

////////////////////////////////////////////////////////////////////////////////

TDynamicAllyReplicaManagerConfig::TDynamicAllyReplicaManagerConfig()
{
    RegisterParameter("enable_ally_replica_announcement", EnableAllyReplicaAnnouncement)
        .Default(false);

    RegisterParameter("enable_endorsements", EnableEndorsements)
        .Default(false);

    RegisterParameter("underreplicated_chunk_announcement_request_delay", UnderreplicatedChunkAnnouncementRequestDelay)
        .Default(TDuration::Seconds(60));

    RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default();

    RegisterParameter("safe_lost_chunk_count", SafeLostChunkCount)
        .GreaterThanOrEqual(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TDynamicChunkAutotomizerConfig::TDynamicChunkAutotomizerConfig()
{
    RegisterParameter("transaction_update_period", TransactionUpdatePeriod)
        .Default(TDuration::Minutes(10));

    RegisterParameter("refresh_period", RefreshPeriod)
        .Default(TDuration::MilliSeconds(500));

    RegisterParameter("chunk_unstage_period", ChunkUnstagePeriod)
        .Default(TDuration::Seconds(5));

    RegisterParameter("tail_chunks_per_allocation", TailChunksPerAllocation)
        .Default(2);

    RegisterParameter("max_chunks_per_unstage", MaxChunksPerUnstage)
        .Default(1000);

    RegisterParameter("max_chunks_per_refresh", MaxChunksPerRefresh)
        .Default(5000);
    RegisterParameter("max_changed_chunks_per_refresh", MaxChangedChunksPerRefresh)
        .Default(1000);

    RegisterParameter("max_concurrent_jobs_per_chunk", MaxConcurrentJobsPerChunk)
        .Default(3);

    RegisterParameter("job_speculaltion_timeout", JobSpeculationTimeout)
        .Default(TDuration::Seconds(3));

    RegisterParameter("job_timeout", JobTimeout)
        .Default(TDuration::Seconds(30));

    RegisterParameter("schedule_urgent_jobs", ScheduleUrgentJobs)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicChunkManagerTestingConfig::TDynamicChunkManagerTestingConfig()
{
    RegisterParameter("force_unreliable_seal", ForceUnreliableSeal)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

TDynamicConsistentReplicaPlacementConfig::TDynamicConsistentReplicaPlacementConfig()
{
    RegisterParameter("enable", Enable)
        .Default(false);

    RegisterParameter("token_distribution_bucket_count", TokenDistributionBucketCount)
        .Default(5)
        .GreaterThanOrEqual(1);

    RegisterParameter("token_redistribution_period", TokenRedistributionPeriod)
        .Default(TDuration::Seconds(30));

    RegisterParameter("tokens_per_node", TokensPerNode)
        .Default(1)
        .GreaterThanOrEqual(1);

    RegisterParameter("replicas_per_chunk", ReplicasPerChunk)
        .Default(DefaultConsistentReplicaPlacementReplicasPerChunk)
        .GreaterThanOrEqual(std::max(
            ChunkReplicaIndexBound,
            NChunkClient::MaxReplicationFactor));
}

////////////////////////////////////////////////////////////////////////////////

TDynamicChunkManagerConfig::TDynamicChunkManagerConfig()
{
    RegisterParameter("enable_chunk_replicator", EnableChunkReplicator)
        .Default(true);

    RegisterParameter("enable_chunk_sealer", EnableChunkSealer)
        .Default(true);

    RegisterParameter("enable_chunk_autotomizer", EnableChunkAutotomizer)
        .Default(false);

    RegisterParameter("replica_approve_timeout", ReplicaApproveTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("max_misscheduled_replication_jobs_per_heartbeat", MaxMisscheduledReplicationJobsPerHeartbeat)
        .Default(128);
    RegisterParameter("max_misscheduled_repair_jobs_per_heartbeat", MaxMisscheduledRepairJobsPerHeartbeat)
        .Default(128);
    RegisterParameter("max_misscheduled_removal_jobs_per_heartbeat", MaxMisscheduledRemovalJobsPerHeartbeat)
        .Default(128);
    RegisterParameter("max_misscheduled_seal_jobs_per_heartbeat", MaxMisscheduledSealJobsPerHeartbeat)
        .Default(128);
    RegisterParameter("max_misscheduled_merge_jobs_per_heartbeat", MaxMisscheduledMergeJobsPerHeartbeat)
        .Default(128);

    RegisterParameter("min_chunk_balancing_fill_factor_diff", MinChunkBalancingFillFactorDiff)
        .InRange(0.0, 1.0)
        .Default(0.2);
    RegisterParameter("min_chunk_balancing_fill_factor", MinChunkBalancingFillFactor)
        .InRange(0.0, 1.0)
        .Default(0.1);

    RegisterParameter("enable_chunk_refresh", EnableChunkRefresh)
        .Default(true);
    RegisterParameter("chunk_refresh_delay", ChunkRefreshDelay)
        .Default(TDuration::Seconds(30));
    RegisterParameter("chunk_refresh_period", ChunkRefreshPeriod)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("max_blob_chunks_per_refresh", MaxBlobChunksPerRefresh)
        .Default(8000)
        .Alias("max_chunks_per_refresh");
    RegisterParameter("max_time_per_blob_chunk_refresh", MaxTimePerBlobChunkRefresh)
        .Default(TDuration::MilliSeconds(80))
        .Alias("max_time_per_refresh");
    RegisterParameter("max_journal_chunks_per_refresh", MaxJournalChunksPerRefresh)
        .Default(6000);
    RegisterParameter("max_time_per_journal_chunk_refresh", MaxTimePerJournalChunkRefresh)
        .Default(TDuration::MilliSeconds(60));

    RegisterParameter("enable_chunk_requisition_update", EnableChunkRequisitionUpdate)
        .Default(true);
    RegisterParameter("chunk_requisition_update_period", ChunkRequisitionUpdatePeriod)
        .Default(TDuration::MilliSeconds(100));

    RegisterParameter("max_blob_chunks_per_requisition_update", MaxBlobChunksPerRequisitionUpdate)
        .Default(8000)
        .Alias("max_chunks_per_requisition_update");
    RegisterParameter("max_time_per_blob_chunk_requisition_update", MaxTimePerBlobChunkRequisitionUpdate)
        .Default(TDuration::MilliSeconds(80))
        .Alias("max_time_per_requisition_update");
    RegisterParameter("max_journal_chunks_per_requisition_update", MaxJournalChunksPerRequisitionUpdate)
        .Default(6000);
    RegisterParameter("max_time_per_journal_chunk_requisition_update", MaxTimePerJournalChunkRequisitionUpdate)
        .Default(TDuration::MilliSeconds(60));

    RegisterParameter("finished_chunk_lists_requisition_traverse_flush_period", FinishedChunkListsRequisitionTraverseFlushPeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("chunk_seal_backoff_time", ChunkSealBackoffTime)
        .Default(TDuration::Seconds(30));
    RegisterParameter("journal_rpc_timeout", JournalRpcTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("quorum_session_delay", QuorumSessionDelay)
        .Default(TDuration::Seconds(5));
    RegisterParameter("max_chunks_per_seal", MaxChunksPerSeal)
        .GreaterThan(0)
        .Default(10000);
    RegisterParameter("max_concurrent_chunk_seals", MaxConcurrentChunkSeals)
        .GreaterThan(0)
        .Default(10);

    RegisterParameter("max_chunks_per_fetch", MaxChunksPerFetch)
        .GreaterThan(0)
        .Default(1000000);
    RegisterParameter("max_cached_replicas_per_fetch", MaxCachedReplicasPerFetch)
        .GreaterThanOrEqual(0)
        .Default(20);

    RegisterParameter("job_timeout", JobTimeout)
        .Default(TDuration::Minutes(5));

    RegisterParameter("safe_online_node_count", SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default(0);
    RegisterParameter("safe_lost_chunk_fraction", SafeLostChunkFraction)
        .InRange(0.0, 1.0)
        .Default(0.5);
    RegisterParameter("safe_lost_chunk_count", SafeLostChunkCount)
        .GreaterThan(0)
        .Default(1000);

    RegisterParameter("max_replication_write_sessions", MaxReplicationWriteSessions)
        .GreaterThanOrEqual(1)
        .Default(128);

    RegisterParameter("repair_job_memory_usage", RepairJobMemoryUsage)
        .Default(256_MB)
        .GreaterThanOrEqual(0);

    RegisterParameter("job_throttler", JobThrottler)
        .DefaultNew();

    RegisterParameter("staged_chunk_expiration_timeout", StagedChunkExpirationTimeout)
        .Default(TDuration::Hours(1))
        .GreaterThanOrEqual(TDuration::Minutes(10));
    RegisterParameter("expiration_check_period", ExpirationCheckPeriod)
        .Default(TDuration::Minutes(1));
    RegisterParameter("max_expired_chunks_unstages_per_commit", MaxExpiredChunksUnstagesPerCommit)
        .Default(1000);

    RegisterParameter("max_heavy_columns", MaxHeavyColumns)
        .Default(30)
        .GreaterThanOrEqual(0);

    RegisterParameter("deprecated_codec_ids", DeprecatedCodecIds)
        .Default();

    RegisterParameter("deprecated_codec_name_to_alias", DeprecatedCodecNameToAlias)
        .Default();

    RegisterParameter("max_oldest_part_missing_chunks", MaxOldestPartMissingChunks)
        .Default(100);

    RegisterParameter("chunk_removal_job_replicas_expiration_time", ChunkRemovalJobReplicasExpirationTime)
        .Default(TDuration::Minutes(15));

    RegisterParameter("data_node_tracker", DataNodeTracker)
        .DefaultNew();

    RegisterParameter("chunk_tree_balancer", ChunkTreeBalancer)
        .DefaultNew();

    RegisterParameter("chunk_merger", ChunkMerger)
        .DefaultNew();

    RegisterParameter("ally_replica_manager", AllyReplicaManager)
        .DefaultNew();

    RegisterParameter("consistent_replica_placement", ConsistentReplicaPlacement)
        .DefaultNew();

    RegisterParameter("locate_chunks_cached_replica_count_limit", LocateChunksCachedReplicaCountLimit)
        .Default(std::nullopt)
        .DontSerializeDefault();

    RegisterParameter("destroyed_replicas_profiling_period", DestroyedReplicasProfilingPeriod)
        .Default(TDuration::Minutes(5))
        .DontSerializeDefault();

    RegisterParameter("chunk_autotomizer", ChunkAutotomizer)
        .DefaultNew();

    RegisterParameter("finished_jobs_queue_size", FinishedJobsQueueSize)
        .GreaterThanOrEqual(0)
        .Default(50'000);

    RegisterParameter("enable_per_node_incremental_heartbeat_profiling", EnablePerNodeIncrementalHeartbeatProfiling)
        .Default(false)
        .DontSerializeDefault();

    RegisterParameter("testing", Testing)
        .DefaultNew();

    RegisterParameter("use_data_center_aware_replicator", UseDataCenterAwareReplicator)
        .Default(false)
        .DontSerializeDefault();

    RegisterParameter("storage_data_centers", StorageDataCenters)
        .Default()
        .DontSerializeDefault();

    RegisterParameter("banned_storage_data_centers", BannedStorageDataCenters)
        .Default()
        .DontSerializeDefault();

    RegisterPreprocessor([&] {
        JobThrottler->Limit = 10000;
    });
}

////////////////////////////////////////////////////////////////////////////////

TDynamicChunkServiceConfig::TDynamicChunkServiceConfig()
{
    RegisterParameter("enable_mutation_boomerangs", EnableMutationBoomerangs)
        .Default(true);
    
    RegisterParameter(
        "enable_alert_on_chunk_confirmation_without_location_uuid",
        EnableAlertOnChunkConfirmationWithoutLocationUuid)
        .Default(false)
        .DontSerializeDefault();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
