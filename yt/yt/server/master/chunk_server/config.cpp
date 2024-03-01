#include "config.h"

#include "helpers.h"

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/core/concurrency/config.h>

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

void TChunkManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_replication_factor", &TThis::MaxReplicationFactor)
        .GreaterThanOrEqual(NChunkClient::DefaultReplicationFactor)
        .Default(NChunkClient::MaxReplicationFactor);
    registrar.Parameter("max_replicas_per_rack", &TThis::MaxReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_regular_replicas_per_rack", &TThis::MaxRegularReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_journal_replicas_per_rack", &TThis::MaxJournalReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_erasure_replicas_per_rack", &TThis::MaxErasureReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_erasure_journal_replicas_per_rack", &TThis::MaxErasureJournalReplicasPerRack)
        .GreaterThan(0)
        .Default(std::numeric_limits<int>::max());

    registrar.Parameter("allow_multiple_erasure_parts_per_node", &TThis::AllowMultipleErasurePartsPerNode)
        .Default(false);

    registrar.Parameter("repair_queue_balancer_weight_decay_interval", &TThis::RepairQueueBalancerWeightDecayInterval)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("repair_queue_balancer_weight_decay_factor", &TThis::RepairQueueBalancerWeightDecayFactor)
        .Default(0.5);
}

////////////////////////////////////////////////////////////////////////////////

void TDomesticMediumConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_replication_factor", &TThis::MaxReplicationFactor)
        .GreaterThanOrEqual(NChunkClient::DefaultReplicationFactor)
        .Default(NChunkClient::MaxReplicationFactor);
    registrar.Parameter("max_replicas_per_rack", &TThis::MaxReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_regular_replicas_per_rack", &TThis::MaxRegularReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_journal_replicas_per_rack", &TThis::MaxJournalReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_erasure_replicas_per_rack", &TThis::MaxErasureReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("max_erasure_journal_replicas_per_rack", &TThis::MaxErasureJournalReplicasPerRack)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<int>::max());
    registrar.Parameter("prefer_local_host_for_dynamic_tables", &TThis::PreferLocalHostForDynamicTables)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TS3MediumConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

void TDynamicChunkMergerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("max_chunk_count", &TThis::MaxChunkCount)
        .GreaterThan(1)
        .Default(20);
    registrar.Parameter("min_chunk_count", &TThis::MinChunkCount)
        .GreaterThan(1)
        .Default(2);
    registrar.Parameter("min_shallow_merge_chunk_count", &TThis::MinShallowMergeChunkCount)
        .GreaterThan(1)
        .Default(5);

    registrar.Parameter("max_row_count", &TThis::MaxRowCount)
        .GreaterThan(0)
        .Default(1000000);
    registrar.Parameter("max_data_weight", &TThis::MaxDataWeight)
        .GreaterThan(0)
        .Default(1_GB);
    registrar.Parameter("max_uncompressed_data_size", &TThis::MaxUncompressedDataSize)
        .GreaterThan(0)
        .Default(2_GB);
    registrar.Parameter("max_compressed_data_size", &TThis::MaxCompressedDataSize)
        .GreaterThan(0)
        .Default(512_MB)
        .DontSerializeDefault();
    registrar.Parameter("max_input_chunk_data_weight", &TThis::MaxInputChunkDataWeight)
        .GreaterThan(0)
        .Default(512_MB);

    registrar.Parameter("max_block_count", &TThis::MaxBlockCount)
        .GreaterThan(0)
        .Default(250);
    registrar.Parameter("max_jobs_per_chunk_list", &TThis::MaxJobsPerChunkList)
        .GreaterThan(0)
        .Default(50);

    registrar.Parameter("schedule_period", &TThis::SchedulePeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("create_chunks_period", &TThis::CreateChunksPeriod)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("transaction_update_period", &TThis::TransactionUpdatePeriod)
        .Default(TDuration::Minutes(10));
    registrar.Parameter("session_finalization_period", &TThis::SessionFinalizationPeriod)
        .Default(TDuration::Seconds(10));

    registrar.Parameter("create_chunks_batch_size", &TThis::CreateChunksBatchSize)
        .GreaterThan(0)
        .Default(100);
    registrar.Parameter("session_finalization_batch_size", &TThis::SessionFinalizationBatchSize)
        .GreaterThan(0)
        .Default(100);

    registrar.Parameter("queue_size_limit", &TThis::QueueSizeLimit)
        .GreaterThan(0)
        .Default(100'000);
    registrar.Parameter("max_running_job_count", &TThis::MaxRunningJobCount)
        .GreaterThan(1)
        .Default(100'000);

    registrar.Parameter("shallow_merge_validation_probability", &TThis::ShallowMergeValidationProbability)
        .Default(0);

    registrar.Parameter("reschedule_merge_on_success", &TThis::RescheduleMergeOnSuccess)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("enable_node_statistics_fix", &TThis::EnableNodeStatisticsFix)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("enable_queue_size_limit_changes", &TThis::EnableQueueSizeLimitChanges)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("respect_account_specific_toggle", &TThis::RespectAccountSpecificToggle)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("max_nodes_being_merged", &TThis::MaxNodesBeingMerged)
        .Default(1'000'000)
        .DontSerializeDefault();

    registrar.Parameter("max_allowed_reschedulings_per_table", &TThis::MaxAllowedReschedulingsPerSession)
        .Default(30);

    registrar.Parameter("max_chunks_per_iteration", &TThis::MaxChunksPerIteration)
        .Default();

    registrar.Parameter("delay_between_iterations", &TThis::DelayBetweenIterations)
        .Default();

    registrar.Parameter("allow_setting_chunk_merger_mode", &TThis::AllowSettingChunkMergerMode)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicMasterCellChunkStatisticsCollectorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_chunks_per_scan", &TThis::MaxChunksPerScan)
        .Default(500);
    registrar.Parameter("chunk_scan_period", &TThis::ChunkScanPeriod)
        .Default(TDuration::Minutes(1));

    registrar.Parameter("creation_time_histogram_bucket_bounds", &TThis::CreationTimeHistogramBucketBounds)
        .Default(GenerateChunkCreationTimeHistogramBucketBounds(TInstant::ParseIso8601("2023-02-15 00:00:00Z")));

    registrar.Postprocessor([] (TThis* config) {
        THROW_ERROR_EXCEPTION_IF(config->CreationTimeHistogramBucketBounds.empty(),
            "\"creation_time_histogram_bucket_bounds\" cannot be empty");

        if (std::ssize(config->CreationTimeHistogramBucketBounds) > MaxChunkCreationTimeHistogramBuckets) {
            THROW_ERROR_EXCEPTION("\"creation_time_histogram_bucket_bounds\" is too large")
                << TErrorAttribute("size", std::ssize(config->CreationTimeHistogramBucketBounds))
                << TErrorAttribute("limit", MaxChunkCreationTimeHistogramBuckets);
        }

        Sort(config->CreationTimeHistogramBucketBounds);
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicChunkReincarnatorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);
    registrar.Parameter("chunk_scan_period", &TThis::ChunkScanPeriod)
        .Default(TDuration::Minutes(3));
    registrar.Parameter("max_chunks_per_scan", &TThis::MaxChunksPerScan)
        .GreaterThanOrEqual(0)
        .Default(50);
    registrar.Parameter("max_visited_chunk_ancestors_per_chunk", &TThis::MaxVisitedChunkAncestorsPerChunk)
        .GreaterThan(0)
        .Default(1000);

    registrar.Parameter("min_allowed_creation_time", &TThis::MinAllowedCreationTime)
        .Default(TInstant::FromValue(0));

    registrar.Parameter("max_running_job_count", &TThis::MaxRunningJobCount)
        .GreaterThanOrEqual(1)
        .Default(300);

    registrar.Parameter("replaced_chunk_batch_size", &TThis::ReplacedChunkBatchSize)
        .GreaterThanOrEqual(1)
        .Default(30);

    registrar.Parameter("transaction_update_period", &TThis::TransactionUpdatePeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("max_failed_jobs", &TThis::MaxFailedJobs)
        .GreaterThanOrEqual(0)
        .Default(10);

    registrar.Parameter("max_tracked_chunks", &TThis::MaxTrackedChunks)
        .GreaterThanOrEqual(0)
        .Default(400);

    registrar.Parameter("multicell_reincarnation_transaction_timeout", &TThis::MulticellReincarnationTransactionTimeout)
        .Default(TDuration::Hours(1));

    registrar.Parameter("ignore_account_settings", &TThis::IgnoreAccountSettings)
        .Default(false);

    registrar.Parameter("enable_verbose_logging", &TThis::EnableVerboseLogging)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("forced_underfilled_batch_replacement_period", &TThis::ForcedUnderfilledBatchReplacementPeriod)
        .Default(TDuration::Minutes(5))
        .DontSerializeDefault();
}

bool TDynamicChunkReincarnatorConfig::ShouldRescheduleAfterChange(
    const TDynamicChunkReincarnatorConfig& that) const noexcept
{
    return
        MinAllowedCreationTime != that.MinAllowedCreationTime ||
        MaxVisitedChunkAncestorsPerChunk != that.MaxVisitedChunkAncestorsPerChunk ||
        IgnoreAccountSettings != that.IgnoreAccountSettings;
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicDataNodeTrackerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_concurrent_full_heartbeats", &TThis::MaxConcurrentFullHeartbeats)
        .Default(1)
        .GreaterThan(0);
    registrar.Parameter("max_concurrent_incremental_heartbeats", &TThis::MaxConcurrentIncrementalHeartbeats)
        .Default(10)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

using TChunkTreeBalancerSettingsPtr = TIntrusivePtr<TChunkTreeBalancerSettings>;

void TChunkTreeBalancerSettings::RegisterParameters(
    TRegistrar registrar,
    int maxChunkTreeRank,
    int minChunkListSize,
    int maxChunkListSize,
    double minChunkListToChunkRatio)
{
    registrar.Parameter("max_chunk_tree_rank", &TThis::MaxChunkTreeRank)
        .GreaterThan(0)
        .Default(maxChunkTreeRank);
    registrar.Parameter("min_chunk_list_size", &TThis::MinChunkListSize)
        .GreaterThan(0)
        .Default(minChunkListSize);
    registrar.Parameter("max_chunk_list_size", &TThis::MaxChunkListSize)
        .GreaterThan(0)
        .Default(maxChunkListSize);
    registrar.Parameter("min_chunk_list_to_chunk_ratio", &TThis::MinChunkListToChunkRatio)
        .GreaterThan(0)
        .Default(minChunkListToChunkRatio);
}

void TStrictChunkTreeBalancerSettings::Register(TRegistrar registrar)
{
    RegisterParameters(registrar, 32, 1024, 2048, 0.01);
}

void TPermissiveChunkTreeBalancerSettings::Register(TRegistrar registrar)
{
    RegisterParameters(registrar, 64, 1024, 4096, 0.05);
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

void TDynamicChunkTreeBalancerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("strict", &TThis::StrictSettings)
        .DefaultCtor([] { return New<TStrictChunkTreeBalancerSettings>(); });

    registrar.Parameter("permissive", &TThis::PermissiveSettings)
        .DefaultCtor([] { return New<TPermissiveChunkTreeBalancerSettings>(); });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicAllyReplicaManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_ally_replica_announcement", &TThis::EnableAllyReplicaAnnouncement)
        .Default(false);

    registrar.Parameter("enable_endorsements", &TThis::EnableEndorsements)
        .Default(false);

    registrar.Parameter("underreplicated_chunk_announcement_request_delay", &TThis::UnderreplicatedChunkAnnouncementRequestDelay)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("safe_online_node_count", &TThis::SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default();

    registrar.Parameter("safe_lost_chunk_count", &TThis::SafeLostChunkCount)
        .GreaterThanOrEqual(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicChunkAutotomizerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("transaction_update_period", &TThis::TransactionUpdatePeriod)
        .Default(TDuration::Minutes(10));

    registrar.Parameter("refresh_period", &TThis::RefreshPeriod)
        .Default(TDuration::MilliSeconds(500));

    registrar.Parameter("chunk_unstage_period", &TThis::ChunkUnstagePeriod)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("tail_chunks_per_allocation", &TThis::TailChunksPerAllocation)
        .Default(2);

    registrar.Parameter("max_chunks_per_unstage", &TThis::MaxChunksPerUnstage)
        .Default(1000);

    registrar.Parameter("max_chunks_per_refresh", &TThis::MaxChunksPerRefresh)
        .Default(5000);
    registrar.Parameter("max_changed_chunks_per_refresh", &TThis::MaxChangedChunksPerRefresh)
        .Default(1000);

    registrar.Parameter("max_concurrent_jobs_per_chunk", &TThis::MaxConcurrentJobsPerChunk)
        .Default(3);

    registrar.Parameter("job_speculaltion_timeout", &TThis::JobSpeculationTimeout)
        .Default(TDuration::Seconds(3));

    registrar.Parameter("job_timeout", &TThis::JobTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("schedule_urgent_jobs", &TThis::ScheduleUrgentJobs)
        .Default(true);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicChunkManagerTestingConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("force_unreliable_seal", &TThis::ForceUnreliableSeal)
        .Default(false);
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicConsistentReplicaPlacementConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable", &TThis::Enable)
        .Default(false);

    registrar.Parameter("enable_pull_replication", &TThis::EnablePullReplication)
        .Default(false);

    registrar.Parameter("token_distribution_bucket_count", &TThis::TokenDistributionBucketCount)
        .Default(5)
        .GreaterThanOrEqual(1);

    registrar.Parameter("token_redistribution_period", &TThis::TokenRedistributionPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("tokens_per_node", &TThis::TokensPerNode)
        .Default(1)
        .GreaterThanOrEqual(1);

    registrar.Parameter("replicas_per_chunk", &TThis::ReplicasPerChunk)
        .Default(DefaultConsistentReplicaPlacementReplicasPerChunk)
        .GreaterThanOrEqual(std::max(
            ChunkReplicaIndexBound,
            NChunkClient::MaxReplicationFactor));
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicChunkManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_chunk_replicator", &TThis::EnableChunkReplicator)
        .Default(true);

    registrar.Parameter("enable_chunk_sealer", &TThis::EnableChunkSealer)
        .Default(true);

    registrar.Parameter("enable_chunk_autotomizer", &TThis::EnableChunkAutotomizer)
        .Default(false);

    registrar.Parameter("replica_approve_timeout", &TThis::ReplicaApproveTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("max_misscheduled_replication_jobs_per_heartbeat", &TThis::MaxMisscheduledReplicationJobsPerHeartbeat)
        .Default(128);
    registrar.Parameter("max_misscheduled_repair_jobs_per_heartbeat", &TThis::MaxMisscheduledRepairJobsPerHeartbeat)
        .Default(128);
    registrar.Parameter("max_misscheduled_removal_jobs_per_heartbeat", &TThis::MaxMisscheduledRemovalJobsPerHeartbeat)
        .Default(128);
    registrar.Parameter("max_misscheduled_seal_jobs_per_heartbeat", &TThis::MaxMisscheduledSealJobsPerHeartbeat)
        .Default(128);
    registrar.Parameter("max_misscheduled_merge_jobs_per_heartbeat", &TThis::MaxMisscheduledMergeJobsPerHeartbeat)
        .Default(128);

    registrar.Parameter("max_running_replication_jobs_per_target_node", &TThis::MaxRunningReplicationJobsPerTargetNode)
        .Default(128);

    registrar.Parameter("enable_chunk_refresh", &TThis::EnableChunkRefresh)
        .Default(true);
    registrar.Parameter("chunk_refresh_delay", &TThis::ChunkRefreshDelay)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("chunk_refresh_period", &TThis::ChunkRefreshPeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("max_blob_chunks_per_refresh", &TThis::MaxBlobChunksPerRefresh)
        .Default(8000)
        .Alias("max_chunks_per_refresh");
    registrar.Parameter("max_journal_chunks_per_refresh", &TThis::MaxJournalChunksPerRefresh)
        .Default(6000);

    registrar.Parameter("max_unsuccessfull_refresh_attempts", &TThis::MaxUnsuccessfullRefreshAttempts)
        .Default(10);

    registrar.Parameter("replicator_enabled_check_period", &TThis::ReplicatorEnabledCheckPeriod)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("enable_chunk_requisition_update", &TThis::EnableChunkRequisitionUpdate)
        .Default(true);
    registrar.Parameter("scheduled_chunk_requisition_updates_flush_period", &TThis::ScheduledChunkRequisitionUpdatesFlushPeriod)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("chunk_requisition_update_period", &TThis::ChunkRequisitionUpdatePeriod)
        .Default(TDuration::MilliSeconds(100));

    registrar.Parameter("max_chunks_per_requisition_update_scheduling", &TThis::MaxChunksPerRequisitionUpdateScheduling)
        .GreaterThan(0)
        .Default(14000);
    registrar.Parameter("max_blob_chunks_per_requisition_update", &TThis::MaxBlobChunksPerRequisitionUpdate)
        .Default(8000)
        .Alias("max_chunks_per_requisition_update");
    registrar.Parameter("max_time_per_blob_chunk_requisition_update", &TThis::MaxTimePerBlobChunkRequisitionUpdate)
        .Default(TDuration::MilliSeconds(80))
        .Alias("max_time_per_requisition_update");
    registrar.Parameter("max_journal_chunks_per_requisition_update", &TThis::MaxJournalChunksPerRequisitionUpdate)
        .Default(6000);
    registrar.Parameter("max_time_per_journal_chunk_requisition_update", &TThis::MaxTimePerJournalChunkRequisitionUpdate)
        .Default(TDuration::MilliSeconds(60));

    registrar.Parameter("finished_chunk_lists_requisition_traverse_flush_period", &TThis::FinishedChunkListsRequisitionTraverseFlushPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("chunk_seal_backoff_time", &TThis::ChunkSealBackoffTime)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("journal_rpc_timeout", &TThis::JournalRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("quorum_session_delay", &TThis::QuorumSessionDelay)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("max_chunks_per_seal", &TThis::MaxChunksPerSeal)
        .GreaterThan(0)
        .Default(10000);
    registrar.Parameter("max_concurrent_chunk_seals", &TThis::MaxConcurrentChunkSeals)
        .GreaterThan(0)
        .Default(10);

    registrar.Parameter("max_chunks_per_fetch", &TThis::MaxChunksPerFetch)
        .GreaterThan(0)
        .Default(1000000);

    registrar.Parameter("job_timeout", &TThis::JobTimeout)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("safe_online_node_count", &TThis::SafeOnlineNodeCount)
        .GreaterThanOrEqual(0)
        .Default(0);
    registrar.Parameter("safe_lost_chunk_fraction", &TThis::SafeLostChunkFraction)
        .InRange(0.0, 1.0)
        .Default(0.5);
    registrar.Parameter("safe_lost_chunk_count", &TThis::SafeLostChunkCount)
        .GreaterThan(0)
        .Default(1000);

    registrar.Parameter("repair_job_memory_usage", &TThis::RepairJobMemoryUsage)
        .Default(256_MB)
        .GreaterThanOrEqual(0);

    registrar.Parameter("job_throttler", &TThis::JobThrottler)
        .DefaultCtor([] {
            auto jobThrottler = New<NConcurrency::TThroughputThrottlerConfig>();
            jobThrottler->Limit = 10'000;

            return jobThrottler;
        });

    registrar.Parameter("per_type_job_throttlers", &TThis::JobTypeToThrottler)
        .DefaultCtor([] {
            THashMap<EJobType, NConcurrency::TThroughputThrottlerConfigPtr> jobTypeToThrottler;

            for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
                if (IsMasterJobType(jobType)) {
                    auto jobThrottler = EmplaceOrCrash(jobTypeToThrottler, jobType, New<NConcurrency::TThroughputThrottlerConfig>());
                    jobThrottler->second->Limit = 10'000;
                }
            }

            return jobTypeToThrottler;
        })
        .ResetOnLoad();

    registrar.Parameter("max_heavy_columns", &TThis::MaxHeavyColumns)
        .Default(30)
        .GreaterThanOrEqual(0);

    registrar.Parameter("deprecated_codec_ids", &TThis::DeprecatedCodecIds)
        .Default();

    registrar.Parameter("deprecated_codec_name_to_alias", &TThis::DeprecatedCodecNameToAlias)
        .Default();

    registrar.Parameter("max_oldest_part_missing_chunks", &TThis::MaxOldestPartMissingChunks)
        .Default(100);

    registrar.Parameter("chunk_removal_job_replicas_expiration_time", &TThis::ChunkRemovalJobReplicasExpirationTime)
        .Default(TDuration::Minutes(15));

    registrar.Parameter("data_node_tracker", &TThis::DataNodeTracker)
        .DefaultNew();

    registrar.Parameter("chunk_tree_balancer", &TThis::ChunkTreeBalancer)
        .DefaultNew();

    registrar.Parameter("chunk_merger", &TThis::ChunkMerger)
        .DefaultNew();

    registrar.Parameter("master_cell_chunk_statistics_collector", &TThis::MasterCellChunkStatisticsCollector)
        .DefaultNew();

    registrar.Parameter("chunk_reincarnator", &TThis::ChunkReincarnator)
        .DefaultNew();

    registrar.Parameter("ally_replica_manager", &TThis::AllyReplicaManager)
        .DefaultNew();

    registrar.Parameter("consistent_replica_placement", &TThis::ConsistentReplicaPlacement)
        .DefaultNew();

    registrar.Parameter("destroyed_replicas_profiling_period", &TThis::DestroyedReplicasProfilingPeriod)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("chunk_autotomizer", &TThis::ChunkAutotomizer)
        .DefaultNew();

    registrar.Parameter("finished_jobs_queue_size", &TThis::FinishedJobsQueueSize)
        .GreaterThanOrEqual(0)
        .Default(50'000);

    registrar.Parameter("abort_jobs_on_epoch_finish", &TThis::AbortJobsOnEpochFinish)
        .Default(true);

    registrar.Parameter("enable_per_node_incremental_heartbeat_profiling", &TThis::EnablePerNodeIncrementalHeartbeatProfiling)
        .Default(false);

    registrar.Parameter("testing", &TThis::Testing)
        .DefaultNew();

    registrar.Parameter("use_data_center_aware_replicator", &TThis::UseDataCenterAwareReplicator)
        .Default(false);

    registrar.Parameter("storage_data_centers", &TThis::StorageDataCenters)
        .Default();

    registrar.Parameter("banned_storage_data_centers", &TThis::BannedStorageDataCenters)
        .Default();

    registrar.Parameter("profiling_period", &TThis::ProfilingPeriod)
        .Default(DefaultProfilingPeriod);

    registrar.Parameter("sequoia_chunk_probability", &TThis::SequoiaChunkProbability)
        .Default(0)
        .InRange(0, 100);

    registrar.Parameter("sequoia_replica_removal_period", &TThis::SequoiaReplicaRemovalPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("sequoia_replica_removal_batch_size", &TThis::SequoiaReplicaRemovalBatchSize)
        .Default(1000);

    registrar.Parameter("sequoia_chunk_replicas_percentage", &TThis::SequoiaChunkReplicasPercentage)
        .Default(0)
        .InRange(0, 100);

    registrar.Parameter("fetch_replicas_from_sequoia", &TThis::FetchReplicasFromSequoia)
        .Default(false);

    registrar.Parameter("store_sequoia_replicas_on_master", &TThis::StoreSequoiaReplicasOnMaster)
        .Default(true);

    registrar.Parameter("processed_removed_sequoia_replicas_on_master", &TThis::ProcessRemovedSequoiaReplicasOnMaster)
        .Default(true);

    registrar.Parameter("removal_job_schedule_delay", &TThis::RemovalJobScheduleDelay)
        .Default(TDuration::Minutes(3))
        .DontSerializeDefault();

    registrar.Parameter("enable_fix_requisition_update_on_merge", &TThis::EnableFixRequisitionUpdateOnMerge)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("enable_more_chunk_confirmation_checks", &TThis::EnableMoreChunkConfirmationChecks)
        .Default(false)
        .DontSerializeDefault();

    // It should be set to |true| after 22.2 -> 22.3 update to keep compatibility with old clients.
    // Usage of |false| as default value allows 22.3 -> 22.3 rolling updates.
    registrar.Parameter("enable_chunk_confirmation_without_location_uuid", &TThis::EnableChunkConfirmationWithoutLocationUuid)
        .Default(false)
        .DontSerializeDefault();

    registrar.Parameter("enable_chunk_schemas", &TThis::EnableChunkSchemas)
        .Default(true);

    registrar.Postprocessor([] (TThis* config) {
        auto& jobTypeToThrottler = config->JobTypeToThrottler;
        for (auto jobType : TEnumTraits<EJobType>::GetDomainValues()) {
            if (IsMasterJobType(jobType) && !jobTypeToThrottler.contains(jobType)) {
                auto jobThrottler = EmplaceOrCrash(jobTypeToThrottler, jobType, New<NConcurrency::TThroughputThrottlerConfig>());
                jobThrottler->second->Limit = 10'000;
            }
        }

        if (config->StoreSequoiaReplicasOnMaster && !config->ProcessRemovedSequoiaReplicasOnMaster) {
            THROW_ERROR_EXCEPTION("Cannot disable removed Sequoia replicas processing on master while master still stores "
                "new sequoia replicas");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TDynamicChunkServiceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("enable_per_user_request_weight_throttling", &TThis::EnablePerUserRequestWeightThrottling)
        .Default(true);
    registrar.Parameter("enable_per_user_request_bytes_throttling", &TThis::EnablePerUserRequestBytesThrottling)
        .Default(false);

    registrar.Parameter("default_request_weight_throttler_config", &TThis::DefaultRequestWeightThrottlerConfig)
        .DefaultNew();

    registrar.Parameter("default_per_user_request_weight_throttler_config", &TThis::DefaultPerUserRequestWeightThrottlerConfig)
        .DefaultNew();
    registrar.Parameter("default_per_user_request_bytes_throttler_config", &TThis::DefaultPerUserRequestBytesThrottlerConfig)
        .DefaultNew();
}

////////////////////////////////////////////////////////////////////////////////

void TS3ConnectionConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("url", &TThis::Url)
        .Default();
    registrar.Parameter("region", &TThis::Region)
        .Default();
    registrar.Parameter("bucket", &TThis::Bucket)
        .Default();

    registrar.Parameter("access_key_id", &TThis::AccessKeyId)
        .Default();
    registrar.Parameter("secret_access_key", &TThis::SecretAccessKey)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TS3ClientConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
