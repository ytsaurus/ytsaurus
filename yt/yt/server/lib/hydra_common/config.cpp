#include "config.h"

#include <yt/yt/client/api/config.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFileChangelogConfig::TFileChangelogConfig()
{
    RegisterParameter("index_block_size", IndexBlockSize)
        .GreaterThan(0)
        .Default(1_MB);
    RegisterParameter("flush_buffer_size", FlushBufferSize)
        .GreaterThanOrEqual(0)
        .Default(16_MB);
    RegisterParameter("flush_period", FlushPeriod)
        .Default(TDuration::MilliSeconds(10));
    RegisterParameter("enable_sync", EnableSync)
        .Default(true);
    RegisterParameter("preallocate_size", PreallocateSize)
        .GreaterThan(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TFileChangelogDispatcherConfig::TFileChangelogDispatcherConfig()
{
    RegisterParameter("io_class", IOClass)
        .Default(1); // IOPRIO_CLASS_RT
    RegisterParameter("io_priority", IOPriority)
        .Default(3);
    RegisterParameter("flush_quantum", FlushQuantum)
        .Default(TDuration::MilliSeconds(10));
}

////////////////////////////////////////////////////////////////////////////////

TFileChangelogStoreConfig::TFileChangelogStoreConfig()
{
    RegisterParameter("path", Path);
    RegisterParameter("changelog_reader_cache", ChangelogReaderCache)
        .DefaultNew();

    RegisterParameter("io_engine_type", IOEngineType)
        .Default(NIO::EIOEngineType::ThreadPool);
    RegisterParameter("io_engine", IOConfig)
        .Optional();

    RegisterPreprocessor([&] () {
        ChangelogReaderCache->Capacity = 4;
    });
}

////////////////////////////////////////////////////////////////////////////////

TLocalSnapshotStoreConfig::TLocalSnapshotStoreConfig()
{
    RegisterParameter("path", Path);
    RegisterParameter("codec", Codec)
        .Default(NCompression::ECodec::Lz4);
}

////////////////////////////////////////////////////////////////////////////////

TRemoteSnapshotStoreConfig::TRemoteSnapshotStoreConfig()
{
    RegisterParameter("reader", Reader)
        .DefaultNew();
    RegisterParameter("writer", Writer)
        .DefaultNew();

    RegisterPreprocessor([&] {
        Reader->WorkloadDescriptor.Category = EWorkloadCategory::SystemTabletRecovery;
        Writer->WorkloadDescriptor.Category = EWorkloadCategory::SystemTabletSnapshot;

        //! We want to evenly distribute snapshot load across the cluster.
        Writer->PreferLocalHost = false;
    });
}

////////////////////////////////////////////////////////////////////////////////

TRemoteChangelogStoreConfig::TRemoteChangelogStoreConfig()
{
    RegisterParameter("reader", Reader)
        .DefaultNew();
    RegisterParameter("writer", Writer)
        .DefaultNew();
    RegisterParameter("lock_transaction_timeout", LockTransactionTimeout)
        .Default();

    RegisterPreprocessor([&] {
        Reader->WorkloadDescriptor.Category = EWorkloadCategory::SystemTabletRecovery;

        Writer->WorkloadDescriptor.Category = EWorkloadCategory::SystemTabletLogging;
        Writer->MaxChunkRowCount = 1'000'000'000;
        Writer->MaxChunkDataSize = 1_TB;
        Writer->MaxChunkSessionDuration = TDuration::Hours(24);
    });
}

////////////////////////////////////////////////////////////////////////////////

THydraJanitorConfig::THydraJanitorConfig()
{
    RegisterParameter("max_snapshot_count_to_keep", MaxSnapshotCountToKeep)
        .GreaterThanOrEqual(0)
        .Default(10);
    RegisterParameter("max_snapshot_size_to_keep", MaxSnapshotSizeToKeep)
        .GreaterThanOrEqual(0)
        .Default();
    RegisterParameter("max_changelog_count_to_keep", MaxChangelogCountToKeep)
        .GreaterThanOrEqual(0)
        .Default();
    RegisterParameter("max_changelog_size_to_keep", MaxChangelogSizeToKeep)
        .GreaterThanOrEqual(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TLocalHydraJanitorConfig::TLocalHydraJanitorConfig()
{
    RegisterParameter("cleanup_period", CleanupPeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

TDistributedHydraManagerConfig::TDistributedHydraManagerConfig()
{
    RegisterParameter("control_rpc_timeout", ControlRpcTimeout)
        .Default(TDuration::Seconds(5));

    RegisterParameter("max_commit_batch_duration", MaxCommitBatchDuration)
        .Default(TDuration::MilliSeconds(100));
    RegisterParameter("leader_lease_check_period", LeaderLeaseCheckPeriod)
        .Default(TDuration::Seconds(2));
    RegisterParameter("leader_lease_timeout", LeaderLeaseTimeout)
        .Default(TDuration::Seconds(5));
    RegisterParameter("leader_lease_grace_delay", LeaderLeaseGraceDelay)
        .Default(TDuration::Seconds(6));
    RegisterParameter("disable_leader_lease_grace_delay", DisableLeaderLeaseGraceDelay)
        .Default(false);

    RegisterParameter("commit_flush_rpc_timeout", CommitFlushRpcTimeout)
        .Default(TDuration::Seconds(15));
    RegisterParameter("commit_forwarding_rpc_timeout", CommitForwardingRpcTimeout)
        .Default(TDuration::Seconds(30));

    RegisterParameter("restart_backoff_time", RestartBackoffTime)
        .Default(TDuration::Seconds(5));

    RegisterParameter("snapshot_build_timeout", SnapshotBuildTimeout)
        .Default(TDuration::Minutes(5));
    RegisterParameter("snapshot_fork_timeout", SnapshotForkTimeout)
        .Default(TDuration::Minutes(2));
    RegisterParameter("snapshot_build_period", SnapshotBuildPeriod)
        .Default(TDuration::Minutes(60));
    RegisterParameter("snapshot_build_splay", SnapshotBuildSplay)
        .Default(TDuration::Minutes(5));

    RegisterParameter("changelog_download_rpc_timeout", ChangelogDownloadRpcTimeout)
        .Default(TDuration::Seconds(10));
    RegisterParameter("max_changelog_records_per_request", MaxChangelogRecordsPerRequest)
        .GreaterThan(0)
        .Default(64 * 1024);
    RegisterParameter("max_changelog_bytes_per_request", MaxChangelogBytesPerRequest)
        .GreaterThan(0)
        .Default(128_MB);

    RegisterParameter("snapshot_download_rpc_timeout", SnapshotDownloadRpcTimeout)
        .Default(TDuration::Seconds(10));
    RegisterParameter("snapshot_download_block_size", SnapshotDownloadBlockSize)
        .GreaterThan(0)
        .Default(32_MB);

    RegisterParameter("max_commit_batch_delay", MaxCommitBatchDelay)
        .Default(TDuration::MilliSeconds(10));
    RegisterParameter("max_commit_batch_record_count", MaxCommitBatchRecordCount)
        .Default(10'000);
    RegisterParameter("max_logged_mutations_per_request", MaxLoggedMutationsPerRequest)
        .Default(10'000);

    RegisterParameter("leader_sync_delay", LeaderSyncDelay)
        .Default(TDuration::MilliSeconds(10));

    RegisterParameter("max_changelog_record_count", MaxChangelogRecordCount)
        .Default(1'000'000)
        .GreaterThan(0);
    RegisterParameter("max_changelog_data_size", MaxChangelogDataSize)
        .Default(1_GB)
        .GreaterThan(0);
    RegisterParameter("preallocate_changelogs", PreallocateChangelogs)
        .Default(false);
    RegisterParameter("close_changelogs", CloseChangelogs)
        .Default(true);

    RegisterParameter("heartbeat_mutation_period", HeartbeatMutationPeriod)
        .Default(TDuration::Seconds(60));
    RegisterParameter("heartbeat_mutation_timeout", HeartbeatMutationTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("changelog_record_count_check_retry_period", ChangelogRecordCountCheckRetryPeriod)
        .Default(TDuration::Seconds(1));

    RegisterParameter("mutation_logging_suspension_timeout", MutationLoggingSuspensionTimeout)
        .Default(TDuration::Seconds(60));

    RegisterParameter("build_snapshot_delay", BuildSnapshotDelay)
        .Default(TDuration::Zero());

    RegisterParameter("min_persistent_store_initialization_backoff_time", MinPersistentStoreInitializationBackoffTime)
        .Default(TDuration::MilliSeconds(200));
    RegisterParameter("max_persistent_store_initialization_backoff_time", MaxPersistentStoreInitializationBackoffTime)
        .Default(TDuration::Seconds(5));
    RegisterParameter("persistent_store_initialization_backoff_time_multiplier", PersistentStoreInitializationBackoffTimeMultiplier)
        .Default(1.5);

    RegisterParameter("abandon_leader_lease_request_timeout", AbandonLeaderLeaseRequestTimeout)
        .Default(TDuration::Seconds(5));

    RegisterParameter("force_mutation_logging", ForceMutationLogging)
        .Default(false);

    RegisterParameter("enable_state_hash_checker", EnableStateHashChecker)
        .Default(true);

    RegisterParameter("max_state_hash_checker_entry_count", MaxStateHashCheckerEntryCount)
        .GreaterThan(0)
        .Default(1000);

    RegisterParameter("state_hash_checker_mutation_verification_sampling_rate", StateHashCheckerMutationVerificationSamplingRate)
        .GreaterThan(0)
        .Default(10);

    RegisterParameter("max_queue_mutation_count", MaxQueueMutationCount)
        .GreaterThan(0)
        .Default(10'000);

    RegisterParameter("max_queue_mutation_data_size", MaxQueueMutationDataSize)
        .GreaterThan(0)
        .Default(1_GB);

    RegisterParameter("leader_switch_timeout", LeaderSwitchTimeout)
        .Default(TDuration::Seconds(30));

    RegisterPostprocessor([&] () {
        if (!DisableLeaderLeaseGraceDelay && LeaderLeaseGraceDelay <= LeaderLeaseTimeout) {
            THROW_ERROR_EXCEPTION("\"leader_lease_grace_delay\" must be larger than \"leader_lease_timeout\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
