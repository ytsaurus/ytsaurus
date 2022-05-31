#include "config.h"

#include <yt/yt/client/api/config.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

void TFileChangelogConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("data_flush_size", &TThis::DataFlushSize)
        .Alias("flush_buffer_size")
        .GreaterThanOrEqual(0)
        .Default(16_MB);
    registrar.Parameter("index_flush_size", &TThis::IndexFlushSize)
        .GreaterThanOrEqual(0)
        .Default(16_MB);
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("enable_sync", &TThis::EnableSync)
        .Default(true);
    registrar.Parameter("preallocate_size", &TThis::PreallocateSize)
        .GreaterThan(0)
        .Default();
    registrar.Parameter("recovery_buffer_size", &TThis::RecoveryBufferSize)
        .GreaterThan(0)
        .Default(16_MB);
}

////////////////////////////////////////////////////////////////////////////////

void TFileChangelogDispatcherConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("io_class", &TThis::IOClass)
        .Default(1); // IOPRIO_CLASS_RT
    registrar.Parameter("io_priority", &TThis::IOPriority)
        .Default(3);
    registrar.Parameter("flush_quantum", &TThis::FlushQuantum)
        .Default(TDuration::MilliSeconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TFileChangelogStoreConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("path", &TThis::Path);
    registrar.Parameter("changelog_reader_cache", &TThis::ChangelogReaderCache)
        .DefaultNew();

    registrar.Parameter("io_engine_type", &TThis::IOEngineType)
        .Default(NIO::EIOEngineType::ThreadPool);
    registrar.Parameter("io_engine", &TThis::IOConfig)
        .Optional();

    registrar.Preprocessor([] (TThis* config) {
        config->ChangelogReaderCache->Capacity = 4;
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

void THydraJanitorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_snapshot_count_to_keep", &TThis::MaxSnapshotCountToKeep)
        .GreaterThanOrEqual(0)
        .Default(10);
    registrar.Parameter("max_snapshot_size_to_keep", &TThis::MaxSnapshotSizeToKeep)
        .GreaterThanOrEqual(0)
        .Default();
    registrar.Parameter("max_changelog_count_to_keep", &TThis::MaxChangelogCountToKeep)
        .GreaterThanOrEqual(0)
        .Default();
    registrar.Parameter("max_changelog_size_to_keep", &TThis::MaxChangelogSizeToKeep)
        .GreaterThanOrEqual(0)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

void TLocalHydraJanitorConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("cleanup_period", &TThis::CleanupPeriod)
        .Default(TDuration::Seconds(10));
}

////////////////////////////////////////////////////////////////////////////////

void TDistributedHydraManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("control_rpc_timeout", &TThis::ControlRpcTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("max_commit_batch_duration", &TThis::MaxCommitBatchDuration)
        .Default(TDuration::MilliSeconds(100));
    registrar.Parameter("leader_lease_check_period", &TThis::LeaderLeaseCheckPeriod)
        .Default(TDuration::Seconds(2));
    registrar.Parameter("leader_lease_timeout", &TThis::LeaderLeaseTimeout)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("leader_lease_grace_delay", &TThis::LeaderLeaseGraceDelay)
        .Default(TDuration::Seconds(6));
    registrar.Parameter("disable_leader_lease_grace_delay", &TThis::DisableLeaderLeaseGraceDelay)
        .Default(false);

    registrar.Parameter("commit_flush_rpc_timeout", &TThis::CommitFlushRpcTimeout)
        .Default(TDuration::Seconds(15));
    registrar.Parameter("commit_forwarding_rpc_timeout", &TThis::CommitForwardingRpcTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("restart_backoff_time", &TThis::RestartBackoffTime)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("snapshot_build_timeout", &TThis::SnapshotBuildTimeout)
        .Default(TDuration::Minutes(5));
    registrar.Parameter("snapshot_fork_timeout", &TThis::SnapshotForkTimeout)
        .Default(TDuration::Minutes(2));
    registrar.Parameter("snapshot_build_period", &TThis::SnapshotBuildPeriod)
        .Default(TDuration::Minutes(60));
    registrar.Parameter("snapshot_build_splay", &TThis::SnapshotBuildSplay)
        .Default(TDuration::Minutes(5));

    registrar.Parameter("changelog_download_rpc_timeout", &TThis::ChangelogDownloadRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("max_changelog_records_per_request", &TThis::MaxChangelogRecordsPerRequest)
        .GreaterThan(0)
        .Default(64 * 1024);
    registrar.Parameter("max_changelog_bytes_per_request", &TThis::MaxChangelogBytesPerRequest)
        .GreaterThan(0)
        .Default(128_MB);

    registrar.Parameter("snapshot_download_rpc_timeout", &TThis::SnapshotDownloadRpcTimeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("snapshot_download_block_size", &TThis::SnapshotDownloadBlockSize)
        .GreaterThan(0)
        .Default(32_MB);

    registrar.Parameter("snapshot_download_total_streaming_timeout", &TThis::SnapshotDownloadTotalStreamingTimeout)
        .Default(TDuration::Minutes(30));
    registrar.Parameter("snapshot_download_streaming_stall_timeout", &TThis::SnapshotDownloadStreamingStallTimeout)
        .Default(TDuration::Seconds(30));
    registrar.Parameter("snapshot_download_window", &TThis::SnapshotDownloadWindowSize)
        .GreaterThan(0)
        .Default(32_MB);
    registrar.Parameter("snapshot_download_streaming_compression_codec", &TThis::SnapshotDownloadStreamingCompressionCodec)
        .Default(NCompression::ECodec::Lz4);

    registrar.Parameter("max_commit_batch_delay", &TThis::MaxCommitBatchDelay)
        .Default(TDuration::MilliSeconds(10));
    registrar.Parameter("max_commit_batch_record_count", &TThis::MaxCommitBatchRecordCount)
        .Default(10'000);

    registrar.Parameter("mutation_serialization_period", &TThis::MutationSerializationPeriod)
        .Default(TDuration::MilliSeconds(5));
    registrar.Parameter("mutation_flush_period", &TThis::MutationFlushPeriod)
        .Default(TDuration::MilliSeconds(5));

    registrar.Parameter("leader_sync_delay", &TThis::LeaderSyncDelay)
        .Default(TDuration::MilliSeconds(10));

    registrar.Parameter("max_changelog_record_count", &TThis::MaxChangelogRecordCount)
        .Default(1'000'000)
        .GreaterThan(0);
    registrar.Parameter("max_changelog_data_size", &TThis::MaxChangelogDataSize)
        .Default(1_GB)
        .GreaterThan(0);
    registrar.Parameter("preallocate_changelogs", &TThis::PreallocateChangelogs)
        .Default(false);
    registrar.Parameter("close_changelogs", &TThis::CloseChangelogs)
        .Default(true);

    registrar.Parameter("heartbeat_mutation_period", &TThis::HeartbeatMutationPeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("heartbeat_mutation_timeout", &TThis::HeartbeatMutationTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("changelog_record_count_check_retry_period", &TThis::ChangelogRecordCountCheckRetryPeriod)
        .Default(TDuration::Seconds(1));

    registrar.Parameter("mutation_logging_suspension_timeout", &TThis::MutationLoggingSuspensionTimeout)
        .Default(TDuration::Seconds(60));

    registrar.Parameter("build_snapshot_delay", &TThis::BuildSnapshotDelay)
        .Default(TDuration::Zero());

    registrar.Parameter("min_persistent_store_initialization_backoff_time", &TThis::MinPersistentStoreInitializationBackoffTime)
        .Default(TDuration::MilliSeconds(200));
    registrar.Parameter("max_persistent_store_initialization_backoff_time", &TThis::MaxPersistentStoreInitializationBackoffTime)
        .Default(TDuration::Seconds(5));
    registrar.Parameter("persistent_store_initialization_backoff_time_multiplier", &TThis::PersistentStoreInitializationBackoffTimeMultiplier)
        .Default(1.5);

    registrar.Parameter("abandon_leader_lease_request_timeout", &TThis::AbandonLeaderLeaseRequestTimeout)
        .Default(TDuration::Seconds(5));

    registrar.Parameter("force_mutation_logging", &TThis::ForceMutationLogging)
        .Default(false);

    registrar.Parameter("enable_state_hash_checker", &TThis::EnableStateHashChecker)
        .Default(true);

    registrar.Parameter("max_state_hash_checker_entry_count", &TThis::MaxStateHashCheckerEntryCount)
        .GreaterThan(0)
        .Default(1000);

    registrar.Parameter("state_hash_checker_mutation_verification_sampling_rate", &TThis::StateHashCheckerMutationVerificationSamplingRate)
        .GreaterThan(0)
        .Default(10);

    registrar.Parameter("max_queued_mutation_count", &TThis::MaxQueuedMutationCount)
        .GreaterThan(0)
        .Default(100'000);

    registrar.Parameter("max_queued_mutation_data_size", &TThis::MaxQueuedMutationDataSize)
        .GreaterThan(0)
        .Default(2_GB);

    registrar.Parameter("leader_switch_timeout", &TThis::LeaderSwitchTimeout)
        .Default(TDuration::Seconds(30));

    registrar.Parameter("invariants_check_probability", &TThis::InvariantsCheckProbability)
        .Default();

    registrar.Parameter("max_in_flight_accept_mutations_request_count", &TThis::MaxInFlightAcceptMutationsRequestCount)
        .GreaterThan(0)
        .Default(10);

    registrar.Parameter("max_in_flight_mutations_count", &TThis::MaxInFlightMutationCount)
        .GreaterThan(0)
        .Default(100000);

    registrar.Parameter("max_in_flight_mutation_data_size", &TThis::MaxInFlightMutationDataSize)
        .GreaterThan(0)
        .Default(2_GB);

    registrar.Parameter("max_changelogs_for_recovery", &TThis::MaxChangelogsForRecovery)
        .GreaterThan(0)
        .Default(20);

    registrar.Parameter("max_changelog_mutation_count_for_recovery", &TThis::MaxChangelogMutationCountForRecovery)
        .GreaterThan(0)
        .Default(20'000'000);

    registrar.Parameter("max_total_changelog_size_for_recovery", &TThis::MaxTotalChangelogSizeForRecovery)
        .GreaterThan(0)
        .Default(20_GB);

    registrar.Parameter("checkpoint_check_period", &TThis::CheckpointCheckPeriod)
        .Default(TDuration::Seconds(15));

    registrar.Parameter("max_changelogs_to_create_during_acquisition", &TThis::MaxChangelogsToCreateDuringAcquisition)
        .Default(10);

    registrar.Parameter("alert_on_snapshot_failure", &TThis::AlertOnSnapshotFailure)
        .Default(true);

    registrar.Postprocessor([] (TThis* config) {
        if (!config->DisableLeaderLeaseGraceDelay && config->LeaderLeaseGraceDelay <= config->LeaderLeaseTimeout) {
            THROW_ERROR_EXCEPTION("\"leader_lease_grace_delay\" must be larger than \"leader_lease_timeout\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

TSerializationDumperConfig::TSerializationDumperConfig()
{
    RegisterParameter("lower_limit", LowerLimit)
        .GreaterThanOrEqual(0)
        .Default(0);
    RegisterParameter("upper_limit", UpperLimit)
        .GreaterThanOrEqual(0)
        .Default(std::numeric_limits<i64>::max());

    RegisterPostprocessor([&] () {
        if (LowerLimit >= UpperLimit) {
            THROW_ERROR_EXCEPTION("\"upper_limit\" must be greater than \"lower_limit\"")
                << TErrorAttribute("lower_limit", LowerLimit)
                << TErrorAttribute("upper_limit", UpperLimit);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
