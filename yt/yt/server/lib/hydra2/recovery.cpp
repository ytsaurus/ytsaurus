#include "recovery.h"
#include "changelog_discovery.h"
#include "changelog_download.h"
#include "config.h"
#include "decorated_automaton.h"
#include "hydra_service_proxy.h"
#include "snapshot_discovery.h"
#include "snapshot_download.h"
#include "helpers.h"

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/response_keeper.h>

namespace NYT::NHydra2 {

using namespace NRpc;
using namespace NElection;
using namespace NConcurrency;
using namespace NHydra;
using namespace NHydra2::NProto;

////////////////////////////////////////////////////////////////////////////////

TRecovery::TRecovery(
    TDistributedHydraManagerConfigPtr config,
    const TDistributedHydraManagerOptions& options,
    const TDistributedHydraManagerDynamicOptions& dynamicOptions,
    TDecoratedAutomatonPtr decoratedAutomaton,
    IChangelogStorePtr changelogStore,
    ISnapshotStorePtr snapshotStore,
    IResponseKeeperPtr responseKeeper,
    TEpochContext* epochContext,
    TReachableState targetState,
    bool isLeader,
    NLogging::TLogger logger)
    : Config_(std::move(config))
    , Options_(options)
    , DynamicOptions_(dynamicOptions)
    , DecoratedAutomaton_(std::move(decoratedAutomaton))
    , ChangelogStore_(std::move(changelogStore))
    , SnapshotStore_(std::move(snapshotStore))
    , ResponseKeeper_(std::move(responseKeeper))
    , EpochContext_(epochContext)
    , TargetState_(targetState)
    , IsLeader_(isLeader)
    , Logger(std::move(logger))
{
    YT_VERIFY(Config_);
    YT_VERIFY(DecoratedAutomaton_);
    YT_VERIFY(ChangelogStore_);
    YT_VERIFY(SnapshotStore_);
    YT_VERIFY(EpochContext_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochSystemAutomatonInvoker, AutomatonThread);
}

void TRecovery::DoRun()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto epochContext = EpochContext_;

    auto currentState = DecoratedAutomaton_->GetReachableState();
    if (currentState > TargetState_) {
        // NB: YT-14934, rollback is possible at followers but not at leaders.
        if (IsLeader_) {
            YT_LOG_FATAL("Current automaton state is greater than target state during leader recovery "
                "(CurrentState: %v, TargetState: %v)",
                currentState,
                TargetState_);
        } else {
            DecoratedAutomaton_->ResetState();
            THROW_ERROR_EXCEPTION("Error recovering to state %v: current automaton state %v is greater",
                TargetState_,
                currentState);
        }
    }

    int snapshotId = InvalidSegmentId;
    if (TargetState_.SegmentId > currentState.SegmentId) {
        auto snapshotParamsOrError = WaitFor(DiscoverLatestSnapshot(Config_, epochContext->CellManager));
        THROW_ERROR_EXCEPTION_IF_FAILED(snapshotParamsOrError, "Error computing the latest snapshot id");
        auto localLatestSnapshotId = WaitFor(SnapshotStore_->GetLatestSnapshotId())
            .ValueOrThrow();
        snapshotId = std::max(snapshotParamsOrError.Value().SnapshotId, localLatestSnapshotId);
    }

    YT_LOG_INFO("Running recovery (CurrentState: %v, TargetState: %v)",
        currentState,
        TargetState_);

    int initialChangelogId;
    if (snapshotId > currentState.SegmentId) {
        // Load the snapshot.
        YT_LOG_INFO("Checking if snapshot is needed for recovery (CurrentState: %v, SnapshotId: %v)",
            currentState,
            snapshotId);

        YT_VERIFY(snapshotId != InvalidSegmentId);

        auto createAndOpenReaderOrThrow = [&] {
            auto reader = SnapshotStore_->CreateReader(snapshotId);
            WaitFor(reader->Open())
                .ThrowOnError();
            return reader;
        };

        ISnapshotReaderPtr snapshotReader;
        try {
            snapshotReader = createAndOpenReaderOrThrow();
        } catch (const TErrorException& ex) {
            if (ex.Error().FindMatching(EErrorCode::NoSuchSnapshot)) {
                YT_LOG_INFO(ex, "Snapshot is missing, attempting to download (SnapshotId: %v)",
                    snapshotId);
                WaitFor(DownloadSnapshot(
                    Config_,
                    Options_,
                    epochContext->CellManager,
                    SnapshotStore_,
                    snapshotId,
                    Logger))
                    .ThrowOnError();
                snapshotReader = createAndOpenReaderOrThrow();
            } else {
                YT_LOG_INFO(ex, "Failed to open snapshot (SnapshotId: %v)", snapshotId);
                throw;
            }
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Failed to open snapshot (SnapshotId: %v)", snapshotId);
            throw;
        }

        auto snapshotMeta = snapshotReader->GetParams().Meta;
        auto snapshoRandomSeed = snapshotMeta.random_seed();
        auto snapshotSequenceNumber = snapshotMeta.sequence_number();
        auto snapshotStateHash = snapshotMeta.state_hash();
        auto snapshotTimestamp = FromProto<TInstant>(snapshotMeta.timestamp());
        // COMPAT(aleksandra-zh)
        auto snapshotSegmentId = snapshotMeta.last_segment_id();
        if (!snapshotSegmentId) {
            snapshotSegmentId = snapshotId;
        }
        auto snapshotRecordId = snapshotMeta.last_record_id();
        auto snapshotLastMutationTerm = snapshotMeta.last_mutation_term();
        auto snapshotReadOnly = snapshotMeta.read_only();

        YT_VERIFY(snapshotSegmentId >= currentState.SegmentId);
        YT_VERIFY(snapshotSequenceNumber >= currentState.SequenceNumber);

        YT_LOG_INFO("Snapshot opened (SnapshotSegmentId: %v, SnapshotSequenceNumber: %v, ReadOnly: %v)",
            snapshotSegmentId,
            snapshotSequenceNumber,
            snapshotReadOnly);

        if (snapshotSegmentId == currentState.SegmentId && snapshotSequenceNumber == currentState.SequenceNumber) {
            YT_LOG_INFO("No need to use snapshot for recovery");
        } else {
            YT_LOG_INFO("Using snapshot for recovery");

            if (ResponseKeeper_) {
                ResponseKeeper_->Stop();
            }

            auto future = BIND(&TDecoratedAutomaton::LoadSnapshot, DecoratedAutomaton_)
                .AsyncVia(epochContext->EpochSystemAutomatonInvoker)
                .Run(snapshotId,
                    snapshotLastMutationTerm,
                    {snapshotSegmentId, snapshotRecordId},
                    snapshotSequenceNumber,
                    snapshotReadOnly,
                    snapshoRandomSeed,
                    snapshotStateHash,
                    snapshotTimestamp,
                    snapshotReader);
            WaitFor(future)
                .ThrowOnError();
        }

        initialChangelogId = snapshotId;
    } else {
        // Recover using changelogs only.
        YT_LOG_INFO("Not using snapshots for recovery (CurrentState: %v, SnapshotId: %v)",
            currentState,
            snapshotId);
        initialChangelogId = currentState.SegmentId;
    }

    auto isPersistenceEnabled = IsPersistenceEnabled(EpochContext_->CellManager, Options_);

    // Shortcut for observer startup.
    // XXX(babenko)
    if (TargetState_ == TReachableState() && !isPersistenceEnabled) {
        return;
    }

    YT_LOG_INFO("Replaying changelogs (TargetState: %v, ChangelogIds: %v-%v)",
        TargetState_,
        initialChangelogId,
        TargetState_.SegmentId);

    auto changelogId = initialChangelogId;
    IChangelogPtr changelog;
    while (changelogId <= TargetState_.SegmentId) {
        YT_LOG_INFO("Opening changelog (ChangelogId: %v)",
            changelogId);

        changelog = WaitFor(ChangelogStore_->TryOpenChangelog(changelogId))
            .ValueOrThrow();

        if (changelog) {
            YT_LOG_INFO("Changelog opened (ChangelogId: %v)",
                changelogId);
        } else {
            if (!isPersistenceEnabled) {
                THROW_ERROR_EXCEPTION("Changelog %v is missing", changelogId);
            }

            // Leader can miss a changelog if we are recovering to snapshot version.

            YT_LOG_INFO("Changelog is missing and will be created (ChangelogId: %v)",
                changelogId);

            changelog = WaitFor(ChangelogStore_->CreateChangelog(changelogId, /*meta*/ {}))
                .ValueOrThrow();
        }

        if (!IsLeader_ && isPersistenceEnabled) {
            SyncChangelog(changelog);
        }

        ReplayChangelog(changelog, TargetState_.SequenceNumber);

        ++changelogId;
        WaitFor(changelog->Close())
            .ThrowOnError();
    }

    auto automatonState = DecoratedAutomaton_->GetReachableState();
    if (automatonState != TargetState_) {
        THROW_ERROR_EXCEPTION("Unable to recover to version (AutomatonState: %v, TargetState: %v)",
            automatonState,
            TargetState_);
    }
}

void TRecovery::SyncChangelog(const IChangelogPtr& changelog)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto channel = EpochContext_->CellManager->GetPeerChannel(EpochContext_->LeaderId);
    YT_VERIFY(channel);

    TInternalHydraServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

    auto req = proxy.LookupChangelog();
    req->set_changelog_id(changelog->GetId());

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting changelog %v info from leader",
        changelog->GetId());
    const auto& rsp = rspOrError.Value();

    i64 remoteRecordCount = rsp->record_count();
    auto firstRemoteSequenceNumber = rsp->has_first_sequence_number()
        ? std::make_optional(rsp->first_sequence_number())
        : std::nullopt;
    i64 localRecordCount = changelog->GetRecordCount();
    i64 adjustedRemoteRecordCount = remoteRecordCount;

    if (firstRemoteSequenceNumber) {
        i64 recordCountLimit = TargetState_.SequenceNumber - *firstRemoteSequenceNumber + 1;
        adjustedRemoteRecordCount = std::min<i64>(remoteRecordCount, recordCountLimit);
    }

    YT_LOG_INFO("Synchronizing changelog (ChangelogId: %v, LocalRecordCount: %v, RemoteRecordCount: %v, "
        "FirstRemoteSequenceNumber: %v, AdjustedRemoteRecordCount: %v)",
        changelog->GetId(),
        localRecordCount,
        remoteRecordCount,
        firstRemoteSequenceNumber,
        adjustedRemoteRecordCount);

    if (localRecordCount > adjustedRemoteRecordCount) {
        int latestChangelogId = ChangelogStore_->GetLatestChangelogIdOrThrow();

        if (firstRemoteSequenceNumber) {
            auto lastRemoteSequenceNumber = *firstRemoteSequenceNumber + adjustedRemoteRecordCount;
            auto automatonSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
            if (lastRemoteSequenceNumber < automatonSequenceNumber) {
                auto reliablyAppliedSequenceNumber = DecoratedAutomaton_->GetReliablyAppliedSequenceNumber();
                YT_LOG_FATAL_IF(reliablyAppliedSequenceNumber > lastRemoteSequenceNumber,
                    "Trying to truncate a mutation that was reliably applied (ReliablyAppliedSequenceNumber, LastRemoteSequenceNumber: %v)",
                    reliablyAppliedSequenceNumber,
                    lastRemoteSequenceNumber);
                YT_LOG_INFO("Truncating a mutation that was already applied (LastRemoteSequenceNumber: %v, AutomatonSequenceNumber: %v, ReliablyAppliedSequenceNumber: %v)",
                    lastRemoteSequenceNumber,
                    automatonSequenceNumber,
                    reliablyAppliedSequenceNumber);
                DecoratedAutomaton_->ResetState();
                THROW_ERROR_EXCEPTION("Truncating a mutation that was already applied");
            }
        }

        for (int changelogIdToTruncate = latestChangelogId; changelogIdToTruncate > changelog->GetId(); --changelogIdToTruncate) {
            auto errorOrChangelogToTruncate = WaitFor(ChangelogStore_->TryOpenChangelog(changelogIdToTruncate));
            if (!errorOrChangelogToTruncate.IsOK()) {
                YT_LOG_INFO("Changelog does not exist, skipping (ChangelogId: %v)",
                    changelogIdToTruncate);
                continue;
            }
            auto changelogToTruncate = errorOrChangelogToTruncate.ValueOrThrow();
            if (changelogToTruncate->GetRecordCount() > 0) {
                YT_LOG_INFO("Removing all records from changelog (ChangelogId: %v, RecordCount: %v)",
                    changelogIdToTruncate,
                    changelogToTruncate->GetRecordCount());
                auto result = WaitFor(changelogToTruncate->Truncate(0));
                THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error truncating changelog");
            } else {
                YT_LOG_INFO("Changelog is empty, will not truncate (ChangelogId: %v)",
                    changelogIdToTruncate);
            }
            WaitFor(changelogToTruncate->Close())
                .ThrowOnError();
        }

        auto result = WaitFor(changelog->Truncate(adjustedRemoteRecordCount));
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error truncating changelog");
    } else if (localRecordCount < adjustedRemoteRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config_,
            EpochContext_->CellManager,
            changelog,
            adjustedRemoteRecordCount,
            Logger);
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

void TRecovery::ReplayChangelog(const IChangelogPtr& changelog, i64 targetSequenceNumber)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto changelogRecordCount = changelog->GetRecordCount();
    YT_LOG_INFO("Replaying changelog (ChangelogId: %v, RecordCount: %v, TargetSequenceNumber: %v)",
        changelog->GetId(),
        changelogRecordCount,
        targetSequenceNumber);

    int currentRecordId = 0;
    auto automatonVersion = DecoratedAutomaton_->GetAutomatonVersion();
    if (automatonVersion.SegmentId == changelog->GetId()) {
        currentRecordId = automatonVersion.RecordId;
    }

    while (currentRecordId < changelogRecordCount && DecoratedAutomaton_->GetSequenceNumber() < targetSequenceNumber) {
        auto automatonNumber = DecoratedAutomaton_->GetSequenceNumber();
        auto recordsNeeded = targetSequenceNumber - automatonNumber;
        YT_VERIFY(recordsNeeded > 0);

        YT_LOG_INFO("Started reading changelog records (ChangelogId: %v, RecordIds: %v-%v)",
            changelog->GetId(),
            currentRecordId,
            currentRecordId + recordsNeeded - 1);

        auto asyncRecordsData = changelog->Read(
            currentRecordId,
            recordsNeeded,
            Config_->MaxChangelogBytesPerRequest);
        auto recordsData = WaitFor(asyncRecordsData)
            .ValueOrThrow();
        auto recordsRead = std::ssize(recordsData);

        YT_LOG_INFO("Finished reading changelog records (ChangelogId: %v, RecordIds: %v-%v)",
            changelog->GetId(),
            currentRecordId,
            currentRecordId + recordsRead - 1);

        YT_LOG_INFO("Applying changelog records (ChangelogId: %v, RecordIds: %v-%v)",
            changelog->GetId(),
            currentRecordId,
            currentRecordId + recordsRead - 1);

        auto future = BIND([=, this, this_ = MakeStrong(this), recordsData = std::move(recordsData)] {
                DecoratedAutomaton_->ApplyMutationsDuringRecovery(recordsData);
            })
            .AsyncVia(EpochContext_->EpochSystemAutomatonInvoker)
            .Run();

        WaitFor(future)
            .ThrowOnError();

        currentRecordId += recordsRead;
    }

    auto automatonSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
    YT_LOG_INFO("Changelog replayed (AutomatonSequenceNumber: %v, TargetSequenceNumber: %v)",
        automatonSequenceNumber,
        targetSequenceNumber);
}

TFuture<void> TRecovery::Run()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TRecovery::DoRun, MakeStrong(this))
        .AsyncVia(EpochContext_->EpochControlInvoker)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
