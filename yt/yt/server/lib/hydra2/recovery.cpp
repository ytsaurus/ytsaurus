#include "recovery.h"
#include "changelog_download.h"
#include "config.h"
#include "decorated_automaton.h"
#include "changelog_discovery.h"
#include "hydra_service_proxy.h"

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

#include <yt/yt/core/misc/variant.h>

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
    TResponseKeeperPtr responseKeeper,
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

void TRecovery::Recover(int term)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto epochContext = EpochContext_;
    // >?
    if (epochContext->Term != term) {
        THROW_ERROR_EXCEPTION("Cannot recover to term %v in term %v",
            epochContext->Term,
            term);
    }

    auto currentState = DecoratedAutomaton_->GetReachableState();
    if (currentState > TargetState_) {
        // NB: YT-14934, rollback is possible at followers but not at leaders.
        if (IsLeader_) {
            YT_LOG_FATAL("Current automaton state is greater than target state during leader recovery "
                "(CurrentState: %v, TargetState: %v)",
                currentState,
                TargetState_);
        } else {
            THROW_ERROR_EXCEPTION("Error recovering to state %v: current automaton state %v is greater",
                TargetState_,
                currentState);
        }
    }

    int snapshotId = InvalidSegmentId;
    if (TargetState_.SegmentId > currentState.SegmentId) {
        auto snapshotIdOrError = WaitFor(SnapshotStore_->GetLatestSnapshotId());
        THROW_ERROR_EXCEPTION_IF_FAILED(snapshotIdOrError, "Error computing the latest snapshot id");
        snapshotId = snapshotIdOrError.Value();
    }

     YT_LOG_INFO("Recovering from %v to %v",
        currentState,
        TargetState_);

    int initialChangelogId;
    if (snapshotId > currentState.SegmentId) {
        // Load the snapshot.
        YT_LOG_INFO("Trying to use snapshot for recovery (SnapshotId: %v, CurrentState: %v)",
            snapshotId,
            currentState);

        YT_VERIFY(snapshotId != InvalidSegmentId);
        auto snapshotReader = SnapshotStore_->CreateReader(snapshotId);

        WaitFor(snapshotReader->Open())
            .ThrowOnError();

        auto meta = snapshotReader->GetParams().Meta;
        auto randomSeed = meta.random_seed();
        auto sequenceNumber = meta.sequence_number();
        auto stateHash = meta.state_hash();
        auto timestamp = FromProto<TInstant>(meta.timestamp());

        auto snapshotSegmentId = meta.has_last_segment_id() ? meta.last_segment_id() : snapshotId;
        auto snapshotRecordId = meta.has_last_record_id() ? meta.last_record_id() : 0;
        auto lastMutationTerm = meta.has_last_mutation_term() ? meta.last_mutation_term() : 0;

        YT_VERIFY(snapshotSegmentId >= currentState.SegmentId);
        YT_VERIFY(sequenceNumber >= currentState.SequenceNumber);

        YT_LOG_INFO("Snapshot state (SegmentId: %v, SequenceNumber: %v)",
            snapshotSegmentId,
            sequenceNumber);

        if (snapshotSegmentId == currentState.SegmentId && sequenceNumber == currentState.SequenceNumber) {
            YT_LOG_INFO("No need to use snapshot for recovery");
        } else {
            YT_LOG_INFO("Using snapshot for recovery");
            if (ResponseKeeper_) {
                ResponseKeeper_->Stop();
            }

            auto future = BIND(&TDecoratedAutomaton::LoadSnapshot, DecoratedAutomaton_)
                .AsyncVia(epochContext->EpochSystemAutomatonInvoker)
                .Run(snapshotId,
                    lastMutationTerm,
                    {snapshotSegmentId, snapshotRecordId},
                    sequenceNumber,
                    randomSeed,
                    stateHash,
                    timestamp,
                    snapshotReader);
            WaitFor(future)
                .ThrowOnError();
        }

        initialChangelogId = snapshotId;
    } else {
        // Recover using changelogs only.
        YT_LOG_INFO("Not using snapshots for recovery (SnapshotId: %v, CurrentState: %v)",
            snapshotId,
            currentState);
        initialChangelogId = currentState.SegmentId;
    }

    // Shortcut for observer startup.
    // TODO
    if (TargetState_ == TReachableState() && !IsLeader_ && !Options_.WriteChangelogsAtFollowers) {
        return;
    }

    YT_LOG_INFO("Replaying changelogs (ChangelogIds: %v-%v, TargetState: %v)",
        initialChangelogId,
        TargetState_.SegmentId,
        TargetState_);

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
            if (!IsLeader_ && !Options_.WriteChangelogsAtFollowers) {
                THROW_ERROR_EXCEPTION("Changelog %v is missing", changelogId);
            }

            // Leader can miss a changelog if we are recovering to snapshot version.

            YT_LOG_INFO("Changelog is missing and will be created (ChangelogId: %v)",
                changelogId);

            NHydra::NProto::TChangelogMeta meta;
            meta.set_term(term);
            changelog = WaitFor(ChangelogStore_->CreateChangelog(changelogId, meta))
                .ValueOrThrow();
        }

        if (!IsLeader_ && Options_.WriteChangelogsAtFollowers) {
            SyncChangelog(changelog, changelogId);
        }

        // int targetRecordId = changelogId == targetVersion.SegmentId
        //     ? targetVersion.RecordId
        //     : changelog->GetRecordCount();

        if (!ReplayChangelog(changelog, changelogId, TargetState_.SequenceNumber)) {
            TDelayedExecutor::WaitForDuration(Config_->ChangelogRecordCountCheckRetryPeriod);
            continue;
        }

        ++changelogId;
        // verify term
    }

    auto automatonState = DecoratedAutomaton_->GetReachableState();
    if (automatonState != TargetState_) {
        THROW_ERROR_EXCEPTION("Unable to recover to version (AutomatonState: %v, TargetState: %v)",
            automatonState,
            TargetState_);
    }
}

void TRecovery::SyncChangelog(IChangelogPtr changelog, int changelogId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto channel = EpochContext_->CellManager->GetPeerChannel(EpochContext_->LeaderId);
    YT_VERIFY(channel);

    TInternalHydraServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->ControlRpcTimeout);

    auto req = proxy.LookupChangelog();
    req->set_changelog_id(changelogId);

    auto rspOrError = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting changelog %v info from leader",
        changelogId);
    const auto& rsp = rspOrError.Value();

    int remoteRecordCount = rsp->record_count();
    int localRecordCount = changelog->GetRecordCount();

    YT_LOG_INFO("Syncing changelog %v: local %v, remote %v",
        changelogId,
        localRecordCount,
        remoteRecordCount);

    if (localRecordCount > remoteRecordCount) {
        auto result = WaitFor(changelog->Truncate(remoteRecordCount));
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error truncating changelog");
    } else if (localRecordCount < remoteRecordCount) {
        auto asyncResult = DownloadChangelog(
            Config_,
            EpochContext_->CellManager,
            ChangelogStore_,
            changelogId,
            remoteRecordCount);
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result, "Error downloading changelog records");
    }
}

bool TRecovery::ReplayChangelog(IChangelogPtr changelog, int changelogId, i64 targetSequenceNumber)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    const auto& meta = changelog->GetMeta();
    auto changelogTerm = meta.term();
    auto changelogRecordCount = changelog->GetRecordCount();
    YT_LOG_INFO("Replaying changelog %v (RecordCount: %v, TargetSequenceNumber: %v, Term: %v)",
        changelogId,
        changelogRecordCount,
        targetSequenceNumber,
        changelogTerm);

    int currentRecordId = 0;
    auto automatonVersion = DecoratedAutomaton_->GetAutomatonVersion();
    if (automatonVersion.SegmentId == changelogId) {
        currentRecordId = automatonVersion.RecordId;
    }

    while (currentRecordId < changelogRecordCount && DecoratedAutomaton_->GetSequenceNumber() < targetSequenceNumber) {
        auto automatonNumber = DecoratedAutomaton_->GetSequenceNumber();
        auto recordsNeeded = targetSequenceNumber - automatonNumber;
        YT_VERIFY(recordsNeeded > 0);

        YT_LOG_INFO("Trying to read mutations %v-%v from changelog %v",
            currentRecordId,
            currentRecordId + recordsNeeded - 1,
            changelogId);

        auto asyncRecordsData = changelog->Read(
            currentRecordId,
            recordsNeeded,
            Config_->MaxChangelogBytesPerRequest);
        auto recordsData = WaitFor(asyncRecordsData)
            .ValueOrThrow();
        int recordsRead = static_cast<int>(recordsData.size());

        YT_LOG_INFO("Finished reading records %v-%v from changelog %v",
            currentRecordId,
            currentRecordId + recordsRead - 1,
            changelogId);

        YT_LOG_INFO("Applying records %v-%v from changelog %v",
            currentRecordId,
            currentRecordId + recordsRead - 1,
            changelogId);

        auto future = BIND([=, this_ = MakeStrong(this), recordsData = std::move(recordsData)] {
                for (const auto& data : recordsData)  {
                    DecoratedAutomaton_->ApplyMutationDuringRecovery(data, changelogTerm);
                }
            })
            .AsyncVia(EpochContext_->EpochSystemAutomatonInvoker)
            .Run();

        WaitFor(future)
            .ThrowOnError();

        currentRecordId += recordsRead;
    }

    auto automatonSequenceNumber = DecoratedAutomaton_->GetSequenceNumber();
    YT_LOG_INFO("Replayed changelog (AutomatonSequenceNumber: %v, TargetSequenceNumber: %v)",
        automatonSequenceNumber,
        targetSequenceNumber);
    return true;
}

TFuture<void> TRecovery::Run(int term)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TRecovery::Recover, MakeStrong(this))
        .AsyncVia(EpochContext_->EpochControlInvoker)
        .Run(term);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
