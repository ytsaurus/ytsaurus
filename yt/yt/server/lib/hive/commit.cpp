#include "commit.h"

#include <yt/yt/server/lib/hydra/composite_automaton.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NHiveServer {

using namespace NRpc;
using namespace NHydra;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TCommit::TCommit(TTransactionId transationId)
    : TransactionId_(transationId)
    , Persistent_(false)
{ }

TCommit::TCommit(
    TTransactionId transationId,
    TMutationId mutationId,
    std::vector<TCellId> participantCellIds,
    std::vector<TCellId> prepareOnlyParticipantCellIds,
    std::vector<TCellId> cellIdsToSyncWithBeforePrepare,
    bool distributed,
    bool generatePrepareTimestamp,
    bool inheritCommitTimestamp,
    NApi::ETransactionCoordinatorCommitMode coordinatorCommitMode,
    NRpc::TAuthenticationIdentity identity,
    std::vector<TTransactionId> prerequisiteTransactionIds)
    : TransactionId_(transationId)
    , MutationId_(mutationId)
    , ParticipantCellIds_(std::move(participantCellIds))
    , PrepareOnlyParticipantCellIds_(std::move(prepareOnlyParticipantCellIds))
    , CellIdsToSyncWithBeforePrepare_(std::move(cellIdsToSyncWithBeforePrepare))
    , Distributed_(distributed)
    , GeneratePrepareTimestamp_(generatePrepareTimestamp)
    , InheritCommitTimestamp_(inheritCommitTimestamp)
    , CoordinatorCommitMode_(coordinatorCommitMode)
    , AuthenticationIdentity_(std::move(identity))
    , PrerequisiteTransactionIds_(std::move(prerequisiteTransactionIds))
{ }

TFuture<TSharedRefArray> TCommit::GetAsyncResponseMessage()
{
    return ResponseMessagePromise_;
}

void TCommit::SetResponseMessage(TSharedRefArray message)
{
    ResponseMessagePromise_.TrySet(std::move(message));
}

bool TCommit::IsPrepareOnlyParticipant(TCellId cellId) const
{
    return
        std::find(PrepareOnlyParticipantCellIds_.begin(), PrepareOnlyParticipantCellIds_.end(), cellId) !=
        PrepareOnlyParticipantCellIds_.end();
}

void TCommit::Save(TSaveContext& context) const
{
    using NYT::Save;

    YT_VERIFY(Persistent_);
    Save(context, TransactionId_);
    Save(context, MutationId_);
    Save(context, ParticipantCellIds_);
    Save(context, PrepareOnlyParticipantCellIds_);
    Save(context, CellIdsToSyncWithBeforePrepare_);
    Save(context, Distributed_);
    Save(context, GeneratePrepareTimestamp_);
    Save(context, InheritCommitTimestamp_);
    Save(context, CommitTimestamps_);
    Save(context, PersistentState_);
    Save(context, CoordinatorCommitMode_);
    Save(context, AuthenticationIdentity_.User);
    Save(context, AuthenticationIdentity_.UserTag);
}

void TCommit::Load(TLoadContext& context)
{
    using NYT::Load;

    Persistent_ = true;
    Load(context, TransactionId_);
    Load(context, MutationId_);
    Load(context, ParticipantCellIds_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 8) {
        Load(context, PrepareOnlyParticipantCellIds_);
    }
    // COMPAT(babenko)
    if (context.GetVersion() >= 10) {
        Load(context, CellIdsToSyncWithBeforePrepare_);
    }
    Load(context, Distributed_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 5) {
        Load(context, GeneratePrepareTimestamp_);
    } else {
        GeneratePrepareTimestamp_ = true;
    }
    Load(context, InheritCommitTimestamp_);
    Load(context, CommitTimestamps_);
    Load(context, PersistentState_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 4) {
        Load(context, CoordinatorCommitMode_);
    } else {
        CoordinatorCommitMode_ = NApi::ETransactionCoordinatorCommitMode::Eager;
    }
    // COMPAT(savrus)
    if (context.GetVersion() >= 6) {
        // COMPAT(babenko)
        Load(context, AuthenticationIdentity_.User);
        if (context.GetVersion() >= 9) {
            Load(context, AuthenticationIdentity_.UserTag);
        } else {
            AuthenticationIdentity_.UserTag = AuthenticationIdentity_.User;
        }
    } else {
        AuthenticationIdentity_ = GetRootAuthenticationIdentity();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
