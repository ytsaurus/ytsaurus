#include "commit.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/core/misc/serialize.h>

namespace NYT::NHiveServer {

using namespace NRpc;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TCommit::TCommit(TTransactionId transationId)
    : TransactionId_(transationId)
    , Persistent_(false)
{ }

TCommit::TCommit(
    TTransactionId transationId,
    const TMutationId& mutationId,
    const std::vector<TCellId>& participantCellIds,
    bool distributed,
    bool generatePrepareTimestamp,
    bool inheritCommitTimestamp,
    NApi::ETransactionCoordinatorCommitMode coordinatorCommitMode,
    const TString& userName)
    : TransactionId_(transationId)
    , MutationId_(mutationId)
    , ParticipantCellIds_(participantCellIds)
    , Distributed_(distributed)
    , GeneratePrepareTimestamp_(generatePrepareTimestamp)
    , InheritCommitTimestamp_(inheritCommitTimestamp)
    , CoordinatorCommitMode_(coordinatorCommitMode)
    , UserName_(userName)
{ }

TFuture<TSharedRefArray> TCommit::GetAsyncResponseMessage()
{
    return ResponseMessagePromise_;
}

void TCommit::SetResponseMessage(TSharedRefArray message)
{
    ResponseMessagePromise_.TrySet(std::move(message));
}

void TCommit::Save(TSaveContext& context) const
{
    using NYT::Save;

    YCHECK(Persistent_);
    Save(context, TransactionId_);
    Save(context, MutationId_);
    Save(context, ParticipantCellIds_);
    Save(context, Distributed_);
    Save(context, GeneratePrepareTimestamp_);
    Save(context, InheritCommitTimestamp_);
    Save(context, CommitTimestamps_);
    Save(context, PersistentState_);
    Save(context, CoordinatorCommitMode_);
    Save(context, UserName_);
}

void TCommit::Load(TLoadContext& context)
{
    using NYT::Load;

    Persistent_ = true;
    Load(context, TransactionId_);
    Load(context, MutationId_);
    Load(context, ParticipantCellIds_);
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
        Load(context, UserName_);
    } else {
        UserName_ = RootUserName;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer
