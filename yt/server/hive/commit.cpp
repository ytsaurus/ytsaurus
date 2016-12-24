#include "commit.h"

#include <yt/server/hydra/composite_automaton.h>

#include <yt/core/misc/serialize.h>

namespace NYT {
namespace NHiveServer {

using namespace NRpc;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TCommit::TCommit(const TTransactionId& transationId)
    : TransactionId_(transationId)
    , Persistent_(false)
{ }

TCommit::TCommit(
    const TTransactionId& transationId,
    const TMutationId& mutationId,
    const std::vector<TCellId>& participantCellIds,
    bool distributed)
    : TransactionId_(transationId)
    , MutationId_(mutationId)
    , ParticipantCellIds_(participantCellIds)
    , Distributed_(distributed)
    , Persistent_(false)
    , CommitTimestamp_(NTransactionClient::NullTimestamp)
    , TransientState_(ECommitState::Start)
    , PersistentState_(ECommitState::Start)
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
    Save(context, CommitTimestamp_);
    Save(context, PersistentState_);
}

void TCommit::Load(TLoadContext& context)
{
    using NYT::Load;

    // COMPAT(savrus)
    YCHECK(context.GetVersion() >= 2);

    Persistent_ = true;
    Load(context, TransactionId_);
    Load(context, MutationId_);
    Load(context, ParticipantCellIds_);
    Load(context, Distributed_);
    Load(context, CommitTimestamp_);
    Load(context, PersistentState_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
