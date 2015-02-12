#include "stdafx.h"
#include "commit.h"

#include <core/misc/serialize.h>

#include <server/hydra/composite_automaton.h>

namespace NYT {
namespace NHive {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TCommit::TCommit(const TTransactionId& transationId)
    : TransactionId_(transationId)
    , Persistent_(false)
{ }

TCommit::TCommit(
    const TTransactionId& transationId,
    const TMutationId& mutationId,
    const std::vector<TCellId>& participantCellIds)
    : TransactionId_(transationId)
    , MutationId_(mutationId)
    , ParticipantCellIds_(participantCellIds)
    , Persistent_(false)
{ }

TFuture<TSharedRefArray> TCommit::GetAsyncResponseMessage()
{
    return ResponseMessagePromise_;
}

void TCommit::SetResponseMessage(TSharedRefArray message)
{
    ResponseMessagePromise_.Set(std::move(message));
}

bool TCommit::IsDistributed() const
{
    return !ParticipantCellIds_.empty();
}

void TCommit::Save(TSaveContext& context) const
{
    using NYT::Save;

    YCHECK(Persistent_);
    Save(context, TransactionId_);
    Save(context, MutationId_);
    Save(context, ParticipantCellIds_);
    Save(context, PreparedParticipantCellIds_);
}

void TCommit::Load(TLoadContext& context)
{
    using NYT::Load;

    Persistent_ = true;
    Load(context, TransactionId_);
    Load(context, MutationId_);
    Load(context, ParticipantCellIds_);
    Load(context, PreparedParticipantCellIds_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
