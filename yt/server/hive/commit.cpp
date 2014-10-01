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
{
    Init();
}

TCommit::TCommit(
    const TTransactionId& transationId,
    const TMutationId& mutationId,
    const std::vector<TCellGuid>& participantCellGuids)
    : TransactionId_(transationId)
    , MutationId_(mutationId)
    , ParticipantCellGuids_(participantCellGuids)
{
    Init();
}

TFuture<TSharedRefArray> TCommit::GetResult()
{
    return Result_;
}

void TCommit::SetResult(TSharedRefArray result)
{
    Result_.Set(std::move(result));
}

bool TCommit::IsDistributed() const
{
    return !ParticipantCellGuids_.empty();
}

void TCommit::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, TransactionId_);
    Save(context, MutationId_);
    Save(context, ParticipantCellGuids_);
    Save(context, PreparedParticipantCellGuids_);
}

void TCommit::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, TransactionId_);
    Load(context, MutationId_);
    Load(context, ParticipantCellGuids_);
    Load(context, PreparedParticipantCellGuids_);
}

void TCommit::Init()
{
    Result_ = NewPromise<TSharedRefArray>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
