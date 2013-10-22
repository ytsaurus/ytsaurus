#include "stdafx.h"
#include "commit.h"

#include <core/misc/serialize.h>

#include <server/hydra/composite_automaton.h>

namespace NYT {
namespace NHive {

using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TCommit::TCommit(const TTransactionId& transationId)
    : Persistent_(true)
    , TransactionId_(transationId)
{
    Init();
}

TCommit::TCommit(
    bool persistent,
    const TTransactionId& transationId,
    const std::vector<TCellGuid>& participantCellGuids)
    : Persistent_(persistent)
    , TransactionId_(transationId)
    , ParticipantCellGuids_(participantCellGuids)
{
    Init();
}

TAsyncError TCommit::GetResult()
{
    return Result_;
}

void TCommit::SetResult(const TError& error)
{
    Result_.Set(error);
}

void TCommit::Save(TSaveContext& context) const
{
    using NYT::Save;
    YCHECK(!Persistent_);

    Save(context, TransactionId_);
    Save(context, ParticipantCellGuids_);
    Save(context, PreparedParticipantCellGuids_);
    Save(context, CommitTimestamp_);
}

void TCommit::Load(TLoadContext& context)
{
    using NYT::Load;
    YCHECK(Persistent_);

    Load(context, TransactionId_);
    Load(context, ParticipantCellGuids_);
    Load(context, PreparedParticipantCellGuids_);
    Load(context, CommitTimestamp_);
}

void TCommit::Init()
{
    CommitTimestamp_ = NullTimestamp;
    Result_ = NewPromise<TError>();
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NHive
} // namespace NYT
