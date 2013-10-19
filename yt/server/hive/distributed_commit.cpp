#include "stdafx.h"
#include "distributed_commit.h"

#include <core/misc/serialize.h>

#include <server/hydra/composite_automaton.h>

namespace NYT {
namespace NHive {

using namespace NHydra;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TDistributedCommit::TDistributedCommit(const TTransactionId& transationId)
    : TransactionId_(transationId)
{ }

TDistributedCommit::TDistributedCommit(
    const TTransactionId& transationId,
    const std::vector<TCellGuid>& participantCellGuids)
    : TransactionId_(transationId)
    , ParticipantCellGuids_(participantCellGuids)
{ }

void TDistributedCommit::Save(TSaveContext& context) const
{
    using NYT::Save;

    Save(context, TransactionId_);
    Save(context, ParticipantCellGuids_);
    Save(context, PreparedParticipantCellGuids_);
}

void TDistributedCommit::Load(TLoadContext& context)
{
    using NYT::Load;

    Load(context, TransactionId_);
    Load(context, ParticipantCellGuids_);
    Load(context, PreparedParticipantCellGuids_);
}

////////////////////////////////////////////////////////////////////////////////


} // namespace NHive
} // namespace NYT
