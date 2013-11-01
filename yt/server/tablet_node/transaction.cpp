#include "stdafx.h"
#include "transaction.h"

#include <server/hydra/composite_automaton.h>

namespace NYT {
namespace NTabletNode {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(const TTransactionId& id)
    : Id_(id)
    , StartTime_(TInstant::Zero())
{ }

void TTransaction::Save(TSaveContext& context) const
{
    using NYT::Save;
    Save(context, Id_);
    Save(context, Timeout_);
    Save(context, StartTime_);
}

void TTransaction::Load(TLoadContext& context)
{
    using NYT::Load;
    Load(context, Id_);
    Load(context, Timeout_);
    Load(context, StartTime_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

