#include "sequoia_actions_executor_state.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TSequoiaActionsExecutorState::TSequoiaActionsExecutorState(TBootstrap* bootstrap)
    : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::SequoiaActionExecutor)

{
    RegisterLoader("SequoiaActionsExecutor.Values", BIND_NO_PROPAGATE(&TSequoiaActionsExecutorState::LoadValues, Unretained(this)));
    RegisterSaver(
        ESyncSerializationPriority::Values,
        "SequoiaActionsExecutor.Values",
        BIND_NO_PROPAGATE(&TSequoiaActionsExecutorState::SaveValues, Unretained(this)));
}

void TSequoiaActionsExecutorState::TPreparedNodeModification::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, SequoiaNodeId);
    Persist(context, Type);
}

void TSequoiaActionsExecutorState::SaveValues(NCellMaster::TSaveContext& context)
{
    using NYT::Save;
    Save(context, PreparedModifications_);
}

void TSequoiaActionsExecutorState::LoadValues(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, PreparedModifications_);
}

void TSequoiaActionsExecutorState::Clear()
{
    PreparedModifications_.clear();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
