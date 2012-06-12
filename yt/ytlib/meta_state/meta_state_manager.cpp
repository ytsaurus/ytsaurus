#include "stdafx.h"
#include "meta_state_manager.h"

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

bool IMetaStateManager::IsLeader() const
{
    auto status = GetStateStatus();
    return status == EPeerStatus::Leading;
}

bool IMetaStateManager::IsFolllower() const
{
    auto status = GetStateStatus();
    return status == EPeerStatus::Following;
}

bool IMetaStateManager::IsRecovery() const
{
    auto status = GetStateStatus();
    return status == EPeerStatus::LeaderRecovery || status == EPeerStatus::FollowerRecovery;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
