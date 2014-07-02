#include "stdafx.h"
#include "hydra_manager.h"

namespace NYT {
namespace NHydra {

///////////////////////////////////////////////////////////////////////////////

bool IHydraManager::IsLeader() const
{
    return GetAutomatonState() == EPeerState::Leading;
}

bool IHydraManager::IsFollower() const
{
    return GetAutomatonState() == EPeerState::Following;
}

bool IHydraManager::IsRecovery() const
{
    return GetAutomatonState() == EPeerState::LeaderRecovery ||
           GetAutomatonState() == EPeerState::FollowerRecovery;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
