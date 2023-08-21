#include "hydra_manager.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

bool ISimpleHydraManager::IsLeader() const
{
    return GetAutomatonState() == EPeerState::Leading;
}

bool ISimpleHydraManager::IsFollower() const
{
    return GetAutomatonState() == EPeerState::Following;
}

bool ISimpleHydraManager::IsRecovery() const
{
    return GetAutomatonState() == EPeerState::LeaderRecovery ||
           GetAutomatonState() == EPeerState::FollowerRecovery;
}

bool ISimpleHydraManager::IsActive() const
{
    return IsActiveLeader() || IsActiveFollower();
}

void ISimpleHydraManager::ValidatePeer(EPeerKind kind)
{
    switch (kind) {
        case EPeerKind::Leader:
            if (!IsActiveLeader()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }
            break;

        case EPeerKind::Follower:
            if (!IsActiveFollower()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active follower");
            }
            break;

        case EPeerKind::LeaderOrFollower:
            if (!IsActive()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active peer");
            }
            break;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
