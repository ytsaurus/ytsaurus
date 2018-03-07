#include "hydra_manager.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

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

bool IHydraManager::IsActive() const
{
    return IsActiveLeader() || IsActiveFollower();
}

void IHydraManager::ValidatePeer(EPeerKind kind)
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
            if (!IsActiveLeader() && !IsActiveFollower()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active peer");
            }
            break;

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
