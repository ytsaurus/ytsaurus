#include "stdafx.h"
#include "hydra_service.h"
#include "hydra_manager.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/hydra/hydra_service.pb.h>

#include <server/election/election_manager.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
    IInvokerPtr invoker,
    const TServiceId& serviceId,
    const NLogging::TLogger& logger,
    int protocolVersion)
    : TServiceBase(
        invoker,
        serviceId,
        logger,
        protocolVersion)
{ }

void THydraServiceBase::ValidatePeer(EPeerKind kind)
{
    auto hydraManager = GetHydraManager();
    switch (kind) {
        case EPeerKind::Leader:
            if (!hydraManager->IsActiveLeader()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active leader");
            }
            break;

        case EPeerKind::Follower:
            if (!hydraManager->IsActiveFollower()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active follower");
            }
            break;

        case EPeerKind::LeaderOrFollower:
            if (!hydraManager->IsActiveLeader() && !hydraManager->IsActiveFollower()) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::Unavailable,
                    "Not an active peer");
            }
            break;

        default:
            YUNREACHABLE();
    }

    auto cancelableInvoker = hydraManager
        ->GetAutomatonCancelableContext()
        ->CreateInvoker(GetCurrentInvoker());
    SetCurrentInvoker(std::move(cancelableInvoker));
}

void THydraServiceBase::SyncWithUpstream()
{
    auto hydraManager = GetHydraManager();
    WaitFor(hydraManager->SyncWithUpstream())
        .ThrowOnError();
}

bool THydraServiceBase::IsUp(TCtxDiscoverPtr context)
{
    const auto& request = context->Request();
    EPeerKind kind;
    if (request.HasExtension(NProto::TPeerKindExt::peer_kind_ext)) {
        const auto& ext = request.GetExtension(NProto::TPeerKindExt::peer_kind_ext);
        kind = EPeerKind(ext.peer_kind());
    } else {
        kind = EPeerKind::Leader;
    }

    auto hydraManager = GetHydraManager();
    bool isLeader = hydraManager->IsActiveLeader();
    bool isFollower = hydraManager->IsActiveFollower();
    switch (kind) {
        case EPeerKind::Leader:
            return isLeader;
        case EPeerKind::Follower:
            return isFollower;
        case EPeerKind::LeaderOrFollower:
            return isLeader || isFollower;
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
