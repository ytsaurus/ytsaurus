#include "stdafx.h"
#include "hydra_service.h"
#include "hydra_manager.h"

#include <ytlib/hydra/hydra_service.pb.h>

#include <server/election/election_manager.h>

namespace NYT {
namespace NHydra {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
    IHydraManagerPtr hydraManager,
    IInvokerPtr automatonInvoker,
    const TServiceId& serviceId,
    const NLog::TLogger& logger,
    int protocolVersion)
    : TServiceBase(
        hydraManager->CreateGuardedAutomatonInvoker(automatonInvoker),
        serviceId,
        logger,
        protocolVersion)
    , AutomatonInvoker_(automatonInvoker)
    , ServiceHydraManager_(hydraManager)
{
    ServiceHydraManager_->SubscribeLeaderActive(BIND(&THydraServiceBase::OnLeaderActive, MakeWeak(this)));
    ServiceHydraManager_->SubscribeStopLeading(BIND(&THydraServiceBase::OnStopLeading, MakeWeak(this)));
}

void THydraServiceBase::OnLeaderActive()
{
    EpochAutomatonInvoker_ = ServiceHydraManager_
        ->GetAutomatonEpochContext()
        ->CancelableContext
        ->CreateInvoker(AutomatonInvoker_);
}

void THydraServiceBase::OnStopLeading()
{
    EpochAutomatonInvoker_.Reset();
}

void THydraServiceBase::ValidateActiveLeader()
{
    if (!ServiceHydraManager_->IsActiveLeader()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Not an active leader");
    }
}

void THydraServiceBase::ValidateActivePeer()
{
    if (!ServiceHydraManager_->IsActiveLeader() && !ServiceHydraManager_->IsActiveFollower()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Not an active peer");
    }
}

bool THydraServiceBase::IsUp(TCtxDiscoverPtr context) const
{
    const auto& request = context->Request();
    EPeerKind kind;
    if (request.HasExtension(NProto::TPeerKindExt::peer_kind_ext)) {
        auto ext = request.GetExtension(NProto::TPeerKindExt::peer_kind_ext);
        kind = EPeerKind(ext.peer_kind());
    } else {
        kind = EPeerKind::Leader;
    }

    bool isLeader = ServiceHydraManager_->IsActiveLeader();
    bool isFollower = ServiceHydraManager_->IsActiveFollower();
    switch (kind) {
        case EPeerKind::Leader:
            return isLeader;
        case EPeerKind::Follower:
            return isFollower;
        case EPeerKind::LeaderOrFollower :
            return isLeader || isFollower;
        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
