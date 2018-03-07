#include "hydra_service.h"
#include "hydra_manager.h"

#include <yt/server/election/election_manager.h>

#include <yt/ytlib/hydra/hydra_service.pb.h>

#include <yt/core/actions/cancelable_context.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
    IInvokerPtr invoker,
    const TServiceDescriptor& descriptor,
    const NLogging::TLogger& logger,
    const TRealmId& realmId)
    : TServiceBase(
        invoker,
        descriptor,
        logger,
        realmId)
{ }

void THydraServiceBase::ValidatePeer(EPeerKind kind)
{
    auto hydraManager = GetHydraManager();
    hydraManager->ValidatePeer(kind);

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

bool THydraServiceBase::IsUp(const TCtxDiscoverPtr& context)
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
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
