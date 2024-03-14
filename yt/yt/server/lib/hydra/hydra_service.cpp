#include "hydra_manager.h"
#include "hydra_service.h"

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/ytlib/hydra/proto/hydra_service.pb.h>

#include <yt/yt/core/actions/cancelable_context.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class THydraManagerUpstreamSynchronizer
    : public IUpstreamSynchronizer
{
public:
    explicit THydraManagerUpstreamSynchronizer(TWeakPtr<IHydraManager> hydraManager)
        : HydraManager_(std::move(hydraManager))
    { }

    TFuture<void> SyncWithUpstream() override
    {
        if (auto hydraManager = HydraManager_.Lock()) {
            return hydraManager->SyncWithLeader();
        }
        return MakeFuture<void>(TError(NRpc::EErrorCode::Unavailable, "Hydra manager has been destroyed"));
    }

private:
    const TWeakPtr<IHydraManager> HydraManager_;
};

IUpstreamSynchronizerPtr CreateHydraManagerUpstreamSynchronizer(TWeakPtr<IHydraManager> hydraManager)
{
    return New<THydraManagerUpstreamSynchronizer>(std::move(hydraManager));
}

////////////////////////////////////////////////////////////////////////////////

THydraServiceBase::THydraServiceBase(
    IHydraManagerPtr hydraManager,
    IInvokerPtr defaultInvoker,
    const TServiceDescriptor& descriptor,
    const NLogging::TLogger& logger,
    TRealmId realmId,
    IUpstreamSynchronizerPtr upstreamSynchronizer,
    IAuthenticatorPtr authenticator)
    : TServiceBase(
        defaultInvoker,
        descriptor,
        logger,
        realmId,
        std::move(authenticator))
    , HydraManager_(std::move(hydraManager))
    , UpstreamSynchronizer_(std::move(upstreamSynchronizer))
{ }

void THydraServiceBase::ValidatePeer(EPeerKind kind)
{
    auto hydraManager = HydraManager_.Lock();
    if (!hydraManager) {
        THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Service is shutting down");
    }

    hydraManager->ValidatePeer(kind);
}

void THydraServiceBase::SyncWithUpstream()
{
    WaitFor(UpstreamSynchronizer_->SyncWithUpstream())
        .ThrowOnError();
}

bool THydraServiceBase::IsUp(const TCtxDiscoverPtr& context)
{
    const auto& request = context->Request();
    EPeerKind kind;
    if (request.HasExtension(NProto::TPeerKindExt::peer_kind_ext)) {
        const auto& ext = request.GetExtension(NProto::TPeerKindExt::peer_kind_ext);
        kind = CheckedEnumCast<EPeerKind>(ext.peer_kind());
    } else {
        kind = EPeerKind::Leader;
    }

    auto hydraManager = HydraManager_.Lock();
    if (!hydraManager) {
        return false;
    }

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
            YT_ABORT();
    }
}

void THydraServiceBase::EnrichDiscoverResponse(TRspDiscover* response)
{
    auto hydraManager = HydraManager_.Lock();
    if (!hydraManager) {
        return;
    }

    auto* ext = response->MutableExtension(NProto::TDiscombobulationExt::discombobulation_ext);
    ext->set_discombobulated(hydraManager->IsDiscombobulated());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
