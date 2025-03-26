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
    NLogging::TLogger logger,
    IUpstreamSynchronizerPtr upstreamSynchronizer,
    TServiceOptions options)
    : TServiceBase(
        std::move(defaultInvoker),
        descriptor,
        std::move(logger),
        std::move(options))
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
    if (request.HasExtension(NProto::TPeerKindExt::discover_peer_kind_ext)) {
        const auto& ext = request.GetExtension(NProto::TPeerKindExt::discover_peer_kind_ext);
        kind = FromProto<EPeerKind>(ext.peer_kind());
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

void THydraServiceBase::BeforeInvoke(IServiceContext* context)
{
    TServiceBase::BeforeInvoke(context);

    if (context->RequestHeader().HasExtension(NProto::TPeerKindExt::request_peer_kind_ext)) {
        const auto& ext = context->RequestHeader().GetExtension(NProto::TPeerKindExt::request_peer_kind_ext);
        auto kind = FromProto<EPeerKind>(ext.peer_kind());
        ValidatePeer(kind);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
