#include "object_service.h"

#include "config.h"
#include "private.h"

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NCypressProxy {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TObjectService
    : public IObjectService
    , public TServiceBase
{
public:
    TObjectService(
        NApi::NNative::IConnectionPtr connection,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            /*invoker*/ nullptr,
            TObjectServiceProxy::GetDescriptor(),
            CypressProxyLogger,
            NullRealmId,
            std::move(authenticator))
        , Connection_(std::move(connection))
        , ThreadPool_(CreateThreadPool(/*threadCount*/ 1, "ObjectService"))
        , Invoker_(ThreadPool_->GetInvoker())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(10'000)
            .SetConcurrencyLimit(10'000)
            .SetInvoker(Invoker_));

        DeclareServerFeature(EMasterFeature::Portals);
        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);
    }

    void Reconfigure(const TObjectServiceDynamicConfigPtr& config) override
    {
        ThreadPool_->Configure(config->ThreadPoolSize);
    }

    IServicePtr GetService() override
    {
        return MakeStrong(this);
    }

private:
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);

    const NApi::NNative::IConnectionPtr Connection_;

    const IThreadPoolPtr ThreadPool_;
    const IInvokerPtr Invoker_;
};

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    if (!request->has_cell_tag()) {
        THROW_ERROR_EXCEPTION("Cell tag is not provided in request");
    }

    if (!request->has_peer_kind()) {
        THROW_ERROR_EXCEPTION("Peer kind is not provided in request");
    }

    auto cellTag = FromProto<TCellTag>(request->cell_tag());
    auto peerKind = FromProto<EMasterChannelKind>(request->peer_kind());

    context->SetRequestInfo("CellTag: %v, PeerKind: %v, RequestCount: %v",
        cellTag,
        peerKind,
        request->part_counts_size());

    if (peerKind != EMasterChannelKind::Leader &&
        peerKind != EMasterChannelKind::Follower)
    {
        THROW_ERROR_EXCEPTION("Expected %Qv or %Qv peer kind, got %Qv",
            EMasterChannelKind::Leader,
            EMasterChannelKind::Follower,
            peerKind);
    }

    auto masterChannel = Connection_->GetMasterChannelOrThrow(peerKind, cellTag);
    auto proxy = TObjectServiceProxy::FromDirectMasterChannel(std::move(masterChannel));
    auto req = proxy.Execute();
    req->CopyFrom(*request);
    req->Attachments() = request->Attachments();

    if (!req->has_original_request_id()) {
        ToProto(req->mutable_original_request_id(), context->GetRequestId());
    }

    SetAuthenticationIdentity(req, context->GetAuthenticationIdentity());

    req->Invoke().Subscribe(
        BIND([=, /*this,*/ this_ = MakeStrong(this)] (const TErrorOr<TObjectServiceProxy::TRspExecutePtr>& rspOrError) {
            if (!rspOrError.IsOK()) {
                context->Reply(rspOrError);
                return;
            }

            const auto& rsp = rspOrError.Value();
            response->CopyFrom(*rsp);
            response->Attachments() = rsp->Attachments();

            context->Reply();
        }).Via(Invoker_));
}

////////////////////////////////////////////////////////////////////////////////

IObjectServicePtr CreateObjectService(
    NApi::NNative::IConnectionPtr connection,
    IAuthenticatorPtr authenticator)
{
    return New<TObjectService>(std::move(connection), std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
