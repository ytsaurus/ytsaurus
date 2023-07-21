#include "proxying_chunk_service.h"
#include "config.h"
#include "private.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/retrying_channel.h>

#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NClusterNode {

using namespace NRpc;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NNodeTrackerClient;
using namespace NElection;
using namespace NApi;
using namespace NApi::NNative;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

class TProxyingChunkService
    : public NRpc::TServiceBase
{
public:
    TProxyingChunkService(
        TCellId cellId,
        TProxyingChunkServiceConfigPtr serviceConfig,
        TMasterConnectionConfigPtr masterConnectionConfig,
        NApi::NNative::TConnectionDynamicConfigPtr connectionConfig,
        IChannelFactoryPtr channelFactory,
        IAuthenticatorPtr authenticator)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TChunkServiceProxy::GetDescriptor(),
            ClusterNodeLogger.WithTag("CellTag: %v", CellTagFromId(cellId)),
            cellId,
            std::move(authenticator))
        , ServiceConfig_(std::move(serviceConfig))
        , ConnectionConfig_(std::move(masterConnectionConfig))
        , LeaderChannel_(CreateMasterChannel(channelFactory, ConnectionConfig_, EPeerKind::Leader))
        , FollowerChannel_(CreateMasterChannel(channelFactory, ConnectionConfig_, EPeerKind::Follower))
        , CostThrottler_(CreateReconfigurableThroughputThrottler(ServiceConfig_->CostThrottler))
        , LocateChunksHandler_(New<TLocateChunksHandler>(this))
        , LocateDynamicStoresHandler_(New<TLocateDynamicStoresHandler>(this))
        , AllocateWriteTargetsHandler_(New<TAllocateWriteTargetsHandler>(
            this,
            /*useFollowers*/ connectionConfig->UseFollowersForWriteTargetsAllocation))
        , ExecuteBatchHandler_(New<TExecuteBatchHandler>(this))
        , CreateChunkHandler_(New<TCreateChunkHandler>(this))
        , ConfirmChunkHandler_(New<TConfirmChunkHandler>(this))
        , SealChunkHandler_(New<TSealChunkHandler>(this))
        , CreateChunkListsHandler_(New<TCreateChunkListsHandler>(this))
        , UnstageChunkTreeHandler_(New<TUnstageChunkTreeHandler>(this))
        , AttachChunkTreesHandler_(New<TAttachChunkTreesHandler>(this))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateChunks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateDynamicStores));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AllocateWriteTargets));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecuteBatch));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ConfirmChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SealChunk));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CreateChunkLists));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(UnstageChunkTree));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AttachChunkTrees));
    }

private:
    const TProxyingChunkServiceConfigPtr ServiceConfig_;
    const TMasterConnectionConfigPtr ConnectionConfig_;

    const IChannelPtr LeaderChannel_;
    const IChannelPtr FollowerChannel_;

    const IThroughputThrottlerPtr CostThrottler_;


    static IChannelPtr CreateMasterChannel(
        IChannelFactoryPtr channelFactory,
        TMasterConnectionConfigPtr config,
        EPeerKind peerKind)
    {
        return CreateRetryingChannel(
            config,
            CreatePeerChannel(config, channelFactory, peerKind));
    }

    template <class TRequestMessage, class TResponseMessage>
    class THandlerBase
        : public TRefCounted
    {
    public:
        using TResponse = TTypedClientResponse<TResponseMessage>;
        using TResponsePtr = TIntrusivePtr<TResponse>;
        using TRequest = TTypedClientRequest<TRequestMessage, TResponse>;
        using TRequestPtr = TIntrusivePtr<TRequest>;
        using TContext = TTypedServiceContext<TRequestMessage, TResponseMessage>;
        using TContextPtr = TIntrusivePtr<TContext>;

        explicit THandlerBase(TProxyingChunkService* owner)
            : Owner_(owner)
            , Logger(owner->Logger)
            , LeaderProxy_(owner->LeaderChannel_)
            , FollowerProxy_(owner->FollowerChannel_)
        { }

        void HandleRequest(const TContextPtr& context)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            context->SetRequestInfo();

            const auto& user = context->RequestHeader().user();

            auto proxyRequest = CreateRequest();
            GenerateMutationId(proxyRequest);
            proxyRequest->SetUser(user);
            proxyRequest->SetTimeout(owner->ConnectionConfig_->RpcTimeout);
            proxyRequest->CopyFrom(context->Request());

            YT_LOG_DEBUG("Chunk Service proxy request created (User: %v, RequestId: %v -> %v)",
                user,
                context->GetRequestId(),
                proxyRequest->GetRequestId());

            ForwardRequest(context, proxyRequest);
        }

    protected:
        const TWeakPtr<TProxyingChunkService> Owner_;
        const NLogging::TLogger Logger;

        TChunkServiceProxy LeaderProxy_;
        TChunkServiceProxy FollowerProxy_;

        virtual TRequestPtr CreateRequest() = 0;
        virtual int GetCost(const TRequestPtr& /*request*/) const
        {
            static const auto simpleRequestCost = 1;
            return simpleRequestCost;
        }

    private:
        void ForwardRequest(const TContextPtr& context, const TRequestPtr& request)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            owner->CostThrottler_->Throttle(GetCost(request))
                .Subscribe(BIND(&THandlerBase::DoForwardRequest, MakeStrong(this), context, request)
                    .Via(owner->GetDefaultInvoker()));
        }

        void DoForwardRequest(const TContextPtr& context, const TRequestPtr& request, const TError& /*error*/)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            YT_LOG_DEBUG("Chunk Service proxy request sent (RequestId: %v)",
                request->GetRequestId());

            request->Invoke().Subscribe(
                BIND(&THandlerBase::OnResponse, MakeStrong(this), context)
                    .Via(owner->GetDefaultInvoker()));
        }

        void OnResponse(const TContextPtr& context, const TErrorOr<TResponsePtr>& responseOrError)
        {
            if (responseOrError.IsOK()) {
                context->Response().CopyFrom(*responseOrError.Value());
                context->Reply();
            } else {
                context->Reply(responseOrError);
            }
        }
    };


    class TLocateChunksHandler
        : public THandlerBase<TReqLocateChunks, TRspLocateChunks>
    {
    public:
        explicit TLocateChunksHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqLocateChunksPtr CreateRequest() override
        {
            auto req = FollowerProxy_.LocateChunks();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }

        int GetCost(const TRequestPtr& request) const override
        {
            return request->subrequests_size();
        }
    };

    const TIntrusivePtr<TLocateChunksHandler> LocateChunksHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateChunks)
    {
        LocateChunksHandler_->HandleRequest(context);
    }


    class TLocateDynamicStoresHandler
        : public THandlerBase<TReqLocateDynamicStores, TRspLocateDynamicStores>
    {
    public:
        explicit TLocateDynamicStoresHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqLocateDynamicStoresPtr CreateRequest() override
        {
            auto req = FollowerProxy_.LocateDynamicStores();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }

        int GetCost(const TRequestPtr& request) const override
        {
            return request->subrequests().size();
        }
    };

    const TIntrusivePtr<TLocateDynamicStoresHandler> LocateDynamicStoresHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateDynamicStores)
    {
        LocateDynamicStoresHandler_->HandleRequest(context);
    }


    class TAllocateWriteTargetsHandler
        : public THandlerBase<TReqAllocateWriteTargets, TRspAllocateWriteTargets>
    {
    public:
        TAllocateWriteTargetsHandler(TProxyingChunkService* owner, bool useFollowers)
            : THandlerBase(owner)
            , UseFollowers_(useFollowers)
        { }

    protected:
        TChunkServiceProxy::TReqAllocateWriteTargetsPtr CreateRequest() override
        {
            auto proxy = UseFollowers_ ? LeaderProxy_ : FollowerProxy_;
            return proxy.AllocateWriteTargets();
        }

        int GetCost(const TRequestPtr& request) const override
        {
            return request->subrequests_size();
        }

    private:
        const bool UseFollowers_;
    };

    const TIntrusivePtr<TAllocateWriteTargetsHandler> AllocateWriteTargetsHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AllocateWriteTargets)
    {
        AllocateWriteTargetsHandler_->HandleRequest(context);
    }


    class TExecuteBatchHandler
        : public THandlerBase<TReqExecuteBatch, TRspExecuteBatch>
    {
    public:
        explicit TExecuteBatchHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqExecuteBatchPtr CreateRequest() override
        {
            return LeaderProxy_.ExecuteBatch();
        }

        int GetCost(const TRequestPtr& request) const override
        {
            return
                request->create_chunk_subrequests_size() +
                request->confirm_chunk_subrequests_size() +
                request->seal_chunk_subrequests_size() +
                request->attach_chunk_trees_subrequests_size();
        }
    };

    const TIntrusivePtr<TExecuteBatchHandler> ExecuteBatchHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ExecuteBatch)
    {
        ExecuteBatchHandler_->HandleRequest(context);
    }


    class TCreateChunkHandler
        : public THandlerBase<TReqCreateChunk, TRspCreateChunk>
    {
    public:
        explicit TCreateChunkHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqCreateChunkPtr CreateRequest() override
        {
            auto req = LeaderProxy_.CreateChunk();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }
    };

    const TIntrusivePtr<TCreateChunkHandler> CreateChunkHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CreateChunk)
    {
        CreateChunkHandler_->HandleRequest(context);
    }


    class TConfirmChunkHandler
        : public THandlerBase<TReqConfirmChunk, TRspConfirmChunk>
    {
    public:
        explicit TConfirmChunkHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqConfirmChunkPtr CreateRequest() override
        {
            auto req = LeaderProxy_.ConfirmChunk();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }
    };

    const TIntrusivePtr<TConfirmChunkHandler> ConfirmChunkHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ConfirmChunk)
    {
        ConfirmChunkHandler_->HandleRequest(context);
    }


    class TSealChunkHandler
        : public THandlerBase<TReqSealChunk, TRspSealChunk>
    {
    public:
        explicit TSealChunkHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqSealChunkPtr CreateRequest() override
        {
            auto req = LeaderProxy_.SealChunk();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }
    };

    const TIntrusivePtr<TSealChunkHandler> SealChunkHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, SealChunk)
    {
        SealChunkHandler_->HandleRequest(context);
    }


    class TCreateChunkListsHandler
        : public THandlerBase<TReqCreateChunkLists, TRspCreateChunkLists>
    {
    public:
        explicit TCreateChunkListsHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqCreateChunkListsPtr CreateRequest() override
        {
            auto req = LeaderProxy_.CreateChunkLists();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }
    };

    const TIntrusivePtr<TCreateChunkListsHandler> CreateChunkListsHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, CreateChunkLists)
    {
        CreateChunkListsHandler_->HandleRequest(context);
    }

    class TUnstageChunkTreeHandler
        : public THandlerBase<TReqUnstageChunkTree, TRspUnstageChunkTree>
    {
    public:
        explicit TUnstageChunkTreeHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqUnstageChunkTreePtr CreateRequest() override
        {
            auto req = LeaderProxy_.UnstageChunkTree();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }
    };

    const TIntrusivePtr<TUnstageChunkTreeHandler> UnstageChunkTreeHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, UnstageChunkTree)
    {
        UnstageChunkTreeHandler_->HandleRequest(context);
    }

    class TAttachChunkTreesHandler
        : public THandlerBase<TReqAttachChunkTrees, TRspAttachChunkTrees>
    {
    public:
        explicit TAttachChunkTreesHandler(TProxyingChunkService* owner)
            : THandlerBase(owner)
        { }

    protected:
        TChunkServiceProxy::TReqAttachChunkTreesPtr CreateRequest() override
        {
            auto req = LeaderProxy_.AttachChunkTrees();
            req->SetRequestHeavy(true);
            req->SetResponseHeavy(true);
            return req;
        }
    };

    const TIntrusivePtr<TAttachChunkTreesHandler> AttachChunkTreesHandler_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AttachChunkTrees)
    {
        AttachChunkTreesHandler_->HandleRequest(context);
    }
};

IServicePtr CreateProxyingChunkService(
    TCellId cellId,
    TProxyingChunkServiceConfigPtr serviceConfig,
    TMasterConnectionConfigPtr connectionConfig,
    IChannelFactoryPtr channelFactory,
    IAuthenticatorPtr authenticator)
{
    return New<TProxyingChunkService>(
        cellId,
        serviceConfig,
        connectionConfig,
        channelFactory,
        authenticator);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
