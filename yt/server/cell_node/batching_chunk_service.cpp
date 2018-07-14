#include "batching_chunk_service.h"
#include "config.h"
#include "private.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_replica.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/node_directory_builder.h>

#include <yt/ytlib/api/native/config.h>

#include <yt/ytlib/hydra/peer_channel.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/dispatcher.h>
#include <yt/core/rpc/retrying_channel.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT {
namespace NCellNode {

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

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

class TBatchingChunkService
    : public NRpc::TServiceBase
{
public:
    TBatchingChunkService(
        const TCellId& cellId,
        TBatchingChunkServiceConfigPtr serviceConfig,
        TMasterConnectionConfigPtr connectionConfig,
        IChannelFactoryPtr channelFactory)
        : TServiceBase(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            TChunkServiceProxy::GetDescriptor(),
            NLogging::TLogger(CellNodeLogger)
                .AddTag("CellTag: %v", CellTagFromId(cellId)),
            cellId)
        , ServiceConfig_(std::move(serviceConfig))
        , ConnectionConfig_(std::move(connectionConfig))
        , LeaderChannel_(CreateMasterChannel(channelFactory, ConnectionConfig_, EPeerKind::Leader))
        , FollowerChannel_(CreateMasterChannel(channelFactory, ConnectionConfig_, EPeerKind::Follower))
        , CostThrottler_(CreateReconfigurableThroughputThrottler(ServiceConfig_->CostThrottler))
        , LocateChunksBatcher_(New<TLocateChunksBatcher>(this))
        , AllocateWriteTargetsBatcher_(New<TAllocateWriteTargetsBatcher>(this))
        , ExecuteBatchBatcher_(New<TExecuteBatchBatcher>(this))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LocateChunks));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(AllocateWriteTargets));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ExecuteBatch));
    }

private:
    const TBatchingChunkServiceConfigPtr ServiceConfig_;
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

    template <class TRequestMessage, class TResponseMessage, class TState>
    class TBatcherBase
        : public TRefCounted
    {
    public:
        using TResponse = TTypedClientResponse<TResponseMessage>;
        using TResponsePtr = TIntrusivePtr<TResponse>;
        using TRequest = TTypedClientRequest<TRequestMessage, TResponse>;
        using TRequestPtr = TIntrusivePtr<TRequest>;
        using TContext = TTypedServiceContext<TRequestMessage, TResponseMessage>;
        using TContextPtr = TIntrusivePtr<TContext>;

        explicit TBatcherBase(TBatchingChunkService* owner)
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

            if (context->IsRetry()) {
                THROW_ERROR_EXCEPTION("Retries are not supported by batcher");
            }

            TGuard<TSpinLock> guard(SpinLock_);

            if (!CurrentBatch_) {
                CurrentBatch_ = New<TBatch>();

                auto request = CurrentBatch_->BatchRequest = CreateBatchRequest();
                GenerateMutationId(request);
                request->SetUser(NSecurityClient::JobUserName);
                request->SetTimeout(owner->ConnectionConfig_->RpcTimeout);

                TDelayedExecutor::Submit(
                    BIND(&TBatcherBase::OnTimeout, MakeStrong(this), CurrentBatch_),
                    owner->ServiceConfig_->MaxBatchDelay);
            }

            CurrentBatch_->ContextsWithStates.emplace_back(context, TState());
            auto& state = CurrentBatch_->ContextsWithStates.back().second;
            BatchRequest(&context->Request(), CurrentBatch_->BatchRequest.Get(), &state);

            LOG_DEBUG("Chunk Service request batched (RequestId: %v -> %v)",
                context->GetRequestId(),
                CurrentBatch_->BatchRequest->GetRequestId());

            if (GetCost(CurrentBatch_->BatchRequest) >= owner->ServiceConfig_->MaxBatchCost) {
                DoFlush();
            }
        }

    protected:
        const TWeakPtr<TBatchingChunkService> Owner_;
        const NLogging::TLogger Logger;

        struct TBatch
            : public TIntrinsicRefCounted
        {
            TRequestPtr BatchRequest;
            std::vector<std::pair<TContextPtr, TState>> ContextsWithStates;
        };

        using TBatchPtr = TIntrusivePtr<TBatch>;

        TChunkServiceProxy LeaderProxy_;
        TChunkServiceProxy FollowerProxy_;

        TSpinLock SpinLock_;
        TBatchPtr CurrentBatch_;


        virtual TRequestPtr CreateBatchRequest() = 0;
        virtual void BatchRequest(
            const TRequestMessage* request,
            TRequestMessage* batchRequest,
            TState* state) = 0;
        virtual void UnbatchResponse(
            TResponseMessage* response,
            const TResponseMessage* batchResponse,
            const TState& state) = 0;
        virtual int GetCost(const TRequestPtr& request) const = 0;


        template <class T>
        static void BatchSubrequests(
            const ::google::protobuf::RepeatedPtrField<T>& src,
            ::google::protobuf::RepeatedPtrField<T>* dst,
            std::vector<int>* indexes)
        {
            for (const auto& subrequest : src) {
                int index = dst->size();
                indexes->push_back(index);
                *dst->Add() = subrequest;
            }
        }

        template <class T>
        static void UnbatchSubresponses(
            const ::google::protobuf::RepeatedPtrField<T>& src,
            ::google::protobuf::RepeatedPtrField<T>* dst,
            const std::vector<int>& indexes)
        {
            for (int index : indexes) {
                *dst->Add() = src.Get(index);
            }
        }

        template <class TBatchResponse, class TResponse>
        static void BuildResponseNodeDirectory(const TBatchResponse* batchResponse, TResponse* response)
        {
            auto nodeDirectory = New<TNodeDirectory>();
            nodeDirectory->MergeFrom(batchResponse->node_directory());
            TNodeDirectoryBuilder builder(nodeDirectory, response->mutable_node_directory());
            for (const auto& subresponse : response->subresponses()) {
                builder.Add(FromProto<TChunkReplicaList>(subresponse.replicas()));
            }
        }

    private:
        void OnTimeout(const TBatchPtr& batch)
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (CurrentBatch_ == batch) {
                DoFlush();
            }
        }

        void DoFlush()
        {
            VERIFY_SPINLOCK_AFFINITY(SpinLock_);

            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            TBatchPtr batch;
            std::swap(batch, CurrentBatch_);

            auto cost = GetCost(batch->BatchRequest);
            owner->CostThrottler_->Throttle(cost)
                .Subscribe(BIND(&TBatcherBase::DoSendBatch, MakeStrong(this), batch)
                    .Via(owner->GetDefaultInvoker()));
        }

        void DoSendBatch(const TBatchPtr& batch, const TError& /*error*/)
        {
            auto owner = Owner_.Lock();
            if (!owner) {
                return;
            }

            LOG_DEBUG("Chunk Service batch request sent (RequestId: %v)",
                batch->BatchRequest->GetRequestId());

            batch->BatchRequest->Invoke().Subscribe(
                BIND(&TBatcherBase::OnBatchResponse, MakeStrong(this), batch)
                    .Via(owner->GetDefaultInvoker()));
        }

        void OnBatchResponse(const TBatchPtr& batch, const TErrorOr<TResponsePtr>& responseOrError)
        {
            if (responseOrError.IsOK()) {
                LOG_DEBUG("Chunk Service batch request succeeded (RequestId: %v)",
                    batch->BatchRequest->GetRequestId());
            } else {
                LOG_DEBUG(responseOrError, "Chunk Service batch request failed (RequestId: %v)",
                    batch->BatchRequest->GetRequestId());
            }

            for (const auto& pair : batch->ContextsWithStates) {
                const auto& context = pair.first;
                const auto& state = pair.second;
                if (responseOrError.IsOK()) {
                    UnbatchResponse(&context->Response(), responseOrError.Value().Get(), state);
                    context->Reply();
                } else {
                    context->Reply(responseOrError);
                }
            }
        }
    };


    struct TLocateChunksState
    {
        std::vector<int> Indexes;
    };

    class TLocateChunksBatcher
        : public TBatcherBase<
            NChunkClient::NProto::TReqLocateChunks,
            NChunkClient::NProto::TRspLocateChunks,
            TLocateChunksState>
    {
    public:
        explicit TLocateChunksBatcher(TBatchingChunkService* owner)
            : TBatcherBase(owner)
        { }

    protected:
        virtual TChunkServiceProxy::TReqLocateChunksPtr CreateBatchRequest() override
        {
            auto req = FollowerProxy_.LocateChunks();
            req->SetHeavy(true);
            return req;
        }

        virtual void BatchRequest(
            const NChunkClient::NProto::TReqLocateChunks* request,
            NChunkClient::NProto::TReqLocateChunks* batchRequest,
            TLocateChunksState* state) override
        {
            BatchSubrequests(request->subrequests(), batchRequest->mutable_subrequests(), &state->Indexes);
        }

        virtual void UnbatchResponse(
            NChunkClient::NProto::TRspLocateChunks* response,
            const NChunkClient::NProto::TRspLocateChunks* batchResponse,
            const TLocateChunksState& state) override
        {
            UnbatchSubresponses(batchResponse->subresponses(), response->mutable_subresponses(), state.Indexes);
            BuildResponseNodeDirectory(batchResponse, response);
        }

        virtual int GetCost(const TRequestPtr& request) const override
        {
            return request->subrequests_size();
        }
    };

    const TIntrusivePtr<TLocateChunksBatcher> LocateChunksBatcher_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, LocateChunks)
    {
        LocateChunksBatcher_->HandleRequest(context);
    }


    struct TAllocateWriteTargetsState
    {
        std::vector<int> Indexes;
    };

    class TAllocateWriteTargetsBatcher
        : public TBatcherBase<
            NChunkClient::NProto::TReqAllocateWriteTargets,
            NChunkClient::NProto::TRspAllocateWriteTargets,
            TAllocateWriteTargetsState>
    {
    public:
        explicit TAllocateWriteTargetsBatcher(TBatchingChunkService* owner)
            : TBatcherBase(owner)
        { }

    protected:
        virtual TChunkServiceProxy::TReqAllocateWriteTargetsPtr CreateBatchRequest() override
        {
            return LeaderProxy_.AllocateWriteTargets();
        }

        virtual void BatchRequest(
            const NChunkClient::NProto::TReqAllocateWriteTargets* request,
            NChunkClient::NProto::TReqAllocateWriteTargets* batchRequest,
            TAllocateWriteTargetsState* state) override
        {
            BatchSubrequests(request->subrequests(), batchRequest->mutable_subrequests(), &state->Indexes);
        }

        virtual void UnbatchResponse(
            NChunkClient::NProto::TRspAllocateWriteTargets* response,
            const NChunkClient::NProto::TRspAllocateWriteTargets* batchResponse,
            const TAllocateWriteTargetsState& state) override
        {
            UnbatchSubresponses(batchResponse->subresponses(), response->mutable_subresponses(), state.Indexes);
            BuildResponseNodeDirectory(batchResponse, response);
        }

        virtual int GetCost(const TRequestPtr& request) const override
        {
            return request->subrequests_size();
        }
    };

    const TIntrusivePtr<TAllocateWriteTargetsBatcher> AllocateWriteTargetsBatcher_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, AllocateWriteTargets)
    {
        AllocateWriteTargetsBatcher_->HandleRequest(context);
    }



    struct TExecuteBatchState
    {
        std::vector<int> CreateIndexes;
        std::vector<int> ConfirmIndexes;
        std::vector<int> SealIndexes;
        std::vector<int> AttachIndexes;
    };

    class TExecuteBatchBatcher
        : public TBatcherBase<
            NChunkClient::NProto::TReqExecuteBatch,
            NChunkClient::NProto::TRspExecuteBatch,
            TExecuteBatchState>
    {
    public:
        explicit TExecuteBatchBatcher(TBatchingChunkService* owner)
            : TBatcherBase(owner)
        { }

    protected:
        virtual TChunkServiceProxy::TReqExecuteBatchPtr CreateBatchRequest() override
        {
            return LeaderProxy_.ExecuteBatch();
        }

        virtual void BatchRequest(
            const NChunkClient::NProto::TReqExecuteBatch* request,
            NChunkClient::NProto::TReqExecuteBatch* batchRequest,
            TExecuteBatchState* state) override
        {
            BatchSubrequests(request->create_chunk_subrequests(), batchRequest->mutable_create_chunk_subrequests(), &state->CreateIndexes);
            BatchSubrequests(request->confirm_chunk_subrequests(), batchRequest->mutable_confirm_chunk_subrequests(), &state->ConfirmIndexes);
            BatchSubrequests(request->seal_chunk_subrequests(), batchRequest->mutable_seal_chunk_subrequests(), &state->SealIndexes);
            BatchSubrequests(request->attach_chunk_trees_subrequests(), batchRequest->mutable_attach_chunk_trees_subrequests(), &state->AttachIndexes);
        }

        virtual void UnbatchResponse(
            NChunkClient::NProto::TRspExecuteBatch* response,
            const NChunkClient::NProto::TRspExecuteBatch* batchResponse,
            const TExecuteBatchState& state) override
        {
            UnbatchSubresponses(batchResponse->create_chunk_subresponses(), response->mutable_create_chunk_subresponses(), state.CreateIndexes);
            UnbatchSubresponses(batchResponse->confirm_chunk_subresponses(), response->mutable_confirm_chunk_subresponses(), state.ConfirmIndexes);
            UnbatchSubresponses(batchResponse->seal_chunk_subresponses(), response->mutable_seal_chunk_subresponses(), state.SealIndexes);
            UnbatchSubresponses(batchResponse->attach_chunk_trees_subresponses(), response->mutable_attach_chunk_trees_subresponses(), state.AttachIndexes);
        }

        virtual int GetCost(const TRequestPtr& request) const override
        {
            return
                request->create_chunk_subrequests_size() +
                request->confirm_chunk_subrequests_size() +
                request->seal_chunk_subrequests_size() +
                request->attach_chunk_trees_subrequests_size();
        }
    };

    const TIntrusivePtr<TExecuteBatchBatcher> ExecuteBatchBatcher_;

    DECLARE_RPC_SERVICE_METHOD(NChunkClient::NProto, ExecuteBatch)
    {
        YCHECK(request->create_chunk_lists_subrequests_size() == 0);
        YCHECK(request->unstage_chunk_tree_subrequests_size() == 0);
        ExecuteBatchBatcher_->HandleRequest(context);
    }
};

IServicePtr CreateBatchingChunkService(
    const TCellId& cellId,
    TBatchingChunkServiceConfigPtr serviceConfig,
    TMasterConnectionConfigPtr connectionConfig,
    IChannelFactoryPtr channelFactory)
{
    return New<TBatchingChunkService>(
        cellId,
        serviceConfig,
        connectionConfig,
        channelFactory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
