#include "object_service.h"
#include "private.h"
#include "object_manager.h"
#include "config.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>
#include <yt/server/cell_master/master_hydra_service.h>

#include <yt/server/cypress_server/cypress_manager.h>

#include <yt/server/security_server/security_manager.h>
#include <yt/server/security_server/user.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/dispatcher.h>
#include <yt/core/rpc/response_keeper.h>

#include <yt/core/ytree/ypath_detail.h>

#include <yt/core/profiling/scoped_timer.h>

#include <atomic>

namespace NYT {
namespace NObjectServer {

using namespace NHydra;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NBus;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TObjectService)

class TObjectService
    : public NCellMaster::TMasterHydraServiceBase
{
public:
    TObjectService(
        TObjectServiceConfigPtr config,
        TBootstrap* bootstrap)
        : TMasterHydraServiceBase(
            bootstrap,
            NObjectClient::TObjectServiceProxy::GetServiceName(),
            ObjectServerLogger,
            NObjectClient::TObjectServiceProxy::GetProtocolVersion())
        , Config_(std::move(config))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetMaxQueueSize(10000)
            .SetMaxConcurrency(10000)
            .SetInvoker(NRpc::TDispatcher::Get()->GetInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));
    }

private:
    const TObjectServiceConfigPtr Config_;

    class TExecuteSession;

    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, GCCollect);

};

DEFINE_REFCOUNTED_TYPE(TObjectService)

IServicePtr CreateObjectService(
    TObjectServiceConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TObjectService>(
        std::move(config),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TObjectService::TExecuteSession
    : public TIntrinsicRefCounted
{
public:
    TExecuteSession(
        TObjectServicePtr owner,
        TCtxExecutePtr context)
        : Owner_(std::move(owner))
        , Context_(std::move(context))
        , SubrequestCount_(Context_->Request().part_counts_size())
        , UserName_(Context_->GetUser())
        , RequestId_(Context_->GetRequestId())
        , HydraFacade_(Owner_->Bootstrap_->GetHydraFacade())
        , HydraManager_(HydraFacade_->GetHydraManager())
        , ObjectManager_(Owner_->Bootstrap_->GetObjectManager())
        , SecurityManager_(Owner_->Bootstrap_->GetSecurityManager())
        , ResponseKeeper_(HydraFacade_->GetResponseKeeper())
    { }

    void Run()
    {
        Context_->SetRequestInfo("Count: %v", SubrequestCount_);

        if (SubrequestCount_ == 0) {
            Reply();
            return;
        }

        try {
            ParseSubrequests();
        } catch (const std::exception& ex) {
            Context_->Reply(ex);
            return;
        }

        HydraFacade_
            ->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::RpcService)
            ->Invoke(BIND(&TExecuteSession::Continue, MakeStrong(this)));
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr Context_;

    const int SubrequestCount_;
    const Stroka UserName_;
    const TRequestId RequestId_;
    const THydraFacadePtr HydraFacade_;
    const IHydraManagerPtr HydraManager_;
    const TObjectManagerPtr ObjectManager_;
    const TSecurityManagerPtr SecurityManager_;
    const TResponseKeeperPtr ResponseKeeper_;

    struct TSubrequest
    {
        IServiceContextPtr Context;
        TFuture<TSharedRefArray> AsyncResponseMessage;
        TMutationPtr Mutation;
        TRequestHeader RequestHeader;
        TSharedRefArray RequestMessage;
        NTracing::TTraceContext TraceContext;
    };

    std::vector<TSubrequest> Subrequests_;
    int CurrentSubrequestIndex_ = 0;
    int ThrottledSubrequestIndex_ = -1;
    IInvokerPtr EpochAutomatonInvoker_;
    bool NeedsUpstreamSync_ = true;
    bool NeedsUserAccessValidation_ = true;

    std::atomic<bool> Replied_ = {false};
    std::atomic<int> SubresponseCount_ = {0};
    int LastMutatingSubRequestIndex_ = -1;

    const NLogging::TLogger& Logger = ObjectServerLogger;


    void ParseSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& request = Context_->Request();
        const auto& attachments = Context_->RequestAttachments();
        Subrequests_.resize(SubrequestCount_);
        int currentPartIndex = 0;
        for (int subrequestIndex = 0; subrequestIndex < SubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            int partCount = request.part_counts(subrequestIndex);
            if (partCount == 0) {
                // Empty subrequest.
                continue;
            }

            std::vector<TSharedRef> subrequestParts(
                attachments.begin() + currentPartIndex,
                attachments.begin() + currentPartIndex + partCount);
            currentPartIndex += partCount;

            auto& subrequestHeader = subrequest.RequestHeader;
            TSharedRefArray subrequestMessage(std::move(subrequestParts));
            if (!ParseRequestHeader(subrequestMessage, &subrequestHeader)) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Error parsing subrequest header");
            }

            // Propagate various parameters to the subrequest.
            ToProto(subrequestHeader.mutable_request_id(), RequestId_);
            subrequestHeader.set_retry(subrequestHeader.retry() || Context_->IsRetry());
            subrequestHeader.set_user(UserName_);

            auto updatedSubrequestMessage = SetRequestHeader(subrequestMessage, subrequestHeader);

            const auto& ypathExt = subrequestHeader.GetExtension(TYPathHeaderExt::ypath_header_ext);
            const auto& path = ypathExt.path();
            bool mutating = ypathExt.mutating();

            auto requestInfo = Format("RequestId: %v, Mutating: %v, RequestPath: %v, User: %v",
                RequestId_,
                mutating,
                path,
                UserName_);
            auto responseInfo = Format("RequestId: %v",
                RequestId_);

            auto subcontext = CreateYPathContext(
                updatedSubrequestMessage,
                ObjectServerLogger,
                NLogging::ELogLevel::Debug,
                requestInfo,
                responseInfo);

            subrequest.RequestMessage = updatedSubrequestMessage;
            subrequest.Context = subcontext;
            subrequest.AsyncResponseMessage = subcontext->GetAsyncResponseMessage();
            subrequest.TraceContext = NTracing::CreateChildTraceContext();
            if (mutating) {
                subrequest.Mutation = ObjectManager_->CreateExecuteMutation(UserName_, subcontext)
                    ->SetMutationId(GetMutationId(subcontext), subcontext->IsRetry());
            }
        }

        NeedsUpstreamSync_ = !request.suppress_upstream_sync();
    }

    template <class T>
    void CheckAndContinue(const TErrorOr<T>& result)
    {
        if (!result.IsOK()) {
            Reply(result);
            return;
        }
        Continue();
    }

    void Continue()
    {
        try {
            GuardedContinue();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    bool WaitForAndContinue(TFuture<void> result)
    {
        if (result.IsSet()) {
            result
                .Get()
                .ThrowOnError();
            return true;
        } else {
            result.Subscribe(BIND(&TExecuteSession::CheckAndContinue<void>, MakeStrong(this))
                .Via(EpochAutomatonInvoker_));
            return false;
        }
    }

    void GuardedContinue()
    {
        auto batchStartTime = NProfiling::GetCpuInstant();
        auto batchDeadlineTime = batchStartTime + NProfiling::DurationToCpuDuration(Owner_->Config_->YieldTimeout);

        if (!EpochAutomatonInvoker_) {
            EpochAutomatonInvoker_ = HydraFacade_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::RpcService);
        }

        if (NeedsUpstreamSync_) {
            NeedsUpstreamSync_ = false;
            auto result = HydraManager_->SyncWithUpstream();
            if (!WaitForAndContinue(result)) {
                return;
            }
        }

        Owner_->ValidateClusterInitialized();
        HydraManager_->ValidatePeer(EPeerKind::LeaderOrFollower);

        auto* user = SecurityManager_->GetUserByNameOrThrow(UserName_);

        if (NeedsUserAccessValidation_) {
            NeedsUserAccessValidation_ = false;
            SecurityManager_->ValidateUserAccess(user);
        }

        while (CurrentSubrequestIndex_ < SubrequestCount_ ) {
            while (CurrentSubrequestIndex_ > ThrottledSubrequestIndex_) {
                ++ThrottledSubrequestIndex_;
                auto result = SecurityManager_->ThrottleUser(user, 1);
                if (!WaitForAndContinue(result)) {
                    return;
                }
            }

            auto& subrequest = Subrequests_[CurrentSubrequestIndex_];
            if (!subrequest.Context) {
                ExecuteEmptySubrequest(&subrequest, user);
                continue;
            }

            NTracing::TraceEvent(
                subrequest.TraceContext,
                subrequest.RequestHeader.service(),
                subrequest.RequestHeader.method(),
                NTracing::ServerReceiveAnnotation);

            if (subrequest.Mutation) {
                ExecuteMutatingSubrequest(&subrequest, user);
                LastMutatingSubRequestIndex_ = CurrentSubrequestIndex_;
            } else {
                // Cannot serve new read requests before previous write ones are done.
                if (LastMutatingSubRequestIndex_ >= 0) {
                    auto& lastCommitResult = Subrequests_[LastMutatingSubRequestIndex_].AsyncResponseMessage;
                    if (!lastCommitResult.IsSet()) {
                        lastCommitResult.Subscribe(
                            BIND(&TExecuteSession::CheckAndContinue<TSharedRefArray>, MakeStrong(this))
                                .Via(EpochAutomatonInvoker_));
                        return;
                    }
                }
                ExecuteReadSubrequest(&subrequest, user);
            }

            // Optimize for the (typical) case of synchronous response.
            auto& asyncResponseMessage = subrequest.AsyncResponseMessage;
            if (asyncResponseMessage.IsSet()) {
                OnSubresponse(&subrequest, asyncResponseMessage.Get());
            } else {
                asyncResponseMessage.Subscribe(
                    BIND(&TExecuteSession::OnSubresponse<TSharedRefArray>, MakeStrong(this), &subrequest));
            }

            ++CurrentSubrequestIndex_;

            if (NProfiling::GetCpuInstant() > batchDeadlineTime) {
                LOG_DEBUG("Yielding automaton thread");
                EpochAutomatonInvoker_->Invoke(BIND(&TExecuteSession::Continue, MakeStrong(this)));
                break;
            }
        }
    }

    void ExecuteEmptySubrequest(TSubrequest* subrequest, TUser* /*user*/)
    {
        OnSubresponse(subrequest, TError());
    }

    void ExecuteMutatingSubrequest(TSubrequest* subrequest, TUser* user)
    {
        subrequest->Mutation->Commit().Subscribe(
            BIND(&TExecuteSession::OnMutationCommitted, MakeStrong(this), subrequest));
    }

    void ExecuteReadSubrequest(TSubrequest* subrequest, TUser* user)
    {
        const auto& context = subrequest->Context;
        auto asyncResponseMessage = context->GetAsyncResponseMessage();
        NProfiling::TScopedTimer timer;
        try {
            auto rootService = ObjectManager_->GetRootService();
            TAuthenticatedUserGuard userGuard(SecurityManager_, user);
            NTracing::TTraceContextGuard traceContextGuard(subrequest->TraceContext);
            ExecuteVerb(rootService, context);
        } catch (const TLeaderFallbackException&) {
            LOG_DEBUG("Performing leader fallback (RequestId: %v)",
                RequestId_);
            context->ReplyFrom(ObjectManager_->ForwardToLeader(
                Owner_->Bootstrap_->GetCellTag(),
                subrequest->RequestMessage,
                Context_->GetTimeout()));
        }

        // NB: Even if the user was just removed the instance is still valid but not alive.
        if (IsObjectAlive(user)) {
            SecurityManager_->ChargeUser(user, 1, timer.GetElapsed(), TDuration());
        }
    }

    void OnMutationCommitted(TSubrequest* subrequest, const TErrorOr<TMutationResponse>& responseOrError)
    {
        if (!responseOrError.IsOK()) {
            Reply(responseOrError);
            return;
        }

        // Here the context is typically already replied.
        // A notable exception is when the mutation response comes from Response Keeper.
        const auto& context = subrequest->Context;
        if (!context->IsReplied()) {
            context->Reply(responseOrError.Value().Data);
        }
    }

    template <class T>
    void OnSubresponse(TSubrequest* subrequest, const TErrorOr<T>& result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!result.IsOK()) {
            Reply(result);
            return;
        }

        if (subrequest->Context) {
            NTracing::TraceEvent(
                subrequest->TraceContext,
                subrequest->RequestHeader.service(),
                subrequest->RequestHeader.method(),
                NTracing::ServerSendAnnotation);
        }

        if (++SubresponseCount_ == SubrequestCount_) {
            Reply();
        }
    }


    void Reply(const TError& error = TError())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        bool expected = false;
        if (!Replied_.compare_exchange_strong(expected, true)) {
            return;
        }

        NRpc::TDispatcher::Get()
            ->GetInvoker()
            ->Invoke(BIND(&TExecuteSession::DoReply, MakeStrong(this), error));
    }

    void DoReply(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (error.IsOK()) {
            auto& response = Context_->Response();
            auto& attachments = Context_->ResponseAttachments();
            for (const auto& subrequest : Subrequests_) {
                if (subrequest.Context) {
                    auto subresponseMessage = subrequest.Context->GetResponseMessage();
                    response.add_part_counts(subresponseMessage.Size());
                    attachments.insert(
                        attachments.end(),
                        subresponseMessage.Begin(),
                        subresponseMessage.End());
                } else {
                    response.add_part_counts(0);
                }
            }
        }
     
        Context_->Reply(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    UNUSED(request);
    UNUSED(response);

    New<TExecuteSession>(this, std::move(context))->Run();
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, GCCollect)
{
    UNUSED(request);
    UNUSED(response);

    context->SetRequestInfo();

    ValidateClusterInitialized();
    ValidatePeer(EPeerKind::Leader);

    auto objectManager = Bootstrap_->GetObjectManager();
    context->ReplyFrom(objectManager->GCCollect());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
