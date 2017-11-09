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

#include <yt/core/profiling/timing.h>
#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/crash_handler.h>

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

static const auto& Profiler = ObjectServerProfiler;
static NProfiling::TSimpleCounter CumulativeReadRequestTimeCounter("/cumulative_read_request_time");
static NProfiling::TSimpleCounter CumulativeMutationScheduleTimeCounter("/cumulative_mutation_schedule_time");
static NProfiling::TSimpleCounter ReadRequestCounter("/read_request_count");
static NProfiling::TSimpleCounter WriteRequestCounter("/write_request_count");

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
            NObjectClient::TObjectServiceProxy::GetDescriptor(),
            // Execute method is being handled in RPC thread pool anyway.
            EAutomatonThreadQueue::Default,
            ObjectServerLogger)
        , Config_(std::move(config))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetMaxQueueSize(10000)
            .SetMaxConcurrency(10000)
            .SetCancelable(true)
            .SetInvoker(NRpc::TDispatcher::Get()->GetHeavyInvoker()));
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
        , CodicilData_(Format("RequestId: %v, User: %v",
            RequestId_,
            UserName_))
    { }

    ~TExecuteSession()
    {
        if (RequestQueueSizeIncreased_) {
            // NB: DoDecreaseRequestQueueSize must be static since the session instance is dying.
            EpochAutomatonInvoker_->Invoke(
                BIND(&TExecuteSession::DoDecreaseRequestQueueSize,
                SecurityManager_,
                UserName_));
        }
    }

    void Run()
    {
        auto codicilGuard = MakeCodicilGuard();

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
            ->GetGuardedAutomatonInvoker(EAutomatonThreadQueue::ObjectService)
            ->Invoke(BIND(&TExecuteSession::Continue, MakeStrong(this)));
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr Context_;

    const int SubrequestCount_;
    const TString UserName_;
    const TRequestId RequestId_;
    const THydraFacadePtr HydraFacade_;
    const IHydraManagerPtr HydraManager_;
    const TObjectManagerPtr ObjectManager_;
    const TSecurityManagerPtr SecurityManager_;
    const TResponseKeeperPtr ResponseKeeper_;
    const TString CodicilData_;

    struct TSubrequest
    {
        IServiceContextPtr Context;
        TFuture<TSharedRefArray> AsyncResponseMessage;
        std::unique_ptr<TMutation> Mutation;
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
    bool RequestQueueSizeIncreased_ = false;

    std::atomic<bool> Replied_ = {false};
    std::atomic<int> SubresponseCount_ = {0};
    int LastMutatingSubrequestIndex_ = -1;

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

            auto loggingInfo = Format("RequestId: %v, Mutating: %v, RequestPath: %v, User: %v",
                RequestId_,
                mutating,
                path,
                UserName_);
            auto subcontext = CreateYPathContext(
                updatedSubrequestMessage,
                ObjectServerLogger,
                NLogging::ELogLevel::Debug,
                std::move(loggingInfo));

            subrequest.RequestMessage = updatedSubrequestMessage;
            subrequest.Context = subcontext;
            subrequest.AsyncResponseMessage = subcontext->GetAsyncResponseMessage();
            subrequest.TraceContext = NTracing::CreateChildTraceContext();
            if (mutating) {
                subrequest.Mutation = ObjectManager_->CreateExecuteMutation(UserName_, subcontext);
                subrequest.Mutation->SetMutationId(subcontext->GetMutationId(), subcontext->IsRetry());
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
        auto codicilGuard = MakeCodicilGuard();
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

        if (Context_->IsCanceled()) {
            return;
        }

        if (!EpochAutomatonInvoker_) {
            EpochAutomatonInvoker_ = HydraFacade_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ObjectService);
        }

        if (NeedsUpstreamSync_) {
            NeedsUpstreamSync_ = false;
            auto result = HydraManager_->SyncWithUpstream();
            if (!WaitForAndContinue(std::move(result))) {
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

        if (!RequestQueueSizeIncreased_) {
            if (!SecurityManager_->TryIncreaseRequestQueueSize(user)) {
                THROW_ERROR_EXCEPTION(
                    NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded,
                    "User %Qv has exceeded its request queue size limit",
                    user->GetName())
                    << TErrorAttribute("limit", user->GetRequestQueueSizeLimit());
            }
            RequestQueueSizeIncreased_ = true;
        }

        while (CurrentSubrequestIndex_ < SubrequestCount_) {
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
                ExecuteWriteSubrequest(&subrequest, user);
                LastMutatingSubrequestIndex_ = CurrentSubrequestIndex_;
            } else {
                // Cannot serve new read requests before previous write ones are done.
                if (LastMutatingSubrequestIndex_ >= 0) {
                    auto& lastCommitResult = Subrequests_[LastMutatingSubrequestIndex_].AsyncResponseMessage;
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

    void ExecuteWriteSubrequest(TSubrequest* subrequest, TUser* user)
    {
        Profiler.Increment(WriteRequestCounter);
        NProfiling::TProfilingTimingGuard timingGuard(Profiler, &CumulativeMutationScheduleTimeCounter);

        subrequest->Mutation->Commit().Subscribe(
            BIND(&TExecuteSession::OnMutationCommitted, MakeStrong(this), subrequest));
    }

    void ExecuteReadSubrequest(TSubrequest* subrequest, TUser* user)
    {
        Profiler.Increment(ReadRequestCounter);
        NProfiling::TProfilingTimingGuard timingGuard(Profiler, &CumulativeReadRequestTimeCounter);

        TAuthenticatedUserGuard userGuard(SecurityManager_, user);

        NTracing::TTraceContextGuard traceContextGuard(subrequest->TraceContext);

        NProfiling::TWallTimer timer;

        const auto& context = subrequest->Context;
        try {
            auto rootService = ObjectManager_->GetRootService();
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
            SecurityManager_->ChargeUserRead(user, 1, timer.GetElapsedTime());
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
            ->GetHeavyInvoker()
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

    static void DoDecreaseRequestQueueSize(
        const TSecurityManagerPtr& securityManager,
        const TString& userName)
    {
        auto* user = securityManager->FindUserByName(userName);
        if (user) {
            securityManager->DecreaseRequestQueueSize(user);
        }
    }

    TCodicilGuard MakeCodicilGuard()
    {
        return TCodicilGuard(CodicilData_);
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    Y_UNUSED(request);
    Y_UNUSED(response);

    New<TExecuteSession>(this, context)->Run();
}

DEFINE_RPC_SERVICE_METHOD(TObjectService, GCCollect)
{
    Y_UNUSED(request);
    Y_UNUSED(response);

    context->SetRequestInfo();

    ValidateClusterInitialized();
    ValidatePeer(EPeerKind::Leader);

    const auto& objectManager = Bootstrap_->GetObjectManager();
    context->ReplyFrom(objectManager->GCCollect());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT
