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

#include <yt/core/ytree/ypath_detail.h>

#include <yt/core/profiling/timing.h>
#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/crash_handler.h>
#include <yt/core/misc/heap.h>
#include <yt/core/misc/lock_free.h>

#include <yt/core/actions/cancelable_context.h>

#include <atomic>
#include <queue>

namespace NYT {
namespace NObjectServer {

using namespace NHydra;
using namespace NRpc;
using namespace NRpc::NProto;
using namespace NBus;
using namespace NYTree;
using namespace NYTree::NProto;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = ObjectServerProfiler;
static NProfiling::TMonotonicCounter CumulativeReadRequestTimeCounter("/cumulative_read_request_time");
static NProfiling::TMonotonicCounter CumulativeMutationScheduleTimeCounter("/cumulative_mutation_schedule_time");
static NProfiling::TMonotonicCounter ReadRequestCounter("/read_request_count");
static NProfiling::TMonotonicCounter WriteRequestCounter("/write_request_count");

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
            EAutomatonThreadQueue::ObjectService,
            ObjectServerLogger)
        , Config_(std::move(config))
        , AutomatonInvoker_(Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(EAutomatonThreadQueue::ObjectService))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetMaxQueueSize(10000)
            .SetMaxConcurrency(10000)
            .SetCancelable(true)
            .SetInvoker(GetRpcInvoker()));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->SubscribeUserCharged(BIND(&TObjectService::OnUserCharged, MakeStrong(this)));
    }

private:
    const TObjectServiceConfigPtr Config_;
    const IInvokerPtr AutomatonInvoker_;

    class TExecuteSession;
    using TExecuteSessionPtr = TIntrusivePtr<TExecuteSession>;

    struct TUserBucket
    {
        explicit TUserBucket(const TString& userName)
            : UserName(userName)
        { }

        TString UserName;
        TDuration ExcessTime;
        //! Typically equals ExcessTime; however when a user is charged we just update ExceesTime
        //! and leave HeapKey intact. Upon extracting heap's top we check if its ExcessTime matches its HeapKey
        //! and if not then readjust the heap.
        TDuration HeapKey;
        std::queue<TExecuteSessionPtr> Sessions;
        bool InHeap = false;
    };

    struct TUserBucketComparer
    {
        bool operator ()(TUserBucket* lhs, TUserBucket* rhs) const
        {
            return lhs->HeapKey < rhs->HeapKey;
        }
    };

    THashMap<TString, TUserBucket> NameToUserBucket_;
    TDuration ExcessBaseline_;

    //! Min-heap ordered by TUserBucket::ExcessTime.
    //! A bucket is only present here iff it has at least one session.
    std::vector<TUserBucket*> BucketHeap_;

    TMultipleProducerSingleConsumerLockFreeStack<TExecuteSessionPtr> EnqueuedSessions_;

    std::atomic<bool> RunSessionsCallbackEnqueued_ = {false};


    static IInvokerPtr GetRpcInvoker()
    {
        return NRpc::TDispatcher::Get()->GetHeavyInvoker();
    }

    void EnqueueSession(TExecuteSessionPtr session);
    void EnqueueRunSessionsCallback();
    void RunSessions();
    TUserBucket* GetOrCreateBucket(const TString& userName);
    void OnUserCharged(TUser* user, const TUserWorkload& workload);

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
        TCtxExecutePtr rpcContext)
        : Owner_(std::move(owner))
        , RpcContext_(std::move(rpcContext))
        , SubrequestCount_(RpcContext_->Request().part_counts_size())
        , UserName_(RpcContext_->GetUser())
        , RequestId_(RpcContext_->GetRequestId())
        , HydraFacade_(Owner_->Bootstrap_->GetHydraFacade())
        , HydraManager_(HydraFacade_->GetHydraManager())
        , ObjectManager_(Owner_->Bootstrap_->GetObjectManager())
        , SecurityManager_(Owner_->Bootstrap_->GetSecurityManager())
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

    const TString& GetUserName() const
    {
        return UserName_;
    }

    bool Prepare()
    {
        auto codicilGuard = MakeCodicilGuard();

        RpcContext_->SetRequestInfo("Count: %v", SubrequestCount_);

        if (SubrequestCount_ == 0) {
            Reply();
            return false;
        }

        if (IsBackoffAllowed()) {
            ScheduleBackoffAlarm();
        }

        try {
            ParseSubrequests();
        } catch (const std::exception& ex) {
            Reply(ex);
            return false;
        }

        return true;
    }

    bool RunFast()
    {
        if (!NeedsUpstreamSync_) {
            return true;
        }
        NeedsUpstreamSync_ = false;

        auto result = HydraManager_->SyncWithUpstream();
        auto maybeError = result.TryGet();
        if (!maybeError) {
            result.Subscribe(
                BIND(&TExecuteSession::CheckAndReschedule<void>, MakeStrong(this)));
            return false;
        }

        const auto& error = *maybeError;
        if (!error.IsOK()) {
            Reply(error);
            return false;
        }

        return true;
    }

    void RunSlow()
    {
        auto codicilGuard = MakeCodicilGuard();
        try {
            GuardedRun();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr RpcContext_;

    TDelayedExecutorCookie BackoffAlarmCookie_;

    const int SubrequestCount_;
    const TString UserName_;
    const TRequestId RequestId_;
    const THydraFacadePtr HydraFacade_;
    const IHydraManagerPtr HydraManager_;
    const TObjectManagerPtr ObjectManager_;
    const TSecurityManagerPtr SecurityManager_;
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
    TCancelableContextPtr EpochCancelableContext_;
    bool NeedsUpstreamSync_ = true;
    bool NeedsUserAccessValidation_ = true;
    bool RequestQueueSizeIncreased_ = false;

    std::atomic<bool> Replied_ = {false};
    std::atomic<int> SubresponseCount_ = {0};
    int LastMutatingSubrequestIndex_ = -1;

    // Has the time to backoff come?
    std::atomic<bool> BackoffAlarmTriggered_ = {false};

    // If this is locked, the automaton invoker is currently busy serving
    // CurrentSubrequestIndex_-th subrequest (at which it may or may not succeed).
    // NB: only TryAcquire() is called on this lock, never Acquire().
    TSpinLock CurrentSubrequestLock_;

    std::vector<i64> Revisions_;

    const NLogging::TLogger& Logger = ObjectServerLogger;


    void ParseSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TTryGuard<TSpinLock> guard(CurrentSubrequestLock_);
        YCHECK(guard.WasAcquired());

        const auto& request = RpcContext_->Request();
        const auto& attachments = RpcContext_->RequestAttachments();
        Subrequests_.resize(SubrequestCount_);
        Revisions_.resize(SubrequestCount_);
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
            subrequestHeader.set_retry(subrequestHeader.retry() || RpcContext_->IsRetry());
            subrequestHeader.set_user(UserName_);

            auto* ypathExt = subrequestHeader.MutableExtension(TYPathHeaderExt::ypath_header_ext);
            const auto& path = ypathExt->path();
            bool mutating = ypathExt->mutating();

            // COMPAT(savrus) Support old mount/unmoun/etc interface.
            if (mutating && (subrequestHeader.method() == "Mount" ||
                subrequestHeader.method() == "Unmount" ||
                subrequestHeader.method() == "Freeze" ||
                subrequestHeader.method() == "Unfreeze" ||
                subrequestHeader.method() == "Remount" ||
                subrequestHeader.method() == "Reshard"))
            {
                mutating = false;
                ypathExt->set_mutating(false);
                SetSuppressAccessTracking(&subrequestHeader, true);
            }

            auto updatedSubrequestMessage = SetRequestHeader(subrequestMessage, subrequestHeader);

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

        // The backoff alarm may've been triggered while we were parsing.
        CheckBackoffAlarmTriggered();
    }

    void Reschedule()
    {
        Owner_->EnqueueSession(this);
    }

    template <class T>
    void CheckAndReschedule(const TErrorOr<T>& result)
    {
        if (!result.IsOK()) {
            Reply(result);
            return;
        }
        Reschedule();
    }

    bool WaitForAndContinue(TFuture<void> result)
    {
        if (result.IsSet()) {
            result
                .Get()
                .ThrowOnError();
            return true;
        } else {
            result.Subscribe(
                BIND(&TExecuteSession::CheckAndReschedule<void>, MakeStrong(this)));
            return false;
        }
    }

    void GuardedRun()
    {
        auto batchStartTime = NProfiling::GetCpuInstant();
        auto batchDeadlineTime = batchStartTime + NProfiling::DurationToCpuDuration(Owner_->Config_->YieldTimeout);

        if (!EpochAutomatonInvoker_) {
            EpochAutomatonInvoker_ = HydraFacade_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ObjectService);
        }
        if (!EpochCancelableContext_) {
            EpochCancelableContext_ = HydraFacade_->GetHydraManager()->GetAutomatonCancelableContext();
        }

        if (RpcContext_->IsCanceled()) {
            return;
        }
        if (EpochCancelableContext_->IsCanceled()) {
            return;
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

        while (CurrentSubrequestIndex_ < SubrequestCount_ &&
            !BackoffAlarmTriggered_ &&
            !Replied_)
        {
            while (CurrentSubrequestIndex_ > ThrottledSubrequestIndex_) {
                ++ThrottledSubrequestIndex_;
                auto result = SecurityManager_->ThrottleUser(user, 1);
                if (!WaitForAndContinue(result)) {
                    return;
                }
            }

            {
                TTryGuard<TSpinLock> guard(CurrentSubrequestLock_);
                if (!guard.WasAcquired()) {
                    Reschedule();
                    break;
                }

                if (Replied_) {
                    break;
                }

                if (!ExecuteCurrentSubrequest(user)) {
                    break;
                }
            }

            if (NProfiling::GetCpuInstant() > batchDeadlineTime) {
                LOG_DEBUG("Yielding automaton thread");
                Reschedule();
                break;
            }
        }
    }

    bool ExecuteCurrentSubrequest(TUser* user)
    {
        // NB: CurrentSubrequestIndex_ must be incremented before OnSubresponse() is called.

        auto& subrequest = Subrequests_[CurrentSubrequestIndex_];
        if (!subrequest.Context) {
            ++CurrentSubrequestIndex_;
            ExecuteEmptySubrequest(&subrequest, user);
            return true;
        }

        NTracing::TraceEvent(
            subrequest.TraceContext,
            subrequest.RequestHeader.service(),
            subrequest.RequestHeader.method(),
            NTracing::ServerReceiveAnnotation);

        Revisions_[CurrentSubrequestIndex_] = HydraFacade_->GetHydraManager()->GetAutomatonVersion().ToRevision();

        if (subrequest.Mutation) {
            ExecuteWriteSubrequest(&subrequest, user);
            LastMutatingSubrequestIndex_ = CurrentSubrequestIndex_;
        } else {
            // Cannot serve new read requests before previous write ones are done.
            if (LastMutatingSubrequestIndex_ >= 0) {
                auto& lastCommitResult = Subrequests_[LastMutatingSubrequestIndex_].AsyncResponseMessage;
                if (!lastCommitResult.IsSet()) {
                    lastCommitResult.Subscribe(
                        BIND(&TExecuteSession::CheckAndReschedule<TSharedRefArray>, MakeStrong(this)));
                    return false;
                }
            }
            ExecuteReadSubrequest(&subrequest, user);
        }

        ++CurrentSubrequestIndex_;

        // Optimize for the (typical) case of synchronous response.
        auto& asyncResponseMessage = subrequest.AsyncResponseMessage;
        if (asyncResponseMessage.IsSet()) {
            OnSubresponse(&subrequest, asyncResponseMessage.Get());
        } else {
            asyncResponseMessage.Subscribe(
                BIND(&TExecuteSession::OnSubresponse<TSharedRefArray>, MakeStrong(this), &subrequest));
        }

        return true;
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
                RpcContext_->GetTimeout()));
        }

        // NB: Even if the user was just removed the instance is still valid but not alive.
        if (IsObjectAlive(user)) {
            SecurityManager_->ChargeUser(user, {EUserWorkloadType::Read, 1, timer.GetElapsedTime()});
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
        } else {
            CheckBackoffAlarmTriggered();
        }
    }


    void Reply(const TError& error = TError())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        bool expected = false;
        if (!Replied_.compare_exchange_strong(expected, true)) {
            return;
        }

        TObjectService::GetRpcInvoker()
            ->Invoke(BIND(&TExecuteSession::DoReply, MakeStrong(this), error));
    }

    void DoReply(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TDelayedExecutor::CancelAndClear(BackoffAlarmCookie_);

        if (RpcContext_->IsCanceled()) {
            return;
        }

        if (error.IsOK()) {
            auto& response = RpcContext_->Response();
            auto& attachments = RpcContext_->ResponseAttachments();

            YCHECK(SubrequestCount_ == 0 || CurrentSubrequestIndex_ != 0);

            for (auto i = 0; i < CurrentSubrequestIndex_; ++i) {
                const auto& subrequest = Subrequests_[i];
                if (subrequest.Context) {
                    YCHECK(subrequest.Context->IsReplied());

                    auto subresponseMessage = subrequest.Context->GetResponseMessage();
                    response.add_part_counts(subresponseMessage.Size());
                    attachments.insert(
                        attachments.end(),
                        subresponseMessage.Begin(),
                        subresponseMessage.End());
                } else {
                    response.add_part_counts(0);
                }

                response.add_revisions(Revisions_[i]);
            }
        }

        YCHECK(!error.IsOK() ||
            SubrequestCount_ == 0 ||
            RpcContext_->Response().part_counts_size() > 0);

        RpcContext_->Reply(error);
    }

    bool IsBackoffAllowed() const
    {
        return RpcContext_->Request().allow_backoff();
    }

    void ScheduleBackoffAlarm()
    {
        auto requestTimeout = RpcContext_->GetTimeout();
        if (requestTimeout &&
            *requestTimeout > Owner_->Config_->TimeoutBackoffLeadTime)
        {
            auto backoffDelay = *requestTimeout - Owner_->Config_->TimeoutBackoffLeadTime;
            BackoffAlarmCookie_ = TDelayedExecutor::Submit(
                BIND(&TObjectService::TExecuteSession::OnBackoffAlarm, MakeStrong(this))
                    .Via(TObjectService::GetRpcInvoker()),
                backoffDelay);
        }
    }

    void OnBackoffAlarm()
    {
        LOG_DEBUG("Backoff alarm triggered (RequestId: %v)", RequestId_);
        BackoffAlarmTriggered_ = true;
        CheckBackoffAlarmTriggered();
    }

    void CheckBackoffAlarmTriggered()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!BackoffAlarmTriggered_) {
            return;
        }

        if (Replied_) {
            return;
        }

        {
            TTryGuard<TSpinLock> guard(CurrentSubrequestLock_);
            if (guard.WasAcquired()) {
                if (SubresponseCount_ > 0 &&
                    SubresponseCount_ == CurrentSubrequestIndex_)
                {
                    LOG_DEBUG("Backing off (RequestId: %v, SubresponseCount: %v, SubrequestCount: %v)",
                        RequestId_,
                        static_cast<int>(SubresponseCount_),
                        SubrequestCount_);
                    Reply();
                }
                return;
            }
        }

        TObjectService::GetRpcInvoker()->Invoke(
            BIND(&TObjectService::TExecuteSession::CheckBackoffAlarmTriggered, MakeStrong(this)));
    }

    static void DoDecreaseRequestQueueSize(
        const TSecurityManagerPtr& securityManager,
        const TString& userName)
    {
        auto* user = securityManager->FindUserByName(userName);
        if (IsObjectAlive(user)) {
            securityManager->DecreaseRequestQueueSize(user);
        }
    }

    TCodicilGuard MakeCodicilGuard()
    {
        return TCodicilGuard(CodicilData_);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TObjectService::EnqueueSession(TExecuteSessionPtr session)
{
    EnqueuedSessions_.Enqueue(std::move(session));
    EnqueueRunSessionsCallback();
}

void TObjectService::EnqueueRunSessionsCallback()
{
    bool expected = false;
    if (RunSessionsCallbackEnqueued_.compare_exchange_strong(expected, true)) {
        AutomatonInvoker_->Invoke(BIND(&TObjectService::RunSessions, MakeStrong(this)));
    }
}

void TObjectService::RunSessions()
{
    auto startTime = NProfiling::GetCpuInstant();
    auto deadlineTime = startTime + NProfiling::DurationToCpuDuration(Config_->YieldTimeout);

    RunSessionsCallbackEnqueued_.store(false);
    
    EnqueuedSessions_.DequeueAll(false, [&] (TExecuteSessionPtr& session) {
        if (!session->RunFast()) {
            return;
        }

        auto* bucket = GetOrCreateBucket(session->GetUserName());
        // Insert the bucket into the heap if this is its first session.
        if (!bucket->InHeap) {
            BucketHeap_.push_back(bucket);
            AdjustHeapBack(BucketHeap_.begin(), BucketHeap_.end(), TUserBucketComparer());
            bucket->InHeap = true;
        }
        bucket->Sessions.push(std::move(session));
    });

    while (!BucketHeap_.empty() && NProfiling::GetCpuInstant() < deadlineTime) {
        auto* bucket = BucketHeap_.front();
        Y_ASSERT(bucket->InHeap);

        auto actualExcessTime = std::max(bucket->ExcessTime, ExcessBaseline_);

        // Account for charged time possibly reordering the heap.
        if (bucket->HeapKey != actualExcessTime) {
            Y_ASSERT(bucket->HeapKey < actualExcessTime);
            bucket->HeapKey = actualExcessTime;
            AdjustHeapFront(BucketHeap_.begin(), BucketHeap_.end(), TUserBucketComparer());
            continue;
        }

        // Remove the bucket from the heap if no sessions are pending.
        if (bucket->Sessions.empty()) {
            ExtractHeap(BucketHeap_.begin(), BucketHeap_.end(), TUserBucketComparer());
            BucketHeap_.pop_back();
            bucket->InHeap = false;
            continue;
        }

        // Promote the baseline.
        ExcessBaseline_ = actualExcessTime;

        // Extract and run the session.
        auto session = std::move(bucket->Sessions.front());
        bucket->Sessions.pop();
        session->RunSlow();
    }

    if (!BucketHeap_.empty()) {
        EnqueueRunSessionsCallback();
    }
}

TObjectService::TUserBucket* TObjectService::GetOrCreateBucket(const TString& userName)
{
    auto pair = NameToUserBucket_.emplace(userName, TUserBucket(userName));
    return &pair.first->second;
}

void TObjectService::OnUserCharged(TUser* user, const TUserWorkload& workload)
{
    auto* bucket = GetOrCreateBucket(user->GetName());
    // Just charge the bucket, do not reorder it in the heap.
    auto actualExcessTime = std::max(bucket->ExcessTime, ExcessBaseline_);
    bucket->ExcessTime = actualExcessTime + workload.Time;
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    Y_UNUSED(request);
    Y_UNUSED(response);

    auto session = New<TExecuteSession>(this, context);
    if (!session->Prepare()) {
        return;
    }

    EnqueueSession(std::move(session));
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
