#include "object_service.h"
#include "private.h"
#include "object_manager.h"
#include "config.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/master_hydra_service.h>

#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/resolve_cache.h>

#include <yt/server/master/object_server/path_resolver.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/server/lib/hive/hive_manager.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/client/object_client/helpers.h>

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

#include <yt/core/concurrency/rw_spinlock.h>

#include <atomic>
#include <queue>

namespace NYT::NObjectServer {

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
using namespace NObjectClient;
using namespace NHiveServer;
using namespace NCellMaster;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = ObjectServerProfiler;
static TMonotonicCounter CumulativeReadRequestTimeCounter("/cumulative_read_request_time");
static TMonotonicCounter CumulativeMutationScheduleTimeCounter("/cumulative_mutation_schedule_time");
static TMonotonicCounter ReadRequestCounter("/read_request_count");
static TMonotonicCounter WriteRequestCounter("/write_request_count");

////////////////////////////////////////////////////////////////////////////////

class TStickyUserErrorCache
{
public:
    explicit TStickyUserErrorCache(TDuration expireTime)
        : ExpireTime_(DurationToCpuDuration(expireTime))
    { }

    TError Get(const TString& userName)
    {
        auto now = GetCpuInstant();
        {
            TReaderGuard guard(Lock_);
            auto it = Map_.find(userName);
            if (it == Map_.end()) {
                return {};
            }
            if (now < it->second.second) {
                return it->second.first;
            }
        }
        TError expiredError;
        {
            TWriterGuard guard(Lock_);
            auto it = Map_.find(userName);
            if (it != Map_.end() && now > it->second.second) {
                // Prevent destructing the error under spin lock.
                expiredError = std::move(it->second.first);
                Map_.erase(it);
            }
        }
        return {};
    }

    void Put(const TString& userName, const TError& error)
    {
        auto now = GetCpuInstant();
        {
            TWriterGuard guard(Lock_);
            Map_.emplace(userName, std::make_pair(error, now + ExpireTime_));
        }
    }

private:
    const TCpuDuration ExpireTime_;

    TReaderWriterSpinLock Lock_;
    //! Maps user name to (error, deadline) pairs.
    THashMap<TString, std::pair<TError, TCpuInstant>> Map_;
};

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
        , StickyUserErrorCache_(Config_->StickyUserErrorExpireTime)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(10000)
            .SetConcurrencyLimit(10000)
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

    TMultipleProducerSingleConsumerLockFreeStack<TExecuteSessionPtr> ReadySessions_;
    TMultipleProducerSingleConsumerLockFreeStack<TExecuteSessionPtr> FinishedSessions_;

    std::atomic<bool> ProcessSessionsCallbackEnqueued_ = {false};

    TStickyUserErrorCache StickyUserErrorCache_;


    static IInvokerPtr GetRpcInvoker()
    {
        return NRpc::TDispatcher::Get()->GetHeavyInvoker();
    }

    void EnqueueReadySession(TExecuteSessionPtr session);
    void EnqueueFinishedSession(TExecuteSessionPtr session);

    void EnqueueProcessSessionsCallback();
    void ProcessSessions();

    TUserBucket* GetOrCreateBucket(const TString& userName);
    void OnUserCharged(TUser* user, const TUserWorkload& workload);

    void SetStickyUserError(const TString& userName, const TError& error);

    TFuture<void> SyncWithCell(TCellTag cellTag);

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

DEFINE_ENUM(EExecutionSessionSubrequestType,
    (Empty)
    (LocalRead)
    (LocalWrite)
    (Remote)
);

class TObjectService::TExecuteSession
    : public TIntrinsicRefCounted
{
public:
    TExecuteSession(
        TObjectServicePtr owner,
        TCtxExecutePtr rpcContext)
        : Owner_(std::move(owner))
        , RpcContext_(std::move(rpcContext))
        , TotalSubrequestCount_(RpcContext_->Request().part_counts_size())
        , UserName_(RpcContext_->GetUser())
        , RequestId_(RpcContext_->GetRequestId())
        , HydraFacade_(Owner_->Bootstrap_->GetHydraFacade())
        , HydraManager_(HydraFacade_->GetHydraManager())
        , HiveManager_(Owner_->Bootstrap_->GetHiveManager())
        , ObjectManager_(Owner_->Bootstrap_->GetObjectManager())
        , SecurityManager_(Owner_->Bootstrap_->GetSecurityManager())
        , CypressManager_(Owner_->Bootstrap_->GetCypressManager())
        , CodicilData_(Format("RequestId: %v, User: %v",
            RequestId_,
            UserName_))
    { }

    ~TExecuteSession()
    {
        YT_LOG_DEBUG_IF(User_ && !Finished_, "User reference leaked due to unfinished request (RequestId: %v)",
            RequestId_);
        YT_LOG_DEBUG_IF(RequestQueueSizeIncreased_ && !Finished_, "Request queue size increment leaked due to unfinished request (RequestId: %v)",
            RequestId_);
    }

    const TString& GetUserName() const
    {
        return UserName_;
    }

    bool Prepare()
    {
        auto codicilGuard = MakeCodicilGuard();

        RpcContext_->SetRequestInfo("SubrequestCount: %v", TotalSubrequestCount_);

        if (TotalSubrequestCount_ == 0) {
            Reply();
            return false;
        }

        if (IsBackoffAllowed()) {
            ScheduleBackoffAlarm();
        }

        try {
            ParseSubrequests();
            return LocalSubrequestCount_ > 0;
        } catch (const std::exception& ex) {
            Reply(ex);
            return false;
        }
    }

    bool RunFast()
    {
        try {
            return GuardedRunFast();
        } catch (const std::exception& ex) {
            Reply(ex);
            return false;
        }
    }

    void RunSlow()
    {
        auto codicilGuard = MakeCodicilGuard();
        try {
            GuardedRunSlow();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void Finish()
    {
        Finished_ = true;

        if (!EpochCancelableContext_) {
            return;
        }

        if (EpochCancelableContext_->IsCanceled()) {
            return;
        }

        if (RequestQueueSizeIncreased_ && IsObjectAlive(User_)) {
            SecurityManager_->DecreaseRequestQueueSize(User_);
        }

        if (User_) {
            ObjectManager_->EphemeralUnrefObject(User_);
        }
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr RpcContext_;

    TDelayedExecutorCookie BackoffAlarmCookie_;

    const int TotalSubrequestCount_;
    const TString UserName_;
    const TRequestId RequestId_;
    const THydraFacadePtr HydraFacade_;
    const IHydraManagerPtr HydraManager_;
    const THiveManagerPtr HiveManager_;
    const TObjectManagerPtr ObjectManager_;
    const TSecurityManagerPtr SecurityManager_;
    const TCypressManagerPtr CypressManager_;
    const TString CodicilData_;

    struct TSubrequest
    {
        EExecutionSessionSubrequestType Type;
        IServiceContextPtr RpcContext;
        std::unique_ptr<TMutation> Mutation;
        TFuture<TMutationResponse> AsyncCommitResult;
        TRequestHeader RequestHeader;
        TSharedRefArray RequestMessage;
        TSharedRefArray ResponseMessage;
        NTracing::TTraceContextPtr TraceContext;
        ui64 Revision = 0;
        bool Forwarded = false;
        std::atomic<bool> Completed = {false};
    };

    std::unique_ptr<TSubrequest[]> Subrequests_;
    int CurrentSubrequestIndex_ = 0;
    int ThrottledSubrequestIndex_ = -1;

    IInvokerPtr EpochAutomatonInvoker_;
    TCancelableContextPtr EpochCancelableContext_;
    TUser* User_ = nullptr;
    bool NeedsSync_ = true;
    bool SuppressUpstreamSync_ = false;
    TCellTagList CellTagsToSyncWith_;
    bool NeedsUserAccessValidation_ = true;
    bool RequestQueueSizeIncreased_ = false;

    // The number of subrequests that must be served locally, i.e. by switching into the Automaton thread.
    int LocalSubrequestCount_ = 0;
    std::atomic<int> CompletedSubrequestCount_ = {0};
    // Subrequests in range [0, ContinuouslyCompletedRequestCount_) are known to be completed.
    std::atomic<int> ContinuouslyCompletedSubrequestCount_ = {0};

    std::atomic<bool> ReplyScheduled_ = {false};
    std::atomic<bool> FinishScheduled_ = {false};
    bool Finished_ = false;

    // Has the time to backoff come?
    std::atomic<bool> BackoffAlarmTriggered_ = {false};

    // If this is locked, the automaton invoker is currently busy serving
    // CurrentSubrequestIndex_-th subrequest (at which it may or may not succeed).
    // NB: only TryAcquire() is called on this lock, never Acquire().
    TSpinLock CurrentSubrequestLock_;

    const NLogging::TLogger& Logger = ObjectServerLogger;


    void ParseSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TTryGuard<TSpinLock> guard(CurrentSubrequestLock_);
        YT_VERIFY(guard.WasAcquired());

        const auto& request = RpcContext_->Request();
        const auto& attachments = RpcContext_->RequestAttachments();

        CellTagsToSyncWith_ = FromProto<TCellTagList>(request.cell_tags_to_sync_with());

        Subrequests_.reset(new TSubrequest[TotalSubrequestCount_]);
        int currentPartIndex = 0;
        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            int partCount = request.part_counts(subrequestIndex);
            if (partCount == 0) {
                // Empty subrequest.
                subrequest.Type = EExecutionSessionSubrequestType::Empty;
                continue;
            }

            std::vector<TSharedRef> subrequestParts(
                attachments.begin() + currentPartIndex,
                attachments.begin() + currentPartIndex + partCount);
            currentPartIndex += partCount;

            auto& subrequestHeader = subrequest.RequestHeader;
            TSharedRefArray subrequestMessage(std::move(subrequestParts), TSharedRefArray::TMoveParts{});
            if (!ParseRequestHeader(subrequestMessage, &subrequestHeader)) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Error parsing subrequest header");
            }

            // Propagate various parameters to the subrequest.
            ToProto(subrequestHeader.mutable_request_id(), RequestId_);
            subrequestHeader.set_retry(subrequestHeader.retry() || RpcContext_->IsRetry());
            subrequestHeader.set_user(UserName_);
            auto timeout = RpcContext_->GetTimeout().value_or(Owner_->Config_->DefaultExecuteTimeout);
            subrequestHeader.set_timeout(ToProto<i64>(timeout));

            auto* ypathExt = subrequestHeader.MutableExtension(TYPathHeaderExt::ypath_header_ext);

            // COMPAT(savrus) Support old mount/unmount/etc interface.
            if (ypathExt->mutating() && (subrequestHeader.method() == "Mount" ||
                subrequestHeader.method() == "Unmount" ||
                subrequestHeader.method() == "Freeze" ||
                subrequestHeader.method() == "Unfreeze" ||
                subrequestHeader.method() == "Remount" ||
                subrequestHeader.method() == "Reshard"))
            {
                ypathExt->set_mutating(false);
                SetSuppressAccessTracking(&subrequestHeader, true);
            }

            subrequest.TraceContext = NRpc::CreateCallTraceContext(
                subrequestHeader.service(),
                subrequestHeader.method());

            const auto& resolveCache = CypressManager_->GetResolveCache();
            auto resolveResult = resolveCache->TryResolve(ypathExt->path());
            if (resolveResult) {
                // Serve the subrequest by forwarding it through the portal.
                // XXX(babenko): profiling
                ypathExt->set_path(FromObjectId(resolveResult->PortalExitId) + resolveResult->UnresolvedPathSufix);
                subrequest.RequestMessage = SetRequestHeader(subrequestMessage, subrequestHeader);
                subrequest.Type = EExecutionSessionSubrequestType::Remote;
                ForwardSubrequestViaPortal(
                    &subrequest,
                    ypathExt->mutating(),
                    timeout,
                    *resolveResult);
            } else {
                // Serve the subrequest by locally.
                ++LocalSubrequestCount_;
                subrequest.RequestMessage = SetRequestHeader(subrequestMessage, subrequestHeader);
                subrequest.RpcContext = CreateYPathContext(
                    subrequest.RequestMessage,
                    ObjectServerLogger,
                    NLogging::ELogLevel::Debug);

                auto transactionId = GetTransactionId(subrequestHeader);
                if (transactionId) {
                    auto cellTag = CellTagFromId(transactionId);
                    if (cellTag != Owner_->Bootstrap_->GetCellTag() &&
                        cellTag != Owner_->Bootstrap_->GetPrimaryCellTag())
                    {
                        CellTagsToSyncWith_.push_back(cellTag);
                    }
                }

                if (ypathExt->mutating()) {
                    subrequest.Mutation = ObjectManager_->CreateExecuteMutation(UserName_, subrequest.RpcContext);
                    subrequest.Mutation->SetMutationId(subrequest.RpcContext->GetMutationId(), subrequest.RpcContext->IsRetry());
                    subrequest.Type = EExecutionSessionSubrequestType::LocalWrite;
                } else {
                    subrequest.Type = EExecutionSessionSubrequestType::LocalRead;
                }
            }
        }

        SuppressUpstreamSync_ = request.suppress_upstream_sync();
        if (!SuppressUpstreamSync_) {
            NeedsSync_ = true;
        }

        SortUnique(CellTagsToSyncWith_);
        if (!CellTagsToSyncWith_.empty()) {
            NeedsSync_ = true;
            YT_LOG_DEBUG("Request will synchronize with secondary cells (RequestId: %v, CellTags: %v)",
                RequestId_,
                CellTagsToSyncWith_);
        }

        // The backoff alarm may've been triggered while we were parsing.
        CheckBackoffAlarmTriggered();
    }

    void Reschedule()
    {
        Owner_->EnqueueReadySession(this);
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

    bool GuardedRunFast()
    {
        if (Finished_) {
            return false;
        }

        HydraManager_->ValidatePeer(EPeerKind::LeaderOrFollower);

        if (!EpochAutomatonInvoker_) {
            EpochAutomatonInvoker_ = HydraFacade_->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ObjectService);
        }

        if (!EpochCancelableContext_) {
            EpochCancelableContext_ = HydraManager_->GetAutomatonCancelableContext();
        }

        if (!SyncIfNeeded()) {
            return false;
        }

        if (ScheduleFinishIfCanceled()) {
            return false;
        }

        // NB: Acquisitions are only possible if the current epoch is not canceled.

        if (!User_) {
            User_ = SecurityManager_->GetUserByNameOrThrow(UserName_);
            ObjectManager_->EphemeralRefObject(User_);
        }

        if (NeedsUserAccessValidation_) {
            NeedsUserAccessValidation_ = false;
            auto error = SecurityManager_->CheckUserAccess(User_);
            if (!error.IsOK()) {
                Owner_->SetStickyUserError(UserName_, error);
                THROW_ERROR error;
            }
        }

        if (!RequestQueueSizeIncreased_) {
            if (!SecurityManager_->TryIncreaseRequestQueueSize(User_)) {
                auto error = TError(
                    NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded,
                    "User %Qv has exceeded its request queue size limit",
                    User_->GetName())
                    << TErrorAttribute("limit", User_->GetRequestQueueSizeLimit());
                Owner_->SetStickyUserError(UserName_, error);
                THROW_ERROR error;
            }
            RequestQueueSizeIncreased_ = true;
        }

        if (!ThrottleRequests()) {
            return false;
        }

        return true;
    }

    bool ThrottleRequests()
    {
        while (CurrentSubrequestIndex_ < TotalSubrequestCount_ &&
               CurrentSubrequestIndex_ > ThrottledSubrequestIndex_)
        {
            auto workloadType = Subrequests_[CurrentSubrequestIndex_].Mutation
                ? EUserWorkloadType::Write
                : EUserWorkloadType::Read;
            ++ThrottledSubrequestIndex_;
            auto result = SecurityManager_->ThrottleUser(User_, 1, workloadType);
            if (!WaitForAndContinue(result)) {
                return false;
            }
        }
        return true;
    }

    void GuardedRunSlow()
    {
        auto batchStartTime = GetCpuInstant();
        auto batchDeadlineTime = batchStartTime + DurationToCpuDuration(Owner_->Config_->YieldTimeout);

        Owner_->ValidateClusterInitialized();

        while (CurrentSubrequestIndex_ < TotalSubrequestCount_) {
            if (ScheduleFinishIfCanceled()) {
                break;
            }

            if (!ThrottleRequests()) {
                break;
            }

            if (GetCpuInstant() > batchDeadlineTime) {
                YT_LOG_DEBUG("Yielding automaton thread");
                Reschedule();
                break;
            }

            {
                TTryGuard<TSpinLock> guard(CurrentSubrequestLock_);
                if (!guard.WasAcquired()) {
                    Reschedule();
                    break;
                }

                if (ReplyScheduled_) {
                    break;
                }

                if (FinishScheduled_) {
                    break;
                }

                if (BackoffAlarmTriggered_) {
                    break;
                }

                if (!ExecuteCurrentSubrequest()) {
                    break;
                }
            }
        }
    }

    bool ExecuteCurrentSubrequest()
    {
        auto& subrequest = Subrequests_[CurrentSubrequestIndex_++];

        subrequest.Revision = HydraFacade_->GetHydraManager()->GetAutomatonVersion().ToRevision();

        if (subrequest.TraceContext) {
            subrequest.TraceContext->ResetStartTime();
        }

        switch (subrequest.Type) {
            case EExecutionSessionSubrequestType::Empty:
                ExecuteEmptySubrequest(&subrequest);
                break;

            case EExecutionSessionSubrequestType::Remote:
                break;

            case EExecutionSessionSubrequestType::LocalRead:
                ExecuteReadSubrequest(&subrequest);
                break;

            case EExecutionSessionSubrequestType::LocalWrite:
                ExecuteWriteSubrequest(&subrequest);
                break;

            default:
                YT_ABORT();
        }

        return true;
    }

    void ExecuteEmptySubrequest(TSubrequest* subrequest)
    {
        OnSuccessfullSubresponse(subrequest, TSharedRefArray());
    }

    void ExecuteWriteSubrequest(TSubrequest* subrequest)
    {
        Profiler.Increment(WriteRequestCounter);
        TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &CumulativeMutationScheduleTimeCounter);

        subrequest->AsyncCommitResult = subrequest->Mutation->Commit();
        subrequest->AsyncCommitResult.Subscribe(
            BIND(&TExecuteSession::OnMutationCommitted, MakeStrong(this), subrequest));
    }

    void ForwardSubrequestViaPortal(
        TSubrequest* subrequest,
        bool mutating,
        TDuration timeout,
        const TResolveCache::TResolveResult& resolveResult)
    {
        auto asyncSubresponse = ObjectManager_->ForwardObjectRequest(
            subrequest->RequestMessage,
            CellTagFromId(resolveResult.PortalExitId),
            mutating ? EPeerKind::Leader : EPeerKind::Follower,
            timeout);
        SubscribeToSubresponse(subrequest, std::move(asyncSubresponse));
    }

    void ForwardSubrequestToLeader(TSubrequest* subrequest)
    {
        YT_LOG_DEBUG("Performing leader fallback (RequestId: %v)",
            RequestId_);

        auto asyncSubresponse = ObjectManager_->ForwardObjectRequest(
            subrequest->RequestMessage,
            Owner_->Bootstrap_->GetCellTag(),
            EPeerKind::Leader,
            RpcContext_->GetTimeout());
        SubscribeToSubresponse(subrequest, std::move(asyncSubresponse));
    }

    void ExecuteReadSubrequest(TSubrequest* subrequest)
    {
        Profiler.Increment(ReadRequestCounter);
        TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &CumulativeReadRequestTimeCounter);

        TAuthenticatedUserGuard userGuard(SecurityManager_, User_);

        NTracing::TTraceContextGuard traceContextGuard(subrequest->TraceContext);

        TWallTimer timer;

        const auto& context = subrequest->RpcContext;
        try {
            auto rootService = ObjectManager_->GetRootService();
            ExecuteVerb(rootService, context);
        } catch (const TLeaderFallbackException&) {
            ForwardSubrequestToLeader(subrequest);
        }

        // NB: Even if the user was just removed the instance is still valid but not alive.
        if (IsObjectAlive(User_) && !EpochCancelableContext_->IsCanceled()) {
            SecurityManager_->ChargeUser(User_, {EUserWorkloadType::Read, 1, timer.GetElapsedTime()});
        }

        WaitForSubresponse(subrequest);
    }

    void OnMutationCommitted(TSubrequest* subrequest, const TErrorOr<TMutationResponse>& responseOrError)
    {
        if (!responseOrError.IsOK()) {
            Reply(responseOrError);
            return;
        }

        const auto& response = responseOrError.Value();
        const auto& context = subrequest->RpcContext;
        if (response.Origin != EMutationResponseOrigin::Commit) {
            context->Reply(response.Data);
        }

        WaitForSubresponse(subrequest);
    }

    void WaitForSubresponse(TSubrequest* subrequest)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Optimize for the (typical) case of synchronous response.
        const auto& context = subrequest->RpcContext;
        if (context->IsReplied()) {
            OnSuccessfullSubresponse(subrequest, context->GetResponseMessage());
        } else {
            SubscribeToSubresponse(subrequest, context->GetAsyncResponseMessage());
        }
    }

    void SubscribeToSubresponse(TSubrequest* subrequest, TFuture<TSharedRefArray> asyncSubresponseMessage)
    {
        asyncSubresponseMessage.Subscribe(
            BIND(&TExecuteSession::OnSubresponse, MakeStrong(this), subrequest));
    }

    void OnSubresponse(TSubrequest* subrequest, const TErrorOr<TSharedRefArray>& result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!result.IsOK()) {
            Reply(result);
            return;
        }

        OnSuccessfullSubresponse(subrequest, result.Value());
    }

    void OnSuccessfullSubresponse(TSubrequest* subrequest, TSharedRefArray subresponseMessage)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (subrequest->TraceContext) {
            subrequest->TraceContext->Finish();
        }

        subrequest->ResponseMessage = std::move(subresponseMessage);
        subrequest->Completed.store(true);

        // Advance ContinuouslyCompletedSubrequestCount_.
        while (true) {
            int continuouslyCompletedRequestCount = ContinuouslyCompletedSubrequestCount_.load();
            if (continuouslyCompletedRequestCount >= TotalSubrequestCount_) {
                break;
            }
            if (!Subrequests_[continuouslyCompletedRequestCount].Completed.load()) {
                break;
            }
            ContinuouslyCompletedSubrequestCount_.compare_exchange_weak(
                continuouslyCompletedRequestCount,
                continuouslyCompletedRequestCount + 1);
        }

        if (++CompletedSubrequestCount_ == TotalSubrequestCount_) {
            Reply();
        } else {
            CheckBackoffAlarmTriggered();
        }
    }


    void Reply(const TError& error = TError())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ScheduleFinish();

        bool expected = false;
        if (ReplyScheduled_.compare_exchange_strong(expected, true)) {
            TObjectService::GetRpcInvoker()
                ->Invoke(BIND(&TExecuteSession::DoReply, MakeStrong(this), error));
        }
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
            auto& attachments = response.Attachments();

            // NB: This number can be larger than that observed in CheckBackoffAlarmTriggered.
            int continuouslyCompletedSubrequestCount = ContinuouslyCompletedSubrequestCount_.load();
            for (auto index = 0; index < continuouslyCompletedSubrequestCount; ++index) {
                const auto& subrequest = Subrequests_[index];
                YT_ASSERT(subrequest.Completed.load());
                const auto& subresponseMessage = subrequest.ResponseMessage;
                if (subresponseMessage) {
                    response.add_part_counts(subresponseMessage.Size());
                    attachments.insert(
                        attachments.end(),
                        subresponseMessage.Begin(),
                        subresponseMessage.End());
                } else {
                    response.add_part_counts(0);
                }

                response.add_revisions(subrequest.Revision);
            }
        }

        YT_VERIFY(!error.IsOK() ||
            TotalSubrequestCount_ == 0 ||
            RpcContext_->Response().part_counts_size() > 0);

        RpcContext_->Reply(error);
    }


    void ScheduleFinish()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        bool expected = false;
        if (FinishScheduled_.compare_exchange_strong(expected, true)) {
            Owner_->EnqueueFinishedSession(this);
        }
    }

    bool SyncIfNeeded()
    {
        if (!NeedsSync_) {
            return true;
        }
        NeedsSync_ = false;

        std::vector<TFuture<void>> asyncResults;

        if (!SuppressUpstreamSync_) {
            auto result = HydraManager_->SyncWithUpstream();
            if (!result.IsSet() || !result.Get().IsOK()) {
                asyncResults.push_back(std::move(result));
            }
        }

        for (auto cellTag : CellTagsToSyncWith_) {
            asyncResults.push_back(Owner_->SyncWithCell(cellTag));
        }

        if (asyncResults.empty()) {
            return true;
        }

        Combine(std::move(asyncResults))
            .Subscribe(BIND(&TExecuteSession::CheckAndReschedule<void>, MakeStrong(this)));

        return false;
    }

    bool ScheduleFinishIfCanceled()
    {
        if (RpcContext_->IsCanceled() || EpochCancelableContext_->IsCanceled()) {
            ScheduleFinish();
            return true;
        } else {
            return false;
        }
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
        YT_LOG_DEBUG("Backoff alarm triggered (RequestId: %v)", RequestId_);
        BackoffAlarmTriggered_.store(true);
        CheckBackoffAlarmTriggered();
    }

    void CheckBackoffAlarmTriggered()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!BackoffAlarmTriggered_) {
            return;
        }

        if (ReplyScheduled_) {
            return;
        }

        TTryGuard<TSpinLock> guard(CurrentSubrequestLock_);
        if (!guard.WasAcquired()) {
            TObjectService::GetRpcInvoker()->Invoke(
                BIND(&TObjectService::TExecuteSession::CheckBackoffAlarmTriggered, MakeStrong(this)));
            return;
        }

        auto continuouslyCompletedSubrequestCount = ContinuouslyCompletedSubrequestCount_.load();
        if (continuouslyCompletedSubrequestCount > 0 && continuouslyCompletedSubrequestCount >= CurrentSubrequestIndex_) {
            YT_LOG_DEBUG("Replying with partial response (RequestId: %v, TotalSubrequestCount: %v, "
                "ContinuouslyCompletedSubrequestCount: %v, CurrentSubrequestIndex: %v)",
                RequestId_,
                TotalSubrequestCount_,
                continuouslyCompletedSubrequestCount,
                CurrentSubrequestIndex_);
            Reply();
            return;
        }

        if (CurrentSubrequestIndex_ == 0) {
            YT_LOG_DEBUG("Dropping request since no subrequests have started running (RequestId: %v)",
                RequestId_);
            ScheduleFinish();
            return;
        }
    }

    TCodicilGuard MakeCodicilGuard()
    {
        return TCodicilGuard(CodicilData_);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TObjectService::EnqueueReadySession(TExecuteSessionPtr session)
{
    ReadySessions_.Enqueue(std::move(session));
    EnqueueProcessSessionsCallback();
}

void TObjectService::EnqueueFinishedSession(TExecuteSessionPtr session)
{
    FinishedSessions_.Enqueue(std::move(session));
    EnqueueProcessSessionsCallback();
}

void TObjectService::EnqueueProcessSessionsCallback()
{
    bool expected = false;
    if (ProcessSessionsCallbackEnqueued_.compare_exchange_strong(expected, true)) {
        AutomatonInvoker_->Invoke(BIND(&TObjectService::ProcessSessions, MakeStrong(this)));
    }
}

void TObjectService::ProcessSessions()
{
    auto startTime = GetCpuInstant();
    auto deadlineTime = startTime + DurationToCpuDuration(Config_->YieldTimeout);

    ProcessSessionsCallbackEnqueued_.store(false);

    FinishedSessions_.DequeueAll(false, [&] (const TExecuteSessionPtr& session) {
        session->Finish();
    });

    ReadySessions_.DequeueAll(false, [&] (TExecuteSessionPtr& session) {
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

    while (!BucketHeap_.empty() && GetCpuInstant() < deadlineTime) {
        auto* bucket = BucketHeap_.front();
        YT_ASSERT(bucket->InHeap);

        auto actualExcessTime = std::max(bucket->ExcessTime, ExcessBaseline_);

        // Account for charged time possibly reordering the heap.
        if (bucket->HeapKey != actualExcessTime) {
            YT_ASSERT(bucket->HeapKey < actualExcessTime);
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
        EnqueueProcessSessionsCallback();
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

void TObjectService::SetStickyUserError(const TString& userName, const TError& error)
{
    StickyUserErrorCache_.Put(userName, error);
}

TFuture<void> TObjectService::SyncWithCell(TCellTag cellTag)
{
    auto cellId = Bootstrap_->GetCellId(cellTag);
    const auto& hiveManager = Bootstrap_->GetHiveManager();
    return hiveManager->SyncWith(cellId, true);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    Y_UNUSED(request);
    Y_UNUSED(response);

    const auto& userName = context->GetUser();
    auto error = StickyUserErrorCache_.Get(userName);
    if (!error.IsOK()) {
        context->Reply(error);
        return;
    }

    auto session = New<TExecuteSession>(this, context);
    if (!session->Prepare()) {
        return;
    }

    EnqueueReadySession(std::move(session));
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

} // namespace NYT::NObjectServer
