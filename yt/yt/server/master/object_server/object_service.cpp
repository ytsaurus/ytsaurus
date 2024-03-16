#include "object_service.h"
#include "private.h"
#include "object_manager.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/master_hydra_service.h>
#include <yt/yt/server/master/cell_master/multi_phase_cell_sync_session.h>

#include <yt/yt/server/master/cypress_server/cypress_manager.h>
#include <yt/yt/server/master/cypress_server/resolve_cache.h>

#include <yt/yt/server/master/object_server/path_resolver.h>
#include <yt/yt/server/master/object_server/request_profiling_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>
#include <yt/yt/server/master/security_server/user.h>

#include <yt/yt/server/master/transaction_server/transaction_replication_session.h>

#include <yt/yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/server/lib/transaction_server/helpers.h>

#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/object_service_cache.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/core/rpc/helpers.h>
#include <yt/yt/core/rpc/message.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/rpc/per_user_request_queue_provider.h>

#include <yt/yt/core/ytree/request_complexity_limiter.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/fair_scheduler.h>
#include <yt/yt/core/misc/heap.h>
#include <yt/yt/core/misc/mpsc_stack.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/quantized_executor.h>

#include <library/cpp/yt/threading/recursive_spin_lock.h>
#include <library/cpp/yt/threading/spin_lock.h>
#include <library/cpp/yt/threading/traceless_guard.h>

#include <util/generic/algorithm.h>

#include <atomic>

namespace NYT::NObjectServer {

using namespace NHydra;
using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressServer;
using namespace NSecurityClient;
using namespace NSecurityServer;
using namespace NTransactionClient;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NHiveServer;
using namespace NCellMaster;
using namespace NProfiling;
using namespace NTracing;

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
            auto guard = ReaderGuard(Lock_);
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
            auto guard = WriterGuard(Lock_);
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
            auto guard = WriterGuard(Lock_);
            Map_.emplace(userName, std::pair(error, now + ExpireTime_));
        }
    }

private:
    const TCpuDuration ExpireTime_;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    //! Maps user name to (error, deadline) pairs.
    THashMap<TString, std::pair<TError, TCpuInstant>> Map_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TObjectService)

class TObjectService
    : public IObjectService
    , public NCellMaster::TMasterHydraServiceBase
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
        , Cache_(New<TObjectServiceCache>(
            Config_->MasterCache,
            GetNullMemoryUsageTracker(),
            ObjectServerLogger,
            ObjectServerProfiler.WithPrefix("/object_service_cache")))
        , ProcessSessionsExecutor_(New<TPeriodicExecutor>(
            AutomatonInvoker_,
            BIND(&TObjectService::ProcessSessions, MakeWeak(this))))
        , LocalReadSessionScheduler_(CreateFairScheduler<TExecuteSessionPtr>())
        , AutomatonSessionScheduler_(CreateFairScheduler<TExecuteSessionPtr>())
        , LocalReadCallbackProvider_(New<TLocalReadCallbackProvider>(LocalReadSessionScheduler_))
        , LocalReadExecutor_(CreateQuantizedExecutor(
            "LocalRead",
            LocalReadCallbackProvider_,
            TQuantizedExecutorOptions{
                .ThreadInitializer = MakeLocalReadThreadInitializer(),
            }))
        , LocalReadOffloadPool_(CreateThreadPool(
            /*threadCount*/ 1,
            "LocalReadOff",
            TThreadPoolOptions{
                .ThreadInitializer = MakeLocalReadThreadInitializer(),
            }))
        , StickyUserErrorCache_(Config_->StickyUserErrorExpireTime)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(100'000)
            .SetConcurrencyLimit(100'000)
            .SetRequestQueueProvider(ExecuteRequestQueueProvider_)
            .SetCancelable(true)
            .SetInvoker(GetRpcInvoker())
            // NB: Execute request is always replied in heavy RPC invoker, so it should not be
            // marked as heavy.
            // Execute request handler needs request to remain alive after Reply call.
            .SetPooled(false));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect)
            .SetHeavy(true));

        DeclareServerFeature(EMasterFeature::OverlayedJournals);
        DeclareServerFeature(EMasterFeature::Portals);
        DeclareServerFeature(EMasterFeature::PortalExitSynchronization);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->SubscribeUserCharged(BIND_NO_PROPAGATE(&TObjectService::OnUserCharged, MakeStrong(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TObjectService::OnDynamicConfigChanged, MakeWeak(this)));

        EnableLocalReadExecutor_.store(Config_->EnableLocalReadExecutor);

        ProcessSessionsExecutor_->Start();
    }

    TObjectServiceCachePtr GetCache() override
    {
        return Cache_;
    }

    IInvokerPtr GetLocalReadOffloadInvoker() override
    {
        return LocalReadOffloadPool_->GetInvoker();
    }

private:
    const TObjectServiceConfigPtr Config_;

    const IInvokerPtr AutomatonInvoker_;
    const TObjectServiceCachePtr Cache_;
    const TPeriodicExecutorPtr ProcessSessionsExecutor_;

    class TExecuteSession;
    using TExecuteSessionPtr = TIntrusivePtr<TExecuteSession>;

    IRequestQueueProviderPtr ExecuteRequestQueueProvider_ = New<TPerUserRequestQueueProvider>();

    class TSessionScheduler;

    struct TExecuteSessionInfo
    {
        TCancelableContextPtr EpochCancelableContext;
        TEphemeralObjectPtr<TUser> User;
        bool RequestQueueSizeIncreased;
    };

    using TSessionSchedulerPtr = IFairSchedulerPtr<TExecuteSessionPtr>;

    //! Scheduler of sessions to process in local read executor.
    TSessionSchedulerPtr LocalReadSessionScheduler_;

    //! Scheduler of sessions to process in automaton.
    TSessionSchedulerPtr AutomatonSessionScheduler_;

    class TLocalReadCallbackProvider
        : public ICallbackProvider
    {
    public:
        explicit TLocalReadCallbackProvider(TSessionSchedulerPtr sessionScheduler);

        virtual TCallback<void()> ExtractCallback() override;

    private:
        TSessionSchedulerPtr SessionScheduler_;
    };

    TIntrusivePtr<TLocalReadCallbackProvider> LocalReadCallbackProvider_;

    const IQuantizedExecutorPtr LocalReadExecutor_;
    const IThreadPoolPtr LocalReadOffloadPool_;

    TMpscStack<TExecuteSessionPtr> ReadySessions_;
    TMpscStack<TExecuteSessionInfo> FinishedSessionInfos_;

    TStickyUserErrorCache StickyUserErrorCache_;
    std::atomic<bool> EnableTwoLevelCache_ = false;
    std::atomic<bool> EnableLocalReadExecutor_ = true;
    std::atomic<TDuration> ScheduleReplyRetryBackoff_ = TDuration::MilliSeconds(100);

    static IInvokerPtr GetRpcInvoker()
    {
        return NRpc::TDispatcher::Get()->GetHeavyInvoker();
    }

    const TDynamicObjectServiceConfigPtr& GetDynamicConfig();
    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig);

    void EnqueueReadySession(TExecuteSessionPtr session);
    void EnqueueFinishedSession(TExecuteSessionInfo sessionInfo);

    void ProcessSessions();
    void FinishSession(const TExecuteSessionInfo& sessionInfo);

    void OnUserCharged(TUser* user, const TUserWorkload& workload);

    void SetStickyUserError(const TString& userName, const TError& error);

    std::function<void()> MakeLocalReadThreadInitializer()
    {
        return [epochContext = Bootstrap_->GetHydraFacade()->GetEpochContext()] {
            NObjectServer::SetupEpochContext(epochContext);
        };
    }

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, Execute);
    DECLARE_RPC_SERVICE_METHOD(NObjectClient::NProto, GCCollect);
};

DEFINE_REFCOUNTED_TYPE(TObjectService)

IObjectServicePtr CreateObjectService(
    TObjectServiceConfigPtr config,
    TBootstrap* bootstrap)
{
    return New<TObjectService>(
        std::move(config),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExecutionSessionSubrequestType,
    (Undefined)
    (LocalRead)
    (LocalWrite)
    (Remote)
    (Cache)
);

DEFINE_ENUM(ESyncPhase,
    (One)
    (Two)
    (Three)
);

class TObjectService::TExecuteSession
    : public TRefCounted
{
public:
    TExecuteSession(
        TObjectServicePtr owner,
        TCtxExecutePtr rpcContext)
        : Owner_(std::move(owner))
        , RpcContext_(std::move(rpcContext))
        , TraceContext_(TryGetCurrentTraceContext())
        , Bootstrap_(Owner_->Bootstrap_)
        , TotalSubrequestCount_(RpcContext_->Request().part_counts_size())
        , UserName_(RpcContext_->GetAuthenticationIdentity().User)
        , RequestId_(RpcContext_->GetRequestId())
        , CodicilData_(Format("RequestId: %v, %v",
            RequestId_,
            RpcContext_->GetAuthenticationIdentity()))
        , TentativePeerState_(Bootstrap_->GetHydraFacade()->GetHydraManager()->GetAutomatonState())
        , CellSyncSession_(New<TMultiPhaseCellSyncSession>(
            Bootstrap_,
            RequestId_))
          // Copy so it doesn't change mid-execution of this particular session.
        , EnableLocalReadExecutor_(Owner_->EnableLocalReadExecutor_.load())
    { }

    ~TExecuteSession()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        CancelPendingCacheSubrequests();

        Owner_->EnqueueFinishedSession(TExecuteSessionInfo{
            std::move(EpochCancelableContext_),
            std::move(User_),
            RequestQueueSizeIncreased_
        });
    }

    const TString& GetUserName() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return UserName_;
    }

    void RunRpc()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        try {
            GuardedRunRpc();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void OnDequeued()
    {
        Enqueued_.store(false);
    }

    bool RunAutomatonFast()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        try {
            return GuardedRunAutomatonFast();
        } catch (const std::exception& ex) {
            Reply(ex);
            return false;
        }
    }

    TRequestId GetRequestId() const
    {
        return RequestId_;
    }

    void RunAutomatonSlow()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto codicilGuard = MakeCodicilGuard();
        try {
            GuardedRunAutomatonSlow();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void RunRead()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto codicilGuard = MakeCodicilGuard();
        try {
            GuardedRunRead();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    TTraceContextPtr GetTraceContext() const
    {
        return TraceContext_;
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr RpcContext_;
    const TTraceContextPtr TraceContext_;

    NCellMaster::TBootstrap* const Bootstrap_;
    const int TotalSubrequestCount_;
    const TString& UserName_;
    const TRequestId RequestId_;
    const TString CodicilData_;
    const EPeerState TentativePeerState_;
    const TMultiPhaseCellSyncSessionPtr CellSyncSession_;
    const bool EnableLocalReadExecutor_;

    TDelayedExecutorCookie BackoffAlarmCookie_;

    struct TSubrequest
    {
        int Index = -1;
        bool TentativelyRemote = false;
        EExecutionSessionSubrequestType Type = EExecutionSessionSubrequestType::Undefined;
        IYPathServiceContextPtr RpcContext;
        std::unique_ptr<TMutation> Mutation;
        std::optional<TObjectServiceCache::TCookie> CacheCookie;
        NRpc::NProto::TRequestHeader RequestHeader;
        const NYTree::NProto::TYPathHeaderExt* YPathExt = nullptr;
        const NObjectClient::NProto::TPrerequisitesExt* PrerequisitesExt = nullptr;
        const NObjectClient::NProto::TMulticellSyncExt* MulticellSyncExt = nullptr;
        TSharedRefArray RequestMessage;
        TCellTag ForwardedCellTag = InvalidCellTag;
        std::optional<TYPathRewrite> TargetPathRewrite;
        std::optional<TCompactVector<TYPathRewrite, TypicalAdditionalPathCount>> AdditionalPathRewrites;
        std::optional<TCompactVector<TYPathRewrite, 4>> PrerequisiteRevisionPathRewrites;
        TSharedRefArray RemoteRequestMessage;
        TSharedRefArray ResponseMessage;
        TTraceContextPtr TraceContext;
        NHydra::TRevision Revision = NHydra::NullRevision;
        std::atomic<bool> Uncertain = false;
        std::atomic<bool> LocallyStarted = false;
        std::atomic<bool> Completed = false;
        TRequestProfilingCountersPtr ProfilingCounters;
        // Only for (local) write requests. (Local read requests are handled by
        // a session-wide replication session).
        TTransactionReplicationSessionWithBoomerangsPtr RemoteTransactionReplicationSession;
        // For local reads, this is a future that will be set when all remote
        // transactions have actually been replicated here.
        // Mutually exclusive with MutationResponseFuture.
        TFuture<void> RemoteTransactionReplicationFuture;
        // For local writes, this is a future that is set when the mutation is applied. That mutation
        // may be either committed in the ordinary fashion or posted as a boomerang.
        // Mutually exclusive with RemoteTransactionReplicationFuture.
        TFuture<TMutationResponse> MutationResponseFuture;

        TReadRequestComplexityOverrides ReadRequestComplexityOverrides;
    };

    // For (local) read requests. (Write requests are handled by per-subrequest replication sessions.)
    TTransactionReplicationSessionWithoutBoomerangsPtr RemoteTransactionReplicationSession_;

    THashMap<TTransactionId, TCompactVector<TSubrequest*, 1>> RemoteTransactionIdToSubrequests_;

    std::unique_ptr<TSubrequest[]> Subrequests_;
    int CurrentAutomatonSubrequestIndex_ = 0;
    int CurrentLocalReadSubrequestIndex_ = 0;

    int ThrottledAutomatonSubrequestIndex_ = -1;
    int ThrottledLocalReadSubrequestIndex_ = -1;

    IInvokerPtr EpochAutomatonInvoker_;
    TCancelableContextPtr EpochCancelableContext_;

    TEphemeralObjectPtr<TUser> User_;

    struct TReadRequestComplexityLimits
    {
        TReadRequestComplexity Default;
        TReadRequestComplexity Max;
    };
    std::optional<TReadRequestComplexityLimits> ReadRequestComplexityLimits_;

    bool SuppressTransactionCoordinatorSync_ = false;
    bool NeedsUserAccessValidation_ = true;
    bool RequestQueueSizeIncreased_ = false;

    std::atomic<bool> ReplyScheduled_ = false;
    std::atomic<bool> LocalExecutionStarted_ = false;
    std::atomic<bool> LocalExecutionInterrupted_ = false;

    // If this is locked, the system is currently busy serving
    // some local subrequest.
    // NB: only TryAcquire() is called on this lock, never Acquire().
    YT_DECLARE_SPIN_LOCK(NThreading::TRecursiveSpinLock, LocalExecutionLock_);

    // Has the time to backoff come?
    std::atomic<bool> BackoffAlarmTriggered_ = false;

    // Once this drops to zero, the request can be replied.
    // Starts with one to indicate that the "ultimate" lock is initially held.
    std::atomic<int> ReplyLockCount_ = 1;

    // Set to true when the "ultimate" reply lock is released and
    // RepyLockCount_ is decremented.
    std::atomic<bool> UltimateReplyLockReleased_ = false;

    // Set to true if we're ready to reply with at least one subresponse.
    std::atomic<bool> SomeSubrequestCompleted_ = false;

    std::atomic<bool> Enqueued_ = false;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const NLogging::TLogger& Logger = ObjectServerLogger;


    void GuardedRunRpc()
    {
        auto codicilGuard = MakeCodicilGuard();

        const auto& request = RpcContext_->Request();

        auto originalRequestId = FromProto<TRequestId>(request.original_request_id());

        RpcContext_->SetRequestInfo("SubrequestCount: %v, SupportsPortals: %v, SuppressUpstreamSync: %v, "
            "SuppressTransactionCoordinatorSync: %v, OriginalRequestId: %v",
            TotalSubrequestCount_,
            request.supports_portals(),
            GetSuppressUpstreamSync(RpcContext_),
            GetSuppressTransactionCoordinatorSync(RpcContext_),
            originalRequestId);

        if (TotalSubrequestCount_ == 0) {
            Reply();
            return;
        }

        if (TentativePeerState_ != EPeerState::Leading && TentativePeerState_ != EPeerState::Following) {
            THROW_ERROR_EXCEPTION(NRpc::EErrorCode::Unavailable, "Hydra peer is not active");
        }

        ScheduleBackoffAlarm();
        ParseSubrequests();
        MarkTentativelyRemoteSubrequests();
        // Necessary to determine which tx coordinator cells require explicitly
        // syncing with, and which will be handled implicitly by tx replication.
        CheckSubrequestsForRemoteTransactions();
        RunSyncPhaseOne();
    }

    void ParseSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& request = RpcContext_->Request();
        const auto& attachments = RpcContext_->RequestAttachments();

        Subrequests_.reset(new TSubrequest[TotalSubrequestCount_]);

        auto now = NProfiling::GetInstant();

        auto suppressUpstreamSync = GetSuppressUpstreamSync(RpcContext_);
        auto suppressTransactionCoordinatorSync = GetSuppressTransactionCoordinatorSync(RpcContext_);
        int currentPartIndex = 0;
        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            subrequest.Index = subrequestIndex;

            int partCount = request.part_counts(subrequestIndex);
            TSharedRefArrayBuilder subrequestPartsBuilder(partCount);
            for (int partIndex = 0; partIndex < partCount; ++partIndex) {
                subrequestPartsBuilder.Add(attachments[currentPartIndex++]);
            }
            subrequest.RequestMessage = subrequestPartsBuilder.Finish();

            auto& requestHeader = subrequest.RequestHeader;
            if (!ParseRequestHeader(subrequest.RequestMessage, &requestHeader)) {
                THROW_ERROR_EXCEPTION(
                    NRpc::EErrorCode::ProtocolError,
                    "Error parsing subrequest header");
            }

            const auto& requestProfilingManager = Bootstrap_->GetRequestProfilingManager();
            subrequest.ProfilingCounters = requestProfilingManager->GetCounters(
                RpcContext_->GetAuthenticationIdentity().UserTag,
                requestHeader.method());

            // Propagate various parameters to the subrequest.
            if (!requestHeader.has_request_id()) {
                ToProto(requestHeader.mutable_request_id(), RequestId_);
            }
            if (RpcContext_->IsRetry()) {
                requestHeader.set_retry(true);
            }
            if (!requestHeader.has_user()) {
                NRpc::WriteAuthenticationIdentityToProto(&requestHeader, RpcContext_->GetAuthenticationIdentity());
            }
            if (!requestHeader.has_timeout()) {
                requestHeader.set_timeout(ToProto<i64>(RpcContext_->GetTimeout().value_or(Owner_->Config_->DefaultExecuteTimeout)));
            }
            if (!requestHeader.has_start_time()) {
                requestHeader.set_start_time(ToProto<ui64>(RpcContext_->GetStartTime().value_or(now)));
            }
            if (GetSuppressUpstreamSync(RpcContext_)) {
                SetSuppressUpstreamSync(&requestHeader, true);
            }
            if (GetSuppressTransactionCoordinatorSync(RpcContext_)) {
                SetSuppressTransactionCoordinatorSync(&requestHeader, true);
            }

            auto* ypathExt = requestHeader.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
            subrequest.YPathExt = ypathExt;

            subrequest.PrerequisitesExt = &requestHeader.GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);

            subrequest.MulticellSyncExt = &requestHeader.GetExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
            suppressUpstreamSync = suppressUpstreamSync ||
                subrequest.MulticellSyncExt->suppress_upstream_sync();
            suppressTransactionCoordinatorSync = suppressTransactionCoordinatorSync ||
                subrequest.MulticellSyncExt->suppress_transaction_coordinator_sync();

            // Store original path.
            if (!ypathExt->has_original_target_path()) {
                ypathExt->set_original_target_path(ypathExt->target_path());
            }

            if (ypathExt->original_additional_paths_size() == 0) {
                *ypathExt->mutable_original_additional_paths() = ypathExt->additional_paths();
            }

            if (subrequest.YPathExt->mutating()) {
                if (!FromProto<TMutationId>(requestHeader.mutation_id())) {
                    ToProto(requestHeader.mutable_mutation_id(), NRpc::GenerateMutationId());
                }
            }

            subrequest.RequestMessage = SetRequestHeader(subrequest.RequestMessage, requestHeader);

            if (ypathExt->mutating()) {
                subrequest.ProfilingCounters->TotalWriteRequestCounter.Increment();
            } else {
                subrequest.ProfilingCounters->TotalReadRequestCounter.Increment();
            }

            if (ypathExt->has_read_complexity_limits()) {
                FromProto(&subrequest.ReadRequestComplexityOverrides, ypathExt->read_complexity_limits());
            }
        }

        CellSyncSession_->SetSyncWithUpstream(!suppressUpstreamSync);
        SuppressTransactionCoordinatorSync_ = suppressTransactionCoordinatorSync;
    }

    void LookupCachedSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!Owner_->EnableTwoLevelCache_) {
            return;
        }

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            const auto& requestHeader = subrequest.RequestHeader;
            if (!requestHeader.HasExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext)) {
                continue;
            }

            if (subrequest.YPathExt->mutating()) {
                THROW_ERROR_EXCEPTION(
                    NObjectClient::EErrorCode::CannotCacheMutatingRequest,
                    "Mutating requests cannot be cached");
            }

            const auto& cachingRequestHeaderExt = requestHeader.GetExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
            const auto multicellSyncExt = requestHeader.GetExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
            TObjectServiceCacheKey key(
                Bootstrap_->GetCellTag(),
                cachingRequestHeaderExt.disable_per_user_cache() ? TString() : RpcContext_->GetAuthenticationIdentity().User,
                subrequest.YPathExt->target_path(),
                requestHeader.service(),
                requestHeader.method(),
                subrequest.RequestMessage[1],
                multicellSyncExt.suppress_upstream_sync(),
                multicellSyncExt.suppress_transaction_coordinator_sync());

            YT_LOG_DEBUG("Serving subrequest from cache (RequestId: %v, SubrequestIndex: %v, Key: %v)",
                RequestId_,
                subrequestIndex,
                key);

            auto refreshRevision = cachingRequestHeaderExt.refresh_revision();
            auto cookie = Owner_->Cache_->BeginLookup(
                RequestId_,
                key,
                FromProto<TDuration>(cachingRequestHeaderExt.expire_after_successful_update_time()),
                FromProto<TDuration>(cachingRequestHeaderExt.expire_after_failed_update_time()),
                FromProto<TDuration>(cachingRequestHeaderExt.success_staleness_bound()),
                refreshRevision);

            if (cookie.IsActive()) {
                subrequest.CacheCookie.emplace(std::move(cookie));
                continue;
            }

            subrequest.Type = EExecutionSessionSubrequestType::Cache;

            AcquireReplyLock();

            cookie.GetValue()
                .Subscribe(BIND([this, this_ = MakeStrong(this), subrequestIndex] (const TErrorOr<TObjectServiceCacheEntryPtr>& entryOrError) {
                    auto& subrequest = Subrequests_[subrequestIndex];
                    if (!entryOrError.IsOK()) {
                        if (entryOrError.FindMatching(NYT::EErrorCode::Canceled)) {
                            Reply(TError(NRpc::EErrorCode::TransientFailure, "Transient failure")
                            << entryOrError);
                        } else {
                            Reply(entryOrError);
                        }
                        return;
                    }
                    const auto& entry = entryOrError.Value();
                    subrequest.Revision = entry->GetRevision();
                    OnSuccessfulSubresponse(&subrequest, entry->GetResponseMessage());
                }));
        }
    }

    TCellTagList CollectCellsToSyncForLocalExecution(ESyncPhase syncPhase)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TCellTagList result;
        auto addCellTagToSyncWith = [&] (TCellTag cellTag) {
            if (std::find(result.begin(), result.end(), cellTag) == result.end()) {
                result.push_back(cellTag);
            }
        };
        auto addCellTagsToSyncWith = [&] (const TCellTagList& cellTags) {
            for (auto cellTag : cellTags) {
                addCellTagToSyncWith(cellTag);
            }
        };

        if (!SuppressTransactionCoordinatorSync_) {
            switch (syncPhase) {
                case ESyncPhase::One:
                case ESyncPhase::Two:
                    addCellTagsToSyncWith(
                        RemoteTransactionReplicationSession_->GetCellTagsToSyncWithDuringInvocation());
                    break;

                case ESyncPhase::Three:
                    addCellTagsToSyncWith(
                        RemoteTransactionReplicationSession_->GetCellTagsToSyncWithAfterInvocation());
                    break;

                default:
                    YT_ABORT();
            }
        }

        const auto& multicellExt = RpcContext_->RequestHeader().GetExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
        for (auto protoCellTag : multicellExt.cell_tags_to_sync_with()) {
            auto cellTag = FromProto<TCellTag>(protoCellTag);
            if (cellTag == Bootstrap_->GetCellTag()) {
                continue;
            }
            addCellTagToSyncWith(cellTag);
        }

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            const auto& subrequest = Subrequests_[subrequestIndex];
            if (subrequest.Type == EExecutionSessionSubrequestType::Undefined && subrequest.TentativelyRemote) {
                // Phase one.
                continue;
            }
            if (subrequest.Type == EExecutionSessionSubrequestType::Remote ||
                subrequest.Type == EExecutionSessionSubrequestType::Cache)
            {
                // Phase two.
                continue;
            }

            if (!SuppressTransactionCoordinatorSync_ && subrequest.RemoteTransactionReplicationSession) {
                addCellTagsToSyncWith(
                    subrequest.RemoteTransactionReplicationSession->GetCellTagsToSyncWithBeforeInvocation());
            }

            for (auto protoCellTag : subrequest.MulticellSyncExt->cell_tags_to_sync_with()) {
                auto cellTag = FromProto<TCellTag>(protoCellTag);
                if (cellTag == Bootstrap_->GetCellTag()) {
                    continue;
                }
                addCellTagToSyncWith(cellTag);
            }
        }

        SortUnique(result);
        return result;
    }

    bool IsTentativelyRemoteSubrequest(const TSubrequest& subrequest)
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& resolveCache = cypressManager->GetResolveCache();
        auto resolveResult = resolveCache->TryResolve(subrequest.YPathExt->target_path());
        if (resolveResult) {
            return true;
        }

        if (subrequest.YPathExt->mutating() && TentativePeerState_ != EPeerState::Leading) {
            return true;
        }

        return false;
    }

    void MarkTentativelyRemoteSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            subrequest.TentativelyRemote = IsTentativelyRemoteSubrequest(subrequest);
        }
    }

    TFuture<void> StartSync(ESyncPhase syncPhase, TFuture<void> additionalFuture = {})
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto cellTags = CollectCellsToSyncForLocalExecution(syncPhase);

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& hydraManager = hydraFacade->GetHydraManager();
        if (!cellTags.empty() && hydraManager->GetReadOnly()) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::ReadOnly,
                "Cannot synchronize with cells when read-only mode is active (CellTags: %v)",
                cellTags);
        }

        if (additionalFuture) {
            return CellSyncSession_->Sync(cellTags, std::move(additionalFuture));
        } else {
            return CellSyncSession_->Sync(cellTags);
        }
    }

    void RunSyncPhaseOne()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto future = StartSync(ESyncPhase::One);
        if (future.IsSet()) {
            OnSyncPhaseOneCompleted(future.Get());
        } else {
            future.Subscribe(BIND(&TExecuteSession::OnSyncPhaseOneCompleted, MakeStrong(this)));
        }
    }

    void OnSyncPhaseOneCompleted(const TError& error = {})
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!error.IsOK()) {
            Reply(error);
            return;
        }

        try {
            LookupCachedSubrequests();

            // Re-check remote requests to see if resolve cache resolve is still OK.
            DecideSubrequestTypes();

            ValidateRequestsFeatures();

            ForwardRemoteRequests();

            // Re-check so that
            //   - previously assumed to be remote but actually local subrequests are handled correctly, and
            //   - previously assumed to be local but actually remote subrequests are ignored.
            CheckSubrequestsForRemoteTransactions();

            RunSyncPhaseTwo();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void MarkSubrequestLocal(TSubrequest* subrequest)
    {
        auto mutating = subrequest->YPathExt->mutating();

        if (mutating && TentativePeerState_ != EPeerState::Leading) {
            Reply(TError(NRpc::EErrorCode::Unavailable, "Peer is not leading"));
            return;
        }

        subrequest->RpcContext = CreateYPathContext(
            subrequest->RequestMessage,
            ObjectServerLogger,
            NLogging::ELogLevel::Debug);

        if (mutating) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            subrequest->Mutation = objectManager->CreateExecuteMutation(subrequest->RpcContext, subrequest->RpcContext->GetAuthenticationIdentity());
            subrequest->Mutation->SetMutationId(subrequest->RpcContext->GetMutationId(), subrequest->RpcContext->IsRetry());
            subrequest->Mutation->SetTraceContext(TraceContext_);
            subrequest->Type = EExecutionSessionSubrequestType::LocalWrite;
            subrequest->ProfilingCounters->LocalWriteRequestCounter.Increment();
        } else {
            subrequest->Type = EExecutionSessionSubrequestType::LocalRead;
            subrequest->ProfilingCounters->LocalReadRequestCounter.Increment();
        }
    }

    void MarkSubrequestRemoteCrossCell(TSubrequest* subrequest, TCellTag forwardedCellTag)
    {
        auto remoteRequestHeader = subrequest->RequestHeader;
        auto* remoteYPathExt = remoteRequestHeader.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
        auto* remotePrerequisitesExt = remoteRequestHeader.MutableExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);

        if (subrequest->TargetPathRewrite) {
            remoteYPathExt->set_target_path(subrequest->TargetPathRewrite->Rewritten);
        }

        if (subrequest->AdditionalPathRewrites) {
            remoteYPathExt->clear_additional_paths();
            for (const auto& rewrite : *subrequest->AdditionalPathRewrites) {
                remoteYPathExt->add_additional_paths(rewrite.Rewritten);
            }
        }

        if (subrequest->PrerequisiteRevisionPathRewrites) {
            YT_VERIFY(static_cast<int>(subrequest->PrerequisiteRevisionPathRewrites->size()) == remotePrerequisitesExt->revisions_size());
            for (int index = 0; index < remotePrerequisitesExt->revisions_size(); ++index) {
                remotePrerequisitesExt->mutable_revisions(index)->set_path((*subrequest->PrerequisiteRevisionPathRewrites)[index].Rewritten);
            }
        }

        if (auto mutationId = NRpc::GetMutationId(remoteRequestHeader)) {
            SetMutationId(&remoteRequestHeader, GenerateNextForwardedMutationId(mutationId), remoteRequestHeader.retry());
        }

        subrequest->ForwardedCellTag = forwardedCellTag;
        subrequest->RemoteRequestMessage = SetRequestHeader(subrequest->RequestMessage, remoteRequestHeader);
        subrequest->Type = EExecutionSessionSubrequestType::Remote;
        subrequest->ProfilingCounters->CrossCellForwardingRequestCounter.Increment();
    }

    void DecideSubrequestType(TSubrequest* subrequest)
    {
        if (subrequest->Type == EExecutionSessionSubrequestType::Cache) {
            return;
        }

        const auto& cypressManager = Bootstrap_->GetCypressManager();
        const auto& resolveCache = cypressManager->GetResolveCache();

        auto targetResolveResult = resolveCache->TryResolve(subrequest->YPathExt->target_path());
        if (!targetResolveResult) {
            MarkSubrequestLocal(subrequest);
            return;
        }

        subrequest->TargetPathRewrite = MakeYPathRewrite(
            subrequest->YPathExt->target_path(),
            targetResolveResult->RemoteNodeId,
            targetResolveResult->UnresolvedPathSuffix);

        subrequest->AdditionalPathRewrites.emplace();
        subrequest->AdditionalPathRewrites->reserve(subrequest->YPathExt->additional_paths_size());
        for (const auto& additionalPath : subrequest->YPathExt->additional_paths()) {
            auto additionalResolveResult = resolveCache->TryResolve(additionalPath);
            if (!additionalResolveResult) {
                MarkSubrequestLocal(subrequest);
                return;
            }

            if (CellTagFromId(additionalResolveResult->RemoteNodeId) != CellTagFromId(targetResolveResult->RemoteNodeId)) {
                MarkSubrequestLocal(subrequest);
                return;
            }

            subrequest->AdditionalPathRewrites->push_back(MakeYPathRewrite(
                additionalPath,
                additionalResolveResult->RemoteNodeId,
                additionalResolveResult->UnresolvedPathSuffix));
        }

        subrequest->PrerequisiteRevisionPathRewrites.emplace();
        for (const auto& prerequisite : subrequest->PrerequisitesExt->revisions()) {
            const auto& prerequisitePath = prerequisite.path();
            auto prerequisiteResolveResult = resolveCache->TryResolve(prerequisitePath);
            if (!prerequisiteResolveResult) {
                MarkSubrequestLocal(subrequest);
                return;
            }

            if (CellTagFromId(prerequisiteResolveResult->RemoteNodeId) != CellTagFromId(targetResolveResult->RemoteNodeId)) {
                MarkSubrequestLocal(subrequest);
                return;
            }

            subrequest->PrerequisiteRevisionPathRewrites->push_back(MakeYPathRewrite(
                prerequisitePath,
                prerequisiteResolveResult->RemoteNodeId,
                prerequisiteResolveResult->UnresolvedPathSuffix));
        }

        if (IsAlienType(TypeFromId(targetResolveResult->RemoteNodeId))) {
            MarkSubrequestLocal(subrequest);
            return;
        }

        MarkSubrequestRemoteCrossCell(subrequest, CellTagFromId(targetResolveResult->RemoteNodeId));
    }

    void DecideSubrequestTypes()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            DecideSubrequestType(&subrequest);
        }
    }

    void ValidateRequestsFeatures()
    {
        for (int index = 0; index < TotalSubrequestCount_; ++index) {
            const auto& subrequest = Subrequests_[index];
            if (subrequest.RpcContext) {
                Owner_->ValidateRequestFeatures(subrequest.RpcContext);
            }
        }
    }

    void CheckSubrequestsForRemoteTransactions()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto addSubrequestTransactions = [&] (
            std::vector<TTransactionId>* transactions,
            TSubrequest& subrequest,
            THashMap<TTransactionId, TCompactVector<TSubrequest*,1>>* transactionIdToSubrequests)
        {
            auto doTransaction = [&] (TTransactionId transactionId) {
                transactions->push_back(transactionId);

                if (transactionIdToSubrequests) {
                    auto& subrequests = (*transactionIdToSubrequests)[transactionId];
                    if (std::find(subrequests.begin(), subrequests.end(), &subrequest) == subrequests.end()) {
                        subrequests.push_back(&subrequest);
                    }
                }
            };

            doTransaction(GetTransactionId(subrequest.RequestHeader));

            for (const auto& prerequisite : subrequest.PrerequisitesExt->transactions()) {
                doTransaction(FromProto<TTransactionId>(prerequisite.transaction_id()));
            }
        };

        std::vector<TTransactionId> transactionsToReplicateWithoutBoomerangs;

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];

            // Phase one.
            if (subrequest.Type == EExecutionSessionSubrequestType::Undefined && subrequest.TentativelyRemote) {
                continue;
            }

            // Phase two.
            if (subrequest.Type == EExecutionSessionSubrequestType::Remote ||
                subrequest.Type == EExecutionSessionSubrequestType::Cache)
            {
                // Some non-tentatively-remote subrequests may have become remote.
                if (subrequest.RemoteTransactionReplicationSession) {
                    // Remove the session as it won't be used.
                    subrequest.RemoteTransactionReplicationSession.Reset();
                }

                continue;
            }

            YT_VERIFY(
                subrequest.Type == EExecutionSessionSubrequestType::Undefined ||
                subrequest.Type == EExecutionSessionSubrequestType::LocalRead ||
                subrequest.Type == EExecutionSessionSubrequestType::LocalWrite);

            if (subrequest.YPathExt->mutating()) {
                if (!subrequest.RemoteTransactionReplicationSession) {
                    // Pre-phase-one or previously-tentatively-remote-but-no-longer-remote subrequest.
                    std::vector<TTransactionId> writeSubrequestTransactions;
                    addSubrequestTransactions(&writeSubrequestTransactions, subrequest, nullptr);
                    subrequest.RemoteTransactionReplicationSession = New<TTransactionReplicationSessionWithBoomerangs>(
                        Bootstrap_,
                        std::move(writeSubrequestTransactions),
                        TInitiatorRequestLogInfo(RequestId_, subrequestIndex),
                        std::move(subrequest.Mutation));
                } else {
                    // Pre-phase-two.
                    subrequest.RemoteTransactionReplicationSession->SetMutation(std::move(subrequest.Mutation));
                }
            } else {
                addSubrequestTransactions(
                    &transactionsToReplicateWithoutBoomerangs,
                    subrequest,
                    &RemoteTransactionIdToSubrequests_);
            }
        }

        if (!RemoteTransactionReplicationSession_) {
            // Pre-phase-one.
            RemoteTransactionReplicationSession_ = New<TTransactionReplicationSessionWithoutBoomerangs>(
                Bootstrap_,
                std::move(transactionsToReplicateWithoutBoomerangs),
                TInitiatorRequestLogInfo(RequestId_));
        } else {
            // Pre-phase-two.
            RemoteTransactionReplicationSession_->Reset(std::move(transactionsToReplicateWithoutBoomerangs));
        }
    }

    TFuture<void> InvokeRemoteTransactionReplicationWithoutBoomerangs()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto future = RemoteTransactionReplicationSession_->InvokeReplicationRequests();
        if (!future) {
            return VoidFuture;
        }

        return future.Apply(BIND(
            [this, this_ = MakeStrong(this)]
            (const THashMap<TTransactionId, TFuture<void>>& transactionReplicationFutures) {
                VERIFY_THREAD_AFFINITY_ANY();

                THashMap<TSubrequest*, std::vector<TFuture<void>>> subrequestToRemoteTransactionReplicationFutures;

                for (auto& [transactionId, replicationFuture] : transactionReplicationFutures) {
                    YT_VERIFY(replicationFuture);

                    auto it = RemoteTransactionIdToSubrequests_.find(transactionId);
                    YT_VERIFY(it != RemoteTransactionIdToSubrequests_.end());
                    const auto& transactionSubrequests = it->second;
                    for (auto* subrequest : transactionSubrequests) {
                        subrequestToRemoteTransactionReplicationFutures[subrequest].push_back(replicationFuture);
                    }
                }

                for (const auto& [subrequest, replicationFutures] : subrequestToRemoteTransactionReplicationFutures) {
                    YT_VERIFY(!subrequest->RemoteTransactionReplicationFuture);
                    subrequest->RemoteTransactionReplicationFuture = AllSucceeded(std::move(replicationFutures));
                }
            }));
    }

    void RunSyncPhaseTwo()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Read subrequests may request their remote transaction replication
        // right away, as they will subscribe and wait for any such transaction
        // later.
        // For write subrequests, boomerangs must be launched after syncing with
        // the rest of the cells (because there's no other way to guarantee that
        // boomerangs will come back *after* the syncing is done).
        auto replicationFuture = InvokeRemoteTransactionReplicationWithoutBoomerangs();

        auto future = StartSync(ESyncPhase::Two, std::move(replicationFuture));
        if (future.IsSet()) {
            OnSyncPhaseTwoCompleted(future.Get());
        } else {
            future.Subscribe(BIND(&TExecuteSession::OnSyncPhaseTwoCompleted, MakeStrong(this)));
        }
    }

    void OnSyncPhaseTwoCompleted(const TError& error = {})
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!error.IsOK()) {
            Reply(error);
            return;
        }

        try {
            RunSyncPhaseThree();
        } catch (const std::exception& ex) {
            Reply(ex);
        }
    }

    void RunSyncPhaseThree()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto future = StartSync(ESyncPhase::Three);
        if (future.IsSet()) {
            // NB: sync-phase-three is usually no-op, so this is the common case.
            OnSyncPhaseThreeCompleted(future.Get());
        } else {
            future.Subscribe(BIND(&TExecuteSession::OnSyncPhaseThreeCompleted, MakeStrong(this)));
        }
    }

    void OnSyncPhaseThreeCompleted(const TError& error = {})
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!error.IsOK()) {
            Reply(error);
            return;
        }

        if (ContainsLocalSubrequests()) {
            LocalExecutionStarted_.store(true);
            Owner_->EnqueueReadySession(this);
        } else {
            ReleaseUltimateReplyLock();
            // NB: No finish is needed.
        }
    }

    bool ContainsLocalSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (int index = 0; index < TotalSubrequestCount_; ++index) {
            auto& subrequest = Subrequests_[index];
            if (subrequest.Type == EExecutionSessionSubrequestType::LocalRead ||
                subrequest.Type == EExecutionSessionSubrequestType::LocalWrite)
            {
                return true;
            }
        }
        return false;
    }

    void ForwardRemoteRequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        using TBatchKey = std::tuple<TCellTag, NApi::EMasterChannelKind>;
        struct TBatchValue
        {
            TObjectServiceProxy::TReqExecuteBatchBasePtr BatchReq;
            TCompactVector<int, 16> Indexes;
        };
        THashMap<TBatchKey, TBatchValue> batchMap;
        auto getOrCreateBatch = [&] (TCellTag cellTag, NApi::EMasterChannelKind channelKind) {
            auto key = std::tuple(cellTag, channelKind);
            auto it = batchMap.find(key);
            if (it == batchMap.end()) {
                TObjectServiceProxy proxy(
                    Bootstrap_->GetClusterConnection(),
                    channelKind,
                    cellTag,
                    /*stickyGroupSizeCache*/ nullptr);
                auto batchReq = proxy.ExecuteBatchNoBackoffRetries();
                batchReq->SetOriginalRequestId(RequestId_);
                batchReq->SetTimeout(ComputeForwardingTimeout(RpcContext_, Owner_->Config_));
                NRpc::SetAuthenticationIdentity(batchReq, RpcContext_->GetAuthenticationIdentity());

                it = batchMap.emplace(key, TBatchValue{
                    .BatchReq = std::move(batchReq)
                }).first;
            }
            return &it->second;
        };

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            if (subrequest.Type != EExecutionSessionSubrequestType::Remote) {
                continue;
            }

            const auto& requestHeader = subrequest.RequestHeader;
            const auto& ypathExt = *subrequest.YPathExt;
            auto channelKind = subrequest.YPathExt->mutating()
                ? NApi::EMasterChannelKind::Leader
                : NApi::EMasterChannelKind::Follower;

            auto* batch = getOrCreateBatch(subrequest.ForwardedCellTag, channelKind);
            batch->BatchReq->AddRequestMessage(subrequest.RemoteRequestMessage);
            batch->Indexes.push_back(subrequestIndex);

            AcquireReplyLock();

            YT_LOG_DEBUG("Forwarding object request (RequestId: %v -> %v, Method: %v.%v, "
                "%v%v%v%v, Mutating: %v, CellTag: %v, ChannelKind: %v)",
                RequestId_,
                batch->BatchReq->GetRequestId(),
                requestHeader.service(),
                requestHeader.method(),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (subrequest.TargetPathRewrite) {
                        builder->AppendFormat("TargetPath: %v, ", subrequest.TargetPathRewrite);
                    }
                }),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (subrequest.AdditionalPathRewrites && !subrequest.AdditionalPathRewrites->empty()) {
                        builder->AppendFormat("AdditionalPaths: %v, ", *subrequest.AdditionalPathRewrites);
                    }
                }),
                MakeFormatterWrapper([&] (auto* builder) {
                    if (subrequest.PrerequisiteRevisionPathRewrites && !subrequest.PrerequisiteRevisionPathRewrites->empty()) {
                        builder->AppendFormat("PrerequisiteRevisionPaths: %v, ", *subrequest.PrerequisiteRevisionPathRewrites);
                    }
                }),
                RpcContext_->GetAuthenticationIdentity(),
                ypathExt.mutating(),
                subrequest.ForwardedCellTag,
                channelKind);
        }

        for (auto& [cellTag, batch] : batchMap) {
            batch.BatchReq->Invoke().Subscribe(
                BIND([=, this, this_ = MakeStrong(this), batch = std::move(batch)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                    if (batchRspOrError.IsOK()) {
                        YT_LOG_DEBUG("Forwarded request succeeded (RequestId: %v -> %v, SubrequestIndexes: %v)",
                            RequestId_,
                            batch.BatchReq->GetRequestId(),
                            batch.Indexes);

                        const auto& batchRsp = batchRspOrError.Value();
                        for (auto index : batchRsp->GetUncertainRequestIndexes()) {
                            MarkSubrequestAsUncertain(batch.Indexes[index]);
                        }
                        for (int index = 0; index < batchRsp->GetSize(); ++index) {
                            auto* subrequest = &Subrequests_[batch.Indexes[index]];
                            if (subrequest->Uncertain.load()) {
                                continue;
                            }
                            auto responseMessage = batchRsp->GetResponseMessage(index);
                            if (responseMessage) {
                                subrequest->Revision = batchRsp->GetRevision(index);
                                OnSuccessfulSubresponse(subrequest, std::move(responseMessage));
                            } else {
                                OnMissingSubresponse(subrequest);
                            }
                        }
                    } else {
                        const auto& forwardingError = batchRspOrError;

                        YT_LOG_DEBUG(forwardingError, "Forwarded request failed (RequestId: %v -> %v, SubrequestIndexes: %v)",
                            RequestId_,
                            batch.BatchReq->GetRequestId(),
                            batch.Indexes);

                        if (!IsRetriableError(forwardingError) || forwardingError.FindMatching(NHydra::EErrorCode::ReadOnly)) {
                            YT_LOG_DEBUG(forwardingError, "Failing request due to non-retryable forwarding error (SubrequestIndexes: %v)",
                                batch.Indexes);
                            Reply(TError(NObjectClient::EErrorCode::ForwardedRequestFailed, "Forwarded request failed")
                                << forwardingError);
                            return;
                        }

                        YT_LOG_DEBUG(forwardingError, "Omitting subresponses due to retryable forwarding error (SubrequestIndexes: %v)",
                            batch.Indexes);

                        for (auto index : batch.Indexes) {
                            MarkSubrequestAsUncertain(index);
                        }
                    }
                }));
        }
    }

    void Reschedule()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!Enqueued_.exchange(true)) {
            Owner_->EnqueueReadySession(this);
        }
    }

    template <class T>
    void CheckAndReschedule(const TErrorOr<T>& result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!result.IsOK()) {
            Reply(result);
            return;
        }
        Reschedule();
    }

    bool WaitForAndContinue(TFuture<void> result)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (result.IsSet()) {
            result
                .Get()
                .ThrowOnError();
            return true;
        } else {
            result.Subscribe(
                BIND_NO_PROPAGATE(&TExecuteSession::CheckAndReschedule<void>, MakeStrong(this)));
            return false;
        }
    }

    bool AllSubrequestsProcessed() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_ASSERT(CurrentAutomatonSubrequestIndex_ <= TotalSubrequestCount_);
        YT_ASSERT(CurrentLocalReadSubrequestIndex_ <= TotalSubrequestCount_);

        if (CurrentAutomatonSubrequestIndex_ < TotalSubrequestCount_) {
            return false;
        }

        if (Owner_->Config_->EnableLocalReadExecutor && CurrentLocalReadSubrequestIndex_ < TotalSubrequestCount_) {
            return false;
        }

        return true;
    }

    bool GuardedRunAutomatonFast()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Owner_->ValidateClusterInitialized();

        const auto& hydraFacade = Bootstrap_->GetHydraFacade();
        const auto& hydraManager = hydraFacade->GetHydraManager();

        hydraManager->ValidatePeer(EPeerKind::LeaderOrFollower);

        if (!EpochAutomatonInvoker_) {
            EpochAutomatonInvoker_ = hydraFacade->GetEpochAutomatonInvoker(EAutomatonThreadQueue::ObjectService);
        }

        if (!EpochCancelableContext_) {
            EpochCancelableContext_ = hydraManager->GetAutomatonCancelableContext();
        }

        if (InterruptIfCanceled()) {
            return false;
        }

        if (ScheduleReplyIfNeeded()) {
            return false;
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();

        if (!User_) {
            auto* user = securityManager->GetUserByNameOrThrow(UserName_, /*activeLifeStageOnly*/ true);
            User_ = TEphemeralObjectPtr<TUser>(user);

            const auto& config = Owner_->GetDynamicConfig();
            if (config->EnableReadRequestComplexityLimits && user != securityManager->GetRootUser()) {
                ReadRequestComplexityLimits_ = TReadRequestComplexityLimits{
                    .Default = config->DefaultReadRequestComplexityLimits->ToReadRequestComplexity(),
                    .Max = config->MaxReadRequestComplexityLimits->ToReadRequestComplexity(),
                };

                const auto& userConfig = user->GetObjectServiceRequestLimits();

                userConfig
                    ->DefaultReadRequestComplexityLimits
                    ->ToReadRequestComplexityOverrides()
                    .ApplyTo(ReadRequestComplexityLimits_->Default);


                userConfig
                    ->MaxReadRequestComplexityLimits
                    ->ToReadRequestComplexityOverrides()
                    .ApplyTo(ReadRequestComplexityLimits_->Max);
            }
        }

        if (NeedsUserAccessValidation_) {
            NeedsUserAccessValidation_ = false;
            auto error = securityManager->CheckUserAccess(User_.Get());
            if (!error.IsOK()) {
                Owner_->SetStickyUserError(UserName_, error);
                THROW_ERROR error;
            }
        }

        if (!RequestQueueSizeIncreased_) {
            if (!securityManager->TryIncreaseRequestQueueSize(User_.Get())) {
                auto cellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
                auto error = TError(
                    NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded,
                    "User %Qv has exceeded its request queue size limit",
                    User_->GetName())
                    << TErrorAttribute("limit", User_->GetRequestQueueSizeLimit(cellTag))
                    << TErrorAttribute("cell_tag", cellTag);
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
        auto doThrottle = [&] (
            int* currentSubrequestIndex,
            int* throttledSubrequestIndex,
            EExecutionSessionSubrequestType subrequestType,
            EUserWorkloadType workloadType)
        {
            while (*currentSubrequestIndex < TotalSubrequestCount_ &&
                *currentSubrequestIndex > *throttledSubrequestIndex)
            {
                ++(*throttledSubrequestIndex);

                auto currentSubrequestType = Subrequests_[*currentSubrequestIndex].Type;
                if (currentSubrequestType != subrequestType) {
                    continue;
                }

                const auto& securityManager = Bootstrap_->GetSecurityManager();
                auto result = securityManager->ThrottleUser(User_.Get(), 1, workloadType);

                if (!WaitForAndContinue(result)) {
                    return false;
                }
            }

            return true;
        };

        if (!doThrottle(
            &CurrentAutomatonSubrequestIndex_,
            &ThrottledAutomatonSubrequestIndex_,
            EExecutionSessionSubrequestType::LocalWrite,
            EUserWorkloadType::Write))
        {
            return false;
        }

        if (!doThrottle(
            &CurrentLocalReadSubrequestIndex_,
            &ThrottledLocalReadSubrequestIndex_,
            EExecutionSessionSubrequestType::LocalRead,
            EUserWorkloadType::Read))
        {
            return false;
        }

        return true;
    }

    void GuardedRunAutomatonSlow()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto subrequestFilter = [this] (TSubrequest* subrequest) {
            return !EnableLocalReadExecutor_ || subrequest->Type != EExecutionSessionSubrequestType::LocalRead;
        };

        GuardedProcessSubrequests(&CurrentAutomatonSubrequestIndex_, subrequestFilter);
    }

    void GuardedRunRead()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto subrequestFilter = [this] (TSubrequest* subrequest) {
            return EnableLocalReadExecutor_ && subrequest->Type == EExecutionSessionSubrequestType::LocalRead;
        };

        GuardedProcessSubrequests(&CurrentLocalReadSubrequestIndex_, subrequestFilter);
    }

    void GuardedProcessSubrequests(
        int* currentSubrequestIndex,
        std::function<bool(TSubrequest*)> filter)
    {
        auto batchStartTime = GetCpuInstant();
        auto batchDeadlineTime = batchStartTime + DurationToCpuDuration(Owner_->Config_->YieldTimeout);

        Owner_->ValidateClusterInitialized();

        while (*currentSubrequestIndex < TotalSubrequestCount_) {
            if (InterruptIfCanceled()) {
                break;
            }

            if (ScheduleReplyIfNeeded()) {
                break;
            }

            if (!ThrottleRequests()) {
                break;
            }

            if (GetCpuInstant() > batchDeadlineTime) {
                YT_LOG_DEBUG("Yielding thread");
                Reschedule();
                break;
            }

            {
                auto guard = NThreading::TracelessTryGuard(LocalExecutionLock_);
                if (!guard.WasAcquired()) {
                    Reschedule();
                    break;
                }

                if (LocalExecutionInterrupted_.load()) {
                    break;
                }

                auto* subrequest = &Subrequests_[*currentSubrequestIndex];
                if (filter(subrequest) && !ExecuteSubrequest(subrequest)) {
                    break;
                }

                ++(*currentSubrequestIndex);
            }
        }

        if (AllSubrequestsProcessed()) {
            ReleaseUltimateReplyLock();
        }
    }

    bool ExecuteSubrequest(TSubrequest* subrequest)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        subrequest->Revision = hydraManager->GetAutomatonVersion().ToRevision();

        switch (subrequest->Type) {
            case EExecutionSessionSubrequestType::LocalRead:
            case EExecutionSessionSubrequestType::LocalWrite:
                return ExecuteLocalSubrequest(subrequest);

            case EExecutionSessionSubrequestType::Remote:
            case EExecutionSessionSubrequestType::Cache:
                return true;

            default:
                YT_ABORT();
        }
    }

    void OnMutationCommitted(TSubrequest* subrequest, const TErrorOr<TMutationResponse>& responseOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_ASSERT(subrequest->MutationResponseFuture.IsSet());

        if (!responseOrError.IsOK()) {
            Reply(responseOrError);
            return;
        }

        const auto& response = responseOrError.Value();
        const auto& context = subrequest->RpcContext;

        if (response.Origin != EMutationResponseOrigin::Commit) {
            YT_VERIFY(!context->IsReplied());
            // Either we're answering with a kept response or this is a boomerang mutation.
            context->SetRequestInfo();
            context->SetResponseInfo("KeptResponse: %v", true);
            context->Reply(response.Data);
        }

        WaitForSubresponse(subrequest);
    }

    void ForwardSubrequestToLeader(TSubrequest* subrequest)
    {
        YT_LOG_DEBUG("Performing leader fallback (RequestId: %v)",
            RequestId_);

        subrequest->ProfilingCounters->LeaderFallbackRequestCounter.Increment();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto asyncSubresponse = objectManager->ForwardObjectRequest(
            subrequest->RequestMessage,
            Bootstrap_->GetMulticellManager()->GetCellTag(),
            NApi::EMasterChannelKind::Leader);

        SubscribeToSubresponse(subrequest, std::move(asyncSubresponse));
    }

    bool ExecuteLocalSubrequest(TSubrequest* subrequest)
    {
        switch (subrequest->Type) {
            case EExecutionSessionSubrequestType::LocalWrite:
                VERIFY_THREAD_AFFINITY(AutomatonThread);
                break;
            case EExecutionSessionSubrequestType::LocalRead:
                VERIFY_THREAD_AFFINITY_ANY();
                break;
            default:
                YT_ABORT();
        }

        YT_VERIFY(!subrequest->RemoteTransactionReplicationFuture || !subrequest->MutationResponseFuture);

        if (!subrequest->LocallyStarted.exchange(true)) {
            AcquireReplyLock();
        }

        auto timeLeft = GetTimeLeft(subrequest);

        if (subrequest->RemoteTransactionReplicationSession && !subrequest->MutationResponseFuture)
        {
            YT_VERIFY(subrequest->Type == EExecutionSessionSubrequestType::LocalWrite);

            subrequest->MutationResponseFuture =
                subrequest->RemoteTransactionReplicationSession->InvokeReplicationRequests()
                .WithTimeout(timeLeft);
        }

        if (!subrequest->RemoteTransactionReplicationFuture && !subrequest->MutationResponseFuture) {
            // A local read subrequest with no remote transactions whatsoever.
            YT_VERIFY(subrequest->Type == EExecutionSessionSubrequestType::LocalRead);

            ExecuteReadSubrequest(subrequest);
            return true;
        }

        if (subrequest->RemoteTransactionReplicationFuture) {
            if (subrequest->RemoteTransactionReplicationFuture.IsSet()) {
                const auto& error = subrequest->RemoteTransactionReplicationFuture.Get();
                if (!error.IsOK()) {
                    subrequest->RpcContext->Reply(error);
                } else {
                    ExecuteReadSubrequest(subrequest);
                }
                return true;
            } else {
                auto doReschedule = [=, this, this_ = MakeStrong(this)] (const TError& error) {
                    if (!error.IsOK()) {
                        subrequest->RpcContext->Reply(error);
                        return;
                    }

                    Reschedule();
                };

                // NB: non-owning capture of this session object. Should be fine,
                // since reply lock will prevent this session from being destroyed.
                subrequest->RemoteTransactionReplicationFuture
                    .WithTimeout(timeLeft)
                    .Subscribe(BIND(doReschedule));

                return false;
            }
        } else {
            YT_VERIFY(subrequest->MutationResponseFuture);
            if (subrequest->MutationResponseFuture.IsSet()) {
                OnMutationCommitted(subrequest, subrequest->MutationResponseFuture.Get());
            } else {
                // NB: non-owning capture of this session object. Should be fine,
                // since reply lock will prevent this session from being destroyed.
                subrequest->MutationResponseFuture
                    .Subscribe(BIND(&TExecuteSession::OnMutationCommitted, MakeStrong(this), subrequest));
            }

            return true;
        }

        YT_ABORT();
    }

    TDuration GetTimeLeft(TSubrequest* subrequest)
    {
        const auto& requestHeader = subrequest->RequestHeader;
        auto timeout = FromProto<TDuration>(requestHeader.timeout());
        auto startTime = FromProto<TInstant>(requestHeader.start_time());
        auto now = NProfiling::GetInstant();
        return timeout - (now - startTime);
    }

    void ExecuteReadSubrequest(TSubrequest* subrequest)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TWallTimer timer;

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, User_.Get());

        const auto& rpcContext = subrequest->RpcContext;

        if (TraceContext_ && TraceContext_->IsRecorded()) {
            subrequest->TraceContext = TraceContext_->CreateChild(
                ConcatToString(TStringBuf("YPathRead:"), rpcContext->GetService(), TStringBuf("."), rpcContext->GetMethod()));
        }

        TCurrentTraceContextGuard traceContextGuard(subrequest->TraceContext);
        try {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto rootService = objectManager->GetRootService();

            if (ReadRequestComplexityLimits_) {
                auto explicitLimits = subrequest->ReadRequestComplexityOverrides;
                explicitLimits.Validate(ReadRequestComplexityLimits_->Max);

                auto limits = ReadRequestComplexityLimits_->Default;
                subrequest->ReadRequestComplexityOverrides.ApplyTo(limits);
                auto limiter = New<TReadRequestComplexityLimiter>(limits);
                rpcContext->SetReadRequestComplexityLimiter(std::move(limiter));
            }

            {
                NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
                ExecuteVerb(rootService, rpcContext);
            }

            WaitForSubresponse(subrequest);
        } catch (const TLeaderFallbackException&) {
            ForwardSubrequestToLeader(subrequest);
        }

        // NB: Even if the user was just removed the instance is still valid but not alive.
        if (!EpochCancelableContext_->IsCanceled()) {
            securityManager->ChargeUser(User_.Get(), {EUserWorkloadType::Read, 1, timer.GetElapsedTime()});
        }
    }

    void WaitForSubresponse(TSubrequest* subrequest)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Optimize for the (typical) case of synchronous response.
        const auto& context = subrequest->RpcContext;
        if (context->IsReplied()) {
            OnSuccessfulSubresponse(subrequest, context->GetResponseMessage());
        } else {
            SubscribeToSubresponse(subrequest, context->GetAsyncResponseMessage());
        }
    }

    void SubscribeToSubresponse(TSubrequest* subrequest, TFuture<TSharedRefArray> asyncSubresponseMessage)
    {
        VERIFY_THREAD_AFFINITY_ANY();

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

        OnSuccessfulSubresponse(subrequest, result.Value());
    }

    void MarkSubrequestAsUncertain(int index)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto& subrequest = Subrequests_[index];
        subrequest.Uncertain.store(true);
        ReleaseReplyLock();
    }

    void OnSuccessfulSubresponse(TSubrequest* subrequest, TSharedRefArray subresponseMessage)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (subrequest->TraceContext) {
            subrequest->TraceContext->Finish();
        }

        subrequest->ResponseMessage = std::move(subresponseMessage);
        subrequest->Completed.store(true);
        SomeSubrequestCompleted_.store(true);

        if (subrequest->CacheCookie) {
            Owner_->Cache_->EndLookup(
                RequestId_,
                std::move(*subrequest->CacheCookie),
                subrequest->ResponseMessage,
                subrequest->Revision,
                true);
        }

        ReleaseReplyLock();
    }

    void OnMissingSubresponse(TSubrequest* subrequest)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        // Missing subresponses are only possible for remote subrequests, and
        // there should be no trace contexts for them.
        YT_ASSERT(!subrequest->TraceContext);

        YT_VERIFY(!subrequest->Uncertain.load());
        YT_VERIFY(!subrequest->Completed.load());

        ReleaseReplyLock();
    }


    void Reply(const TError& error = TError())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Interrupt();

        if (!ReplyScheduled_.exchange(true)) {
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

        if (!error.IsOK()) {
            RpcContext_->Reply(error);
            return;
        }

        auto& request = RpcContext_->Request();
        auto& response = RpcContext_->Response();
        auto& attachments = response.Attachments();

        // Will take a snapshot.
        TCompactVector<int, 16> completedIndexes;
        TCompactVector<int, 16> uncertainIndexes;

        // Check for forwarding errors.
        for (auto index = 0; index < TotalSubrequestCount_; ++index) {
            auto& subrequest = Subrequests_[index];

            if (subrequest.Uncertain) {
                uncertainIndexes.push_back(index);
                continue;
            }

            if (!subrequest.Completed) {
                if (subrequest.LocallyStarted) {
                    // Possible for mutating subrequests because boomerangs are
                    // launched ASAP but reply locks are taken later.
                    uncertainIndexes.push_back(index);
                }

                continue;
            }

            const auto& subresponseMessage = subrequest.ResponseMessage;
            NRpc::NProto::TResponseHeader subresponseHeader;
            YT_VERIFY(TryParseResponseHeader(subresponseMessage, &subresponseHeader));

            auto subresponseErrorCode = FromProto<NObjectClient::EErrorCode>(subresponseHeader.error().code());
            if (subresponseErrorCode == NObjectClient::EErrorCode::ForwardedRequestFailed) {
                auto wrapperError = FromProto<TError>(subresponseHeader.error());
                YT_VERIFY(wrapperError.InnerErrors().size() == 1);

                const auto& forwardingError = wrapperError.InnerErrors()[0];
                if (!IsRetriableError(forwardingError) || forwardingError.FindMatching(NHydra::EErrorCode::ReadOnly)) {
                    YT_LOG_DEBUG(forwardingError, "Failing request due to non-retryable forwarding error (SubrequestIndex: %v)",
                        index);
                    RpcContext_->Reply(wrapperError);
                    return;
                }

                YT_LOG_DEBUG(forwardingError, "Omitting subresponse due to retryable forwarding error (SubrequestIndex: %v)",
                    index);

                uncertainIndexes.push_back(index);
            } else {
                completedIndexes.push_back(index);
            }
        }

        // COMPAT(babenko)
        if (!request.supports_portals()) {
            for (int index = 0; index < static_cast<int>(completedIndexes.size()); ++index) {
                if (completedIndexes[index] != index) {
                    completedIndexes.erase(completedIndexes.begin() + index, completedIndexes.end());
                    break;
                }
            }
        }

        for (int index : completedIndexes) {
            const auto& subrequest = Subrequests_[index];
            const auto& subresponseMessage = subrequest.ResponseMessage;
            attachments.insert(attachments.end(), subresponseMessage.Begin(), subresponseMessage.End());

            // COMPAT(babenko)
            response.add_part_counts(subresponseMessage.Size());
            response.add_revisions(subrequest.Revision);

            auto* subresponse = response.add_subresponses();
            subresponse->set_index(index);
            subresponse->set_part_count(subresponseMessage.Size());
            subresponse->set_revision(subrequest.Revision);
        }

        if (Owner_->EnableTwoLevelCache_) {
            response.set_caching_enabled(true);
        }

        ToProto(response.mutable_uncertain_subrequest_indexes(), uncertainIndexes);

        if (response.subresponses_size() == 0) {
            YT_LOG_DEBUG("Dropping request since no subresponses are available (RequestId: %v)",
                RequestId_);
            return;
        }

        RpcContext_->SetResponseInfo("SubresponseCount: %v, UncertainSubrequestIndexes: %v",
            response.subresponses_size(),
            response.uncertain_subrequest_indexes());

        RpcContext_->Reply();
    }


    void Interrupt()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!LocalExecutionInterrupted_.exchange(true)) {
            YT_LOG_DEBUG("Request interrupted (RequestId: %v)", RequestId_);
        }
    }

    void CancelPendingCacheSubrequests()
    {
        if (!Subrequests_) {
            return;
        }

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            if (subrequest.CacheCookie) {
                auto& cookie = *subrequest.CacheCookie;
                if (cookie.IsActive()) {
                    cookie.Cancel(TError(NYT::EErrorCode::Canceled, "Cache request canceled"));
                }
            }
        }
    }

    bool InterruptIfCanceled()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (RpcContext_->IsCanceled() || EpochCancelableContext_->IsCanceled()) {
            Interrupt();
            return true;
        } else {
            return false;
        }
    }

    void ScheduleBackoffAlarm()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!RpcContext_->Request().allow_backoff()) {
            return;
        }

        auto requestTimeout = RpcContext_->GetTimeout();
        if (requestTimeout && *requestTimeout > Owner_->Config_->TimeoutBackoffLeadTime) {
            auto backoffDelay = *requestTimeout - Owner_->Config_->TimeoutBackoffLeadTime;
            BackoffAlarmCookie_ = TDelayedExecutor::Submit(
                BIND(&TObjectService::TExecuteSession::OnBackoffAlarm, MakeWeak(this)),
                backoffDelay,
                TObjectService::GetRpcInvoker());
        }
    }

    void OnBackoffAlarm()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Backoff alarm triggered (RequestId: %v)", RequestId_);

        BackoffAlarmTriggered_.store(true);

        ScheduleReplyIfNeeded();
    }

    void AcquireReplyLock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int result = ++ReplyLockCount_;
        YT_VERIFY(result > 1);
        YT_LOG_TRACE("Reply lock acquired (LockCount: %v, RequestId: %v)",
            result,
            RequestId_);
    }

    bool ReleaseReplyLock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        int result = --ReplyLockCount_;
        YT_VERIFY(result >= 0);
        YT_LOG_TRACE("Reply lock released (LockCount: %v, RequestId: %v)",
            result,
            RequestId_);
        return ScheduleReplyIfNeeded();
    }

    bool ReleaseUltimateReplyLock()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (UltimateReplyLockReleased_.exchange(true)) {
            return false;
        }

        LocalExecutionInterrupted_.store(true);
        return ReleaseReplyLock();
    }

    bool ScheduleReplyIfNeeded()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (ReplyScheduled_.load()) {
            return true;
        }

        auto guard = NThreading::TracelessTryGuard(LocalExecutionLock_);
        if (!guard.WasAcquired()) {
            YT_LOG_DEBUG("Failed to acquire execution lock, backing off and retrying (RequestId: %v)",
                RequestId_);
            NConcurrency::TDelayedExecutor::Submit(
                BIND(IgnoreResult(&TObjectService::TExecuteSession::ScheduleReplyIfNeeded), MakeStrong(this)),
                Owner_->ScheduleReplyRetryBackoff_.load(),
                TObjectService::GetRpcInvoker());
            return false;
        }

        if (ReplyLockCount_.load() == 0) {
            Reply();
            return true;
        }

        if (BackoffAlarmTriggered_ && LocalExecutionStarted_ && SomeSubrequestCompleted_) {
            YT_LOG_DEBUG("Local execution interrupted due to backoff alarm (RequestId: %v)",
                RequestId_);
            LocalExecutionInterrupted_.store(true);
            return ReleaseUltimateReplyLock();
        }

        return false;
    }

    TCodicilGuard MakeCodicilGuard()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return TCodicilGuard(CodicilData_);
    }

    static bool GetSuppressUpstreamSync(const TCtxExecutePtr& rpcContext)
    {
        // COMPAT(shakurov): remove the former.
        return
            rpcContext->Request().suppress_upstream_sync() ||
            NObjectClient::GetSuppressUpstreamSync(rpcContext->RequestHeader());
    }

    static bool GetSuppressTransactionCoordinatorSync(const TCtxExecutePtr& rpcContext)
    {
        // COMPAT(shakurov): remove the former.
        return
            rpcContext->Request().suppress_transaction_coordinator_sync() ||
            NObjectClient::GetSuppressTransactionCoordinatorSync(rpcContext->RequestHeader());
    }
};

////////////////////////////////////////////////////////////////////////////////

TObjectService::TLocalReadCallbackProvider::TLocalReadCallbackProvider(TSessionSchedulerPtr sessionScheduler)
    : SessionScheduler_(std::move(sessionScheduler))
{ }

TCallback<void()> TObjectService::TLocalReadCallbackProvider::ExtractCallback()
{
    if (SessionScheduler_->IsEmpty()) {
        return {};
    }

    auto session = SessionScheduler_->Dequeue();
    TCurrentTraceContextGuard guard(session->GetTraceContext());

    return BIND(&TObjectService::TExecuteSession::RunRead, session);
}

////////////////////////////////////////////////////////////////////////////////

const TDynamicObjectServiceConfigPtr& TObjectService::GetDynamicConfig()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    return Bootstrap_->GetConfigManager()->GetConfig()->ObjectService;
}

void TObjectService::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& config = GetDynamicConfig();
    EnableTwoLevelCache_ = config->EnableTwoLevelCache;
    EnableLocalReadExecutor_ = config->EnableLocalReadExecutor && Config_->EnableLocalReadExecutor;
    ScheduleReplyRetryBackoff_ = config->ScheduleReplyRetryBackoff;

    LocalReadExecutor_->Reconfigure(config->LocalReadWorkerCount);
    LocalReadOffloadPool_->Configure(config->LocalReadOffloadThreadCount);
    ProcessSessionsExecutor_->SetPeriod(config->ProcessSessionsPeriod);
}

void TObjectService::EnqueueReadySession(TExecuteSessionPtr session)
{
    VERIFY_THREAD_AFFINITY_ANY();

    ReadySessions_.Enqueue(std::move(session));
}

void TObjectService::EnqueueFinishedSession(TExecuteSessionInfo sessionInfo)
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (sessionInfo.EpochCancelableContext) {
        FinishedSessionInfos_.Enqueue(std::move(sessionInfo));
    }
}

void TObjectService::ProcessSessions()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto startTime = GetCpuInstant();
    auto deadlineTime = startTime + DurationToCpuDuration(Config_->YieldTimeout);

    FinishedSessionInfos_.DequeueAll(false, [&] (const TExecuteSessionInfo& sessionInfo) {
        FinishSession(sessionInfo);
    });

    ReadySessions_.DequeueAll(false, [&] (TExecuteSessionPtr& session) {
        TCurrentTraceContextGuard guard(session->GetTraceContext());

        session->OnDequeued();

        if (!session->RunAutomatonFast()) {
            return;
        }

        const auto& userName = session->GetUserName();
        AutomatonSessionScheduler_->Enqueue(session, userName);
        if (Config_->EnableLocalReadExecutor) {
            LocalReadSessionScheduler_->Enqueue(session, userName);
        }
    });

    while (!AutomatonSessionScheduler_->IsEmpty() && GetCpuInstant() < deadlineTime) {
        auto session = AutomatonSessionScheduler_->Dequeue();
        TCurrentTraceContextGuard guard(session->GetTraceContext());

        session->RunAutomatonSlow();
    }

    // NB: Local read executor cannot be turned off in runtime since some requests can be already in it.
    if (Config_->EnableLocalReadExecutor) {
        auto quantumDuration = GetDynamicConfig()->LocalReadExecutorQuantumDuration;

        TAutomatonBlockGuard guard(Bootstrap_->GetHydraFacade());
        auto readFuture = LocalReadExecutor_->Run(quantumDuration);

        if (Config_->EnableLocalReadBusyWait) {
            // Busy wait is intended here to account local read time
            // into automaton thread CPU usage.
            while (!readFuture.IsSet())
            { }
        }

        readFuture
            .Get()
            .ThrowOnError();
    }
}

void TObjectService::FinishSession(const TExecuteSessionInfo& sessionInfo)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (sessionInfo.EpochCancelableContext->IsCanceled()) {
        return;
    }

    if (sessionInfo.RequestQueueSizeIncreased && IsObjectAlive(sessionInfo.User)) {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->DecreaseRequestQueueSize(sessionInfo.User.Get());
    }
}

void TObjectService::OnUserCharged(TUser* user, const TUserWorkload& workload)
{
    VERIFY_THREAD_AFFINITY_ANY();

    const auto& scheduler = workload.Type == EUserWorkloadType::Read && EnableLocalReadExecutor_
        ? LocalReadSessionScheduler_
        : AutomatonSessionScheduler_;

    scheduler->ChargeUser(user->GetName(), workload.RequestTime);
}

void TObjectService::SetStickyUserError(const TString& userName, const TError& error)
{
    VERIFY_THREAD_AFFINITY_ANY();

    StickyUserErrorCache_.Put(userName, error);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    Y_UNUSED(response);

    YT_LOG_ALERT_UNLESS(
        request->supports_portals(),
        "Received batch request without portals support (RequestId: %v)",
        context->GetRequestId());

    const auto& userName = context->GetAuthenticationIdentity().User;
    auto error = StickyUserErrorCache_.Get(userName);
    if (!error.IsOK()) {
        context->Reply(error);
        return;
    }

    New<TExecuteSession>(this, context)->RunRpc();
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
