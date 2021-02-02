#include "object_service.h"
#include "private.h"
#include "object_manager.h"
#include "config.h"
#include "helpers.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/master_hydra_service.h>
#include <yt/server/master/cell_master/multi_phase_cell_sync_session.h>

#include <yt/server/master/cypress_server/cypress_manager.h>
#include <yt/server/master/cypress_server/resolve_cache.h>

#include <yt/server/master/object_server/path_resolver.h>
#include <yt/server/master/object_server/request_profiling_manager.h>

#include <yt/server/master/security_server/security_manager.h>
#include <yt/server/master/security_server/user.h>

#include <yt/server/master/transaction_server/transaction_replication_session.h>

#include <yt/server/master/transaction_server/proto/transaction_manager.pb.h>

#include <yt/server/lib/hive/hive_manager.h>

#include <yt/server/lib/transaction_server/helpers.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/object_client/object_service_cache.h>
#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/transaction_client/helpers.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/rpc/helpers.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/service_detail.h>
#include <yt/core/rpc/dispatcher.h>

#include <yt/core/ytree/ypath_detail.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/misc/crash_handler.h>
#include <yt/core/misc/heap.h>
#include <yt/core/misc/lock_free.h>

#include <yt/core/actions/cancelable_context.h>

#include <yt/core/concurrency/spinlock.h>
#include <yt/core/concurrency/spinlock.h>

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
            Map_.emplace(userName, std::make_pair(error, now + ExpireTime_));
        }
    }

private:
    const TCpuDuration ExpireTime_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, Lock_);
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
        , Cache_(New<TObjectServiceCache>(
            Config_->MasterCache,
            GetNullMemoryUsageTracker(),
            ObjectServerLogger,
            ObjectServerProfiler.WithPrefix("/master_cache")))
        , StickyUserErrorCache_(Config_->StickyUserErrorExpireTime)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Execute)
            .SetQueueSizeLimit(100000)
            .SetConcurrencyLimit(100000)
            .SetCancelable(true)
            .SetInvoker(GetRpcInvoker())
            // Execute request handler needs request to remain alive after Reply call.
            .SetPooled(false));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GCCollect));

        DeclareServerFeature(EMasterFeature::OverlayedJournals);

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->SubscribeUserCharged(BIND(&TObjectService::OnUserCharged, MakeStrong(this)));

        const auto& configManager = Bootstrap_->GetConfigManager();
        configManager->SubscribeConfigChanged(BIND(&TObjectService::OnDynamicConfigChanged, MakeWeak(this)));

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        hydraManager->SubscribeAutomatonFollowerRecoveryComplete(BIND(&TObjectService::OnDynamicConfigChanged, MakeWeak(this), /*oldConfig*/ nullptr));
        hydraManager->SubscribeAutomatonLeaderRecoveryComplete(BIND(&TObjectService::OnDynamicConfigChanged, MakeWeak(this), /*oldConfig*/ nullptr));
    }

private:
    const TObjectServiceConfigPtr Config_;
    const IInvokerPtr AutomatonInvoker_;
    const TObjectServiceCachePtr Cache_;

    class TExecuteSession;
    using TExecuteSessionPtr = TIntrusivePtr<TExecuteSession>;

    struct TUserBucket
    {
        explicit TUserBucket(const TString& userName)
            : UserName(userName)
        { }

        TString UserName;
        TDuration ExcessTime;
        //! Typically equals ExcessTime; however when a user is charged we just update ExcessTime
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

    std::atomic<bool> ProcessSessionsCallbackEnqueued_ = false;

    TStickyUserErrorCache StickyUserErrorCache_;
    std::atomic<bool> EnableTwoLevelCache_ = false;
    std::atomic<bool> EnableMutationBoomerangs_ = true;
    std::atomic<TDuration> ScheduleReplyRetryBackoff_ = TDuration::MilliSeconds(100);

    static IInvokerPtr GetRpcInvoker()
    {
        return NRpc::TDispatcher::Get()->GetHeavyInvoker();
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr oldConfig = nullptr);
    void EnqueueReadySession(TExecuteSessionPtr session);
    void EnqueueFinishedSession(TExecuteSessionPtr session);

    void EnqueueProcessSessionsCallback();
    void ProcessSessions();

    TUserBucket* GetOrCreateBucket(const TString& userName);
    void OnUserCharged(TUser* user, const TUserWorkload& workload);

    void SetStickyUserError(const TString& userName, const TError& error);

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

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
        , TraceContext_(NTracing::GetCurrentTraceContext())
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
            !RpcContext_->Request().suppress_upstream_sync(),
            RequestId_))
          // Copy so it doesn't change mid-execution of this particular session.
        , EnableMutationBoomerangs_(Owner_->EnableMutationBoomerangs_)
    { }

    ~TExecuteSession()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG_IF(User_ && !Finished_, "User reference leaked due to unfinished request (RequestId: %v)",
            RequestId_);
        YT_LOG_DEBUG_IF(RequestQueueSizeIncreased_ && !Finished_, "Request queue size increment leaked due to unfinished request (RequestId: %v)",
            RequestId_);
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

    void Finish()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        Finished_ = true;

        if (!EpochCancelableContext_) {
            return;
        }

        if (EpochCancelableContext_->IsCanceled()) {
            return;
        }

        if (RequestQueueSizeIncreased_ && IsObjectAlive(User_)) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            securityManager->DecreaseRequestQueueSize(User_);
        }

        if (User_) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            objectManager->EphemeralUnrefObject(User_);
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

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr RpcContext_;
    const NTracing::TTraceContextPtr TraceContext_;

    NCellMaster::TBootstrap* const Bootstrap_;
    const int TotalSubrequestCount_;
    const TString& UserName_;
    const TRequestId RequestId_;
    const TString CodicilData_;
    const EPeerState TentativePeerState_;
    const TMultiPhaseCellSyncSessionPtr CellSyncSession_;
    const bool EnableMutationBoomerangs_;

    TDelayedExecutorCookie BackoffAlarmCookie_;

    struct TSubrequest
    {
        int Index = -1;
        bool TentativelyRemote = false;
        EExecutionSessionSubrequestType Type = EExecutionSessionSubrequestType::Undefined;
        IServiceContextPtr RpcContext;
        std::unique_ptr<TMutation> Mutation;
        std::optional<TObjectServiceCache::TCookie> CacheCookie;
        NRpc::NProto::TRequestHeader RequestHeader;
        const NYTree::NProto::TYPathHeaderExt* YPathExt = nullptr;
        const NObjectClient::NProto::TPrerequisitesExt* PrerequisitesExt = nullptr;
        const NObjectClient::NProto::TMulticellSyncExt* MulticellSyncExt = nullptr;
        TSharedRefArray RequestMessage;
        TCellTag ForwardedCellTag = InvalidCellTag;
        std::optional<TYPathRewrite> TargetPathRewrite;
        std::optional<SmallVector<TYPathRewrite, TypicalAdditionalPathCount>> AdditionalPathRewrites;
        std::optional<SmallVector<TYPathRewrite, 4>> PrerequisiteRevisionPathRewrites;
        TSharedRefArray RemoteRequestMessage;
        TSharedRefArray ResponseMessage;
        NTracing::TTraceContextPtr TraceContext;
        NHydra::TRevision Revision = NHydra::NullRevision;
        std::atomic<bool> Uncertain = false;
        std::atomic<bool> LocallyStarted = false;
        std::atomic<bool> Completed = false;
        TRequestProfilingCountersPtr ProfilingCounters;
        // Only for (local) write requests when boomerangs are enabled.
        // (Local read requests are handled by a session-wide replication session).
        TTransactionReplicationSessionWithBoomerangsPtr RemoteTransactionReplicationSession;
        // For local reads (and also local writes if boomerangs are disabled), this is a future
        // that will be set when all remote transactions have actually been replicated here.
        // Mutually exclusive with MutationResponseFuture.
        TFuture<void> RemoteTransactionReplicationFuture;
        // For local writes, this is a future that is set when the mutation is applied. That mutation
        // may be either committed in the ordinary fashion or posted as a boomerang.
        // Mutually exclusive with RemoteTransactionReplicationFuture.
        TFuture<TMutationResponse> MutationResponseFuture;
    };

    // For (local) read requests and, if boomerangs are disabled, for (local) write requests.
    // (Otherwise write requests are handled by per-subrequest replication sessions.)
    TTransactionReplicationSessionWithoutBoomerangsPtr RemoteTransactionReplicationSession_;

    THashMap<TTransactionId, SmallVector<TSubrequest*, 1>> RemoteTransactionIdToSubrequests_;

    std::unique_ptr<TSubrequest[]> Subrequests_;
    int CurrentSubrequestIndex_ = 0;
    int ThrottledSubrequestIndex_ = -1;

    IInvokerPtr EpochAutomatonInvoker_;
    TCancelableContextPtr EpochCancelableContext_;
    TUser* User_ = nullptr;
    bool NeedsUserAccessValidation_ = true;
    bool RequestQueueSizeIncreased_ = false;

    std::atomic<bool> ReplyScheduled_ = false;
    std::atomic<bool> FinishScheduled_ = false;
    bool Finished_ = false;

    std::atomic<bool> LocalExecutionStarted_ = false;
    std::atomic<bool> LocalExecutionInterrupted_ = false;
    // If this is locked, the automaton invoker is currently busy serving
    // some local subrequest.
    // NB: only TryAcquire() is called on this lock, never Acquire().
    YT_DECLARE_SPINLOCK(TAdaptiveLock, LocalExecutionLock_);

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
            request.suppress_upstream_sync(),
            request.suppress_transaction_coordinator_sync(),
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

            auto* ypathExt = requestHeader.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
            subrequest.YPathExt = ypathExt;

            subrequest.PrerequisitesExt = &requestHeader.GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);

            subrequest.MulticellSyncExt = &requestHeader.GetExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);

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

            if (subrequest.YPathExt->mutating()) {
                subrequest.ProfilingCounters->TotalWriteRequestCounter.Increment();
                subrequest.ProfilingCounters->TotalReadRequestCounter.Increment();
            }
        }
    }

    void LookupCachedSubrequests()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            auto& subrequest = Subrequests_[subrequestIndex];
            const auto& requestHeader = subrequest.RequestHeader;

            if (requestHeader.HasExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext)) {
                if (subrequest.YPathExt->mutating()) {
                    Reply(TError(NObjectClient::EErrorCode::CannotCacheMutatingRequest, "Mutating requests cannot be cached"));
                    return;
                }

                TObjectServiceCacheKey key(
                    Bootstrap_->GetCellTag(),
                    RpcContext_->GetAuthenticationIdentity().User,
                    subrequest.YPathExt->target_path(),
                    requestHeader.service(),
                    requestHeader.method(),
                    subrequest.RequestMessage[1]);

                YT_LOG_DEBUG("Serving subrequest from cache (RequestId: %v, SubrequestIndex: %v, Key: %v)",
                    RequestId_,
                    subrequestIndex,
                    key);

                const auto& cachingRequestHeaderExt = requestHeader.GetExtension(NYTree::NProto::TCachingHeaderExt::caching_header_ext);
                auto refreshRevision = cachingRequestHeaderExt.refresh_revision();
                auto cookie = Owner_->Cache_->BeginLookup(
                    RequestId_,
                    key,
                    FromProto<TDuration>(cachingRequestHeaderExt.success_expiration_time()),
                    FromProto<TDuration>(cachingRequestHeaderExt.failure_expiration_time()),
                    refreshRevision);

                if (cookie.IsActive()) {
                    subrequest.CacheCookie.emplace(std::move(cookie));
                } else {
                    subrequest.Type = EExecutionSessionSubrequestType::Cache;

                    AcquireReplyLock();

                    cookie.GetValue()
                        .Subscribe(BIND([this, this_ = MakeStrong(this), subrequestIndex] (const TErrorOr<TObjectServiceCacheEntryPtr>& entry) {
                            auto& subrequest = Subrequests_[subrequestIndex];
                            if (!entry.IsOK()) {
                                Reply(entry);
                                return;
                            }
                            const auto& value = entry.Value();
                            subrequest.Revision = value->GetRevision();
                            OnSuccessfullSubresponse(&subrequest, value->GetResponseMessage());
                        }));
                }
            }
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

        const auto& request = RpcContext_->Request();
        auto suppressTransactionCoordinatorSync = request.suppress_transaction_coordinator_sync();

        if (!suppressTransactionCoordinatorSync) {
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

            if (!suppressTransactionCoordinatorSync && subrequest.RemoteTransactionReplicationSession) {
                addCellTagsToSyncWith(
                    subrequest.RemoteTransactionReplicationSession->GetCellTagsToSyncWithBeforeInvocation());
            }

            for (auto cellTag : subrequest.MulticellSyncExt->cell_tags_to_sync_with()) {
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

        if (Owner_->EnableTwoLevelCache_) {
            LookupCachedSubrequests();
        }

        try {
            // Re-check remote requests to see if the cache resolve is still OK.
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
            MarkSubrequestRemoteIntraCell(subrequest);
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

    void MarkSubrequestRemoteIntraCell(TSubrequest* subrequest)
    {
        subrequest->ForwardedCellTag = Bootstrap_->GetMulticellManager()->GetCellTag();
        subrequest->RemoteRequestMessage = subrequest->RequestMessage;
        subrequest->Type = EExecutionSessionSubrequestType::Remote;
        subrequest->ProfilingCounters->IntraCellForwardingRequestCounter.Increment();
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
            targetResolveResult->PortalExitId,
            targetResolveResult->UnresolvedPathSuffix);

        subrequest->AdditionalPathRewrites.emplace();
        subrequest->AdditionalPathRewrites->reserve(subrequest->YPathExt->additional_paths_size());
        for (const auto& additionalPath : subrequest->YPathExt->additional_paths()) {
            auto additionalResolveResult = resolveCache->TryResolve(additionalPath);
            if (!additionalResolveResult) {
                MarkSubrequestLocal(subrequest);
                return;
            }

            if (CellTagFromId(additionalResolveResult->PortalExitId) != CellTagFromId(targetResolveResult->PortalExitId)) {
                MarkSubrequestLocal(subrequest);
                return;
            }

            subrequest->AdditionalPathRewrites->push_back(MakeYPathRewrite(
                additionalPath,
                additionalResolveResult->PortalExitId,
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

            if (CellTagFromId(prerequisiteResolveResult->PortalExitId) != CellTagFromId(targetResolveResult->PortalExitId)) {
                MarkSubrequestLocal(subrequest);
                return;
            }

            subrequest->PrerequisiteRevisionPathRewrites->push_back(MakeYPathRewrite(
                prerequisitePath,
                prerequisiteResolveResult->PortalExitId,
                prerequisiteResolveResult->UnresolvedPathSuffix));
        }

        MarkSubrequestRemoteCrossCell(subrequest, CellTagFromId(targetResolveResult->PortalExitId));
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
            THashMap<TTransactionId, SmallVector<TSubrequest*,1>>* transactionIdToSubrequests)
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
                // Some non-tentatively remote subrequests may have become remote.
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

            if (EnableMutationBoomerangs_ && subrequest.YPathExt->mutating()) {
                if (!subrequest.RemoteTransactionReplicationSession) {
                    // Pre-phase-one or prevously-tentatively-remote-but-no-longer-remote subrequest.
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

        // Read subrequests may request their remote transaction replication right away,
        // as they will subscribe and wait for any such transaction later.
        // Write subrequests may do the same if boomerang mutations are disabled.
        // Otherwise boomerangs must be launched after syncing with the rest of the cells
        // (because there's no other way to guarantee that boomerangs will come back *after*
        // the syncing is done).
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

        using TBatchKey = std::tuple<TCellTag, EPeerKind>;
        struct TBatchValue
        {
            TObjectServiceProxy::TReqExecuteBatchBasePtr BatchReq;
            SmallVector<int, 16> Indexes;
        };
        THashMap<TBatchKey, TBatchValue> batchMap;
        auto getOrCreateBatch = [&] (TCellTag cellTag, EPeerKind peerKind) {
            auto key = std::make_tuple(cellTag, peerKind);
            auto it = batchMap.find(key);
            if (it == batchMap.end()) {
                const auto& connection = Bootstrap_->GetClusterConnection();
                auto cellId = connection->GetMasterCellId(cellTag);

                const auto& cellDirectory = Bootstrap_->GetCellDirectory();
                auto channel = cellDirectory->GetChannelOrThrow(cellId, peerKind);

                TObjectServiceProxy proxy(std::move(channel));
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
            auto peerKind = subrequest.YPathExt->mutating() ? EPeerKind::Leader : EPeerKind::Follower;

            auto* batch = getOrCreateBatch(subrequest.ForwardedCellTag, peerKind);
            batch->BatchReq->AddRequestMessage(subrequest.RemoteRequestMessage);
            batch->Indexes.push_back(subrequestIndex);

            AcquireReplyLock();

            YT_LOG_DEBUG("Forwarding object request (RequestId: %v -> %v, Method: %v.%v, "
                "%v%v%v%v, Mutating: %v, CellTag: %v, PeerKind: %v)",
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
                peerKind);
        }

        for (auto& [cellTag, batch] : batchMap) {
            batch.BatchReq->Invoke().Subscribe(
                BIND([=, this_ = MakeStrong(this), batch = std::move(batch)] (const TObjectServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError) {
                    if (batchRspOrError.IsOK()) {
                        YT_LOG_DEBUG("Forwarded request succeeded (RequestId: %v -> %v, SubrequestIndexes: %v)",
                            RequestId_,
                            batch.BatchReq->GetRequestId(),
                            batch.Indexes);

                        const auto& batchRsp = batchRspOrError.Value();
                        for (int index = 0; index < batchRsp->GetSize(); ++index) {
                            auto responseMessage = batchRsp->GetResponseMessage(index);
                            if (!responseMessage) {
                                continue;
                            }
                            auto& subrequest = Subrequests_[batch.Indexes[index]];
                            subrequest.Revision = batchRsp->GetRevision(index);
                            OnSuccessfullSubresponse(&subrequest, std::move(responseMessage));
                        }
                        for (auto index : batchRsp->GetUncertainRequestIndexes()) {
                            MarkSubrequestAsUncertain(batch.Indexes[index]);
                        }
                    } else {
                        const auto& forwardingError = batchRspOrError;

                        YT_LOG_DEBUG(forwardingError, "Forwarded request failed (RequestId: %v -> %v, SubrequestIndexes: %v)",
                            RequestId_,
                            batch.BatchReq->GetRequestId(),
                            batch.Indexes);

                        if (!IsRetriableError(forwardingError)) {
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

        Owner_->EnqueueReadySession(this);
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
                BIND(&TExecuteSession::CheckAndReschedule<void>, MakeStrong(this)));
            return false;
        }
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

        if (ScheduleFinishIfCanceled()) {
            return false;
        }

        if (ScheduleReplyIfNeeded()) {
            return false;
        }

        // NB: Acquisitions are only possible if the current epoch is not canceled.
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        if (!User_) {
            User_ = securityManager->GetUserByNameOrThrow(UserName_, true /*activeLifeStageOnly*/);
            objectManager->EphemeralRefObject(User_);
        }

        if (NeedsUserAccessValidation_) {
            NeedsUserAccessValidation_ = false;
            auto error = securityManager->CheckUserAccess(User_);
            if (!error.IsOK()) {
                Owner_->SetStickyUserError(UserName_, error);
                THROW_ERROR error;
            }
        }

        if (!RequestQueueSizeIncreased_) {
            if (!securityManager->TryIncreaseRequestQueueSize(User_)) {
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        while (CurrentSubrequestIndex_ < TotalSubrequestCount_ &&
               CurrentSubrequestIndex_ > ThrottledSubrequestIndex_)
        {
            ++ThrottledSubrequestIndex_;

            const auto& securityManager = Bootstrap_->GetSecurityManager();
            auto subrequestType = Subrequests_[CurrentSubrequestIndex_].Type;
            if (subrequestType != EExecutionSessionSubrequestType::LocalRead &&
                subrequestType != EExecutionSessionSubrequestType::LocalWrite)
            {
                continue;
            }

            auto workloadType = subrequestType == EExecutionSessionSubrequestType::LocalWrite
                ? EUserWorkloadType::Write
                : EUserWorkloadType::Read;
            auto result = securityManager->ThrottleUser(User_, 1, workloadType);

            if (!WaitForAndContinue(result)) {
                return false;
            }
        }
        return true;
    }

    void GuardedRunAutomatonSlow()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto batchStartTime = GetCpuInstant();
        auto batchDeadlineTime = batchStartTime + DurationToCpuDuration(Owner_->Config_->YieldTimeout);

        Owner_->ValidateClusterInitialized();

        while (CurrentSubrequestIndex_ < TotalSubrequestCount_) {
            if (ScheduleFinishIfCanceled()) {
                break;
            }

            if (ScheduleReplyIfNeeded()) {
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
                TTryGuard guard(LocalExecutionLock_);
                if (!guard.WasAcquired()) {
                    Reschedule();
                    break;
                }

                if (LocalExecutionInterrupted_.load()) {
                    break;
                }

                ExecuteCurrentSubrequest();

                ++CurrentSubrequestIndex_;
            }
        }

        if (CurrentSubrequestIndex_ >= TotalSubrequestCount_) {
            ReleaseUltimateReplyLock();
        }
    }
    void ExecuteCurrentSubrequest()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto& subrequest = Subrequests_[CurrentSubrequestIndex_];

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        subrequest.Revision = hydraManager->GetAutomatonVersion().ToRevision();

        switch (subrequest.Type) {
            case EExecutionSessionSubrequestType::LocalRead:
            case EExecutionSessionSubrequestType::LocalWrite:
                ExecuteLocalSubrequest(&subrequest);
                break;

            case EExecutionSessionSubrequestType::Remote:
                break;

            case EExecutionSessionSubrequestType::Cache:
                break;

            default:
                YT_ABORT();
        }
    }

    // NB: this method is only used when boomerangs are disabled.
    void ExecuteWriteSubrequest(TSubrequest* subrequest)
    {
        subrequest->MutationResponseFuture = subrequest->Mutation->Commit();
        subrequest->MutationResponseFuture.AsVoid().Subscribe(
            BIND(&TExecuteSession::OnMutationCommitted, MakeStrong(this), subrequest));
    }

    void OnMutationCommitted(TSubrequest* subrequest, const TError& /*error*/)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_ASSERT(subrequest->MutationResponseFuture.IsSet());
        const auto& responseOrError = subrequest->MutationResponseFuture.Get();

        if (!responseOrError.IsOK()) {
            Reply(responseOrError);
            return;
        }

        const auto& response = responseOrError.Value();
        const auto& context = subrequest->RpcContext;

        if (response.Origin != EMutationResponseOrigin::Commit) {
            YT_VERIFY(!context->IsReplied());
            // Either we're answering with a kept response or this is a boomerang mutation.
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
            EPeerKind::Leader);

        SubscribeToSubresponse(subrequest, std::move(asyncSubresponse));
    }

    void ExecuteLocalSubrequest(TSubrequest* subrequest)
    {
        YT_ASSERT(
            subrequest->Type == EExecutionSessionSubrequestType::LocalRead ||
            subrequest->Type == EExecutionSessionSubrequestType::LocalWrite);

        YT_VERIFY(!subrequest->RemoteTransactionReplicationFuture || !subrequest->MutationResponseFuture);

        AcquireReplyLock();
        subrequest->LocallyStarted.store(true);

        if (subrequest->RemoteTransactionReplicationSession &&
            !subrequest->MutationResponseFuture)
        {
            YT_VERIFY(EnableMutationBoomerangs_);
            YT_VERIFY(subrequest->Type == EExecutionSessionSubrequestType::LocalWrite);

            subrequest->MutationResponseFuture = subrequest->RemoteTransactionReplicationSession->InvokeReplicationRequests();
        }

        auto doExecuteSubrequest = [=, this_ = MakeStrong(this)] (const TError& error) {
            if (!error.IsOK()) {
                subrequest->RpcContext->Reply(error);
                return;
            }

            try {
                if (subrequest->Type == EExecutionSessionSubrequestType::LocalRead) {
                    ExecuteReadSubrequest(subrequest);
                } else {
                    ExecuteWriteSubrequest(subrequest);
                }
            } catch (const std::exception& ex) {
                Reply(ex);
            }
        };

        if (!subrequest->RemoteTransactionReplicationFuture && !subrequest->MutationResponseFuture) {
            doExecuteSubrequest(TError());
            return;
        }

        auto timeLeft = GetTimeLeft(subrequest);

        if (subrequest->RemoteTransactionReplicationFuture) {
            if (subrequest->RemoteTransactionReplicationFuture.IsSet()) {
                doExecuteSubrequest(subrequest->RemoteTransactionReplicationFuture.Get());
            } else {
                // NB: non-owning capture of this session object. Should be fine,
                // since reply lock will prevent this session from being destroyed.
                subrequest->RemoteTransactionReplicationFuture
                    .WithTimeout(timeLeft)
                    .Subscribe(BIND(doExecuteSubrequest));
            }
        } else {
            YT_VERIFY(subrequest->MutationResponseFuture);
            if (subrequest->MutationResponseFuture.IsSet()) {
                OnMutationCommitted(subrequest, {});
            } else {
                // NB: non-owning capture of this session object. Should be fine,
                // since reply lock will prevent this session from being destroyed.
                subrequest->MutationResponseFuture
                    .AsVoid()
                    .WithTimeout(timeLeft)
                    .Subscribe(BIND(&TExecuteSession::OnMutationCommitted, MakeStrong(this), subrequest));
            }
        }
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
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TWallTimer timer;

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, User_);

        const auto& rpcContext = subrequest->RpcContext;

        if (TraceContext_) {
            subrequest->TraceContext = NTracing::CreateChildTraceContext(
                TraceContext_,
                ConcatToString(TStringBuf("YPathRead:"), rpcContext->GetService(), TStringBuf("."), rpcContext->GetMethod()));
        }
        NTracing::TCurrentTraceContextGuard traceContextGuard(subrequest->TraceContext);

        try {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto rootService = objectManager->GetRootService();
            ExecuteVerb(rootService, rpcContext);

            WaitForSubresponse(subrequest);
        } catch (const TLeaderFallbackException&) {
            ForwardSubrequestToLeader(subrequest);
        }

        // NB: Even if the user was just removed the instance is still valid but not alive.
        if (!EpochCancelableContext_->IsCanceled()) {
            securityManager->ChargeUser(User_, {EUserWorkloadType::Read, 1, timer.GetElapsedTime()});
        }
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

        OnSuccessfullSubresponse(subrequest, result.Value());
    }

    void MarkSubrequestAsUncertain(int index)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto& subrequest = Subrequests_[index];
        subrequest.Uncertain.store(true);
        ReleaseReplyLock();
    }

    void OnSuccessfullSubresponse(TSubrequest* subrequest, TSharedRefArray subresponseMessage)
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


    void Reply(const TError& error = TError())
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ScheduleFinish();

        bool expected = false;
        if (ReplyScheduled_.compare_exchange_strong(expected, true)) {
            LocalExecutionInterrupted_.store(true);
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
        SmallVector<int, 16> completedIndexes;
        SmallVector<int, 16> uncertainIndexes;

        // Check for forwarding errors.
        for (auto index = 0; index < TotalSubrequestCount_; ++index) {
            auto& subrequest = Subrequests_[index];
            if (!subrequest.Completed) {
                if (subrequest.LocallyStarted) {
                    // Possible for mutating subrequests when boomerangs are
                    // enabled as those boomerangs are launched ASAP but reply
                    // locks are taken later.
                    uncertainIndexes.push_back(index);
                }

                continue;
            }

            if (subrequest.Uncertain) {
                uncertainIndexes.push_back(index);
                continue;
            }

            const auto& subresponseMessage = subrequest.ResponseMessage;
            NRpc::NProto::TResponseHeader subresponseHeader;
            YT_VERIFY(TryParseResponseHeader(subresponseMessage, &subresponseHeader));

            if (subresponseHeader.error().code() == NObjectClient::EErrorCode::ForwardedRequestFailed) {
                auto wrapperError = FromProto<TError>(subresponseHeader.error());
                YT_VERIFY(wrapperError.InnerErrors().size() == 1);

                const auto& forwardingError = wrapperError.InnerErrors()[0];
                if (!IsRetriableError(forwardingError)) {
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


    void ScheduleFinish()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        bool expected = false;
        if (FinishScheduled_.compare_exchange_strong(expected, true)) {
            LocalExecutionInterrupted_.store(true);
            Owner_->EnqueueFinishedSession(this);
        }
    }

    bool ScheduleFinishIfCanceled()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (RpcContext_->IsCanceled() || EpochCancelableContext_->IsCanceled()) {
            ScheduleFinish();
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
                BIND(&TObjectService::TExecuteSession::OnBackoffAlarm, MakeStrong(this)),
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

        auto expected = false;
        if (!UltimateReplyLockReleased_.compare_exchange_strong(expected, true)) {
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

        TTryGuard guard(LocalExecutionLock_);
        if (!guard.WasAcquired()) {
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
};

////////////////////////////////////////////////////////////////////////////////

void TObjectService::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& config = Bootstrap_->GetConfigManager()->GetConfig()->ObjectService;
    EnableTwoLevelCache_ = config->EnableTwoLevelCache;
    EnableMutationBoomerangs_ = config->EnableMutationBoomerangs;
    ScheduleReplyRetryBackoff_ = config->ScheduleReplyRetryBackoff;
}

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
        if (!session->RunAutomatonFast()) {
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
        session->RunAutomatonSlow();
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
    bucket->ExcessTime = actualExcessTime + workload.RequestTime;
}

void TObjectService::SetStickyUserError(const TString& userName, const TError& error)
{
    StickyUserErrorCache_.Put(userName, error);
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TObjectService, Execute)
{
    Y_UNUSED(request);
    Y_UNUSED(response);

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
