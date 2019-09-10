#include "object_service.h"
#include "private.h"
#include "object_manager.h"
#include "config.h"
#include "helpers.h"

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

#include <yt/ytlib/transaction_client/helpers.h>

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

#include <yt/core/profiling/profile_manager.h>

#include <atomic>
#include <queue>

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
using namespace NObjectServer;
using namespace NObjectClient;
using namespace NHiveServer;
using namespace NCellMaster;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = ObjectServerProfiler;

////////////////////////////////////////////////////////////////////////////////

struct TRequestProfilngCounters
    : public TIntrinsicRefCounted
{
    explicit TRequestProfilngCounters(const NProfiling::TTagIdList& tagIds)
        : TotalReadRequestCounter("/total_read_request_count", tagIds)
        , TotalWriteRequestCounter("/total_write_request_count", tagIds)
        , LocalReadRequestCounter("/local_read_request_count", tagIds)
        , LocalWriteRequestCounter("/local_write_request_count", tagIds)
        , LeaderFallbackRequestCounter("/leader_fallback_request_count", tagIds)
        , IntraCellForwardingRequestCounter("/intra_cell_forwarding_request_count", tagIds)
        , CrossCellForwardingRequestCounter("/cross_cell_forwarding_request_count", tagIds)
        , LocalMutationScheduleTimeCounter("/local_mutation_schedule_time", tagIds)
    { }

    TMonotonicCounter TotalReadRequestCounter;
    TMonotonicCounter TotalWriteRequestCounter;
    TMonotonicCounter LocalReadRequestCounter;
    TMonotonicCounter LocalWriteRequestCounter;
    TMonotonicCounter LeaderFallbackRequestCounter;
    TMonotonicCounter IntraCellForwardingRequestCounter;
    TMonotonicCounter CrossCellForwardingRequestCounter;
    TMonotonicCounter LocalMutationScheduleTimeCounter;
};

using TRequestProfilngCountersPtr = TIntrusivePtr<TRequestProfilngCounters>;

class TRequestProfilingManager
{
public:
    TRequestProfilngCountersPtr GetCounters(const TString& user, const TString& method)
    {
        auto key = std::make_tuple(user, method);

        {
            NConcurrency::TReaderGuard guard(Lock_);
            if (auto it = KeyToCounters_.find(key)) {
                return it->second;
            }
        }

        TTagIdList tagIds{
            TProfileManager::Get()->RegisterTag("user", user),
            TProfileManager::Get()->RegisterTag("method", method)
        };
        auto counters = New<TRequestProfilngCounters>(tagIds);

        {
            NConcurrency::TWriterGuard guard(Lock_);
            auto [it, inserted] = KeyToCounters_.emplace(key, std::move(counters));
            return it->second;
        }
    }

private:
    // (user, method)
    using TKey = std::tuple<TString, TString>;
    NConcurrency::TReaderWriterSpinLock Lock_;
    THashMap<TKey, TRequestProfilngCountersPtr> KeyToCounters_;
};

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
    TRequestProfilingManager ProfilingStatisticsManager_;


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
        , Bootstrap_(Owner_->Bootstrap_)
        , TotalSubrequestCount_(RpcContext_->Request().part_counts_size())
        , UserName_(RpcContext_->GetUser())
        , RequestId_(RpcContext_->GetRequestId())
        , CodicilData_(Format("RequestId: %v, User: %v",
            RequestId_,
            UserName_))
        , TentativePeerState_(Bootstrap_->GetHydraFacade()->GetHydraManager()->GetTentativeState())
    { }

    ~TExecuteSession()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG_IF(User_ && !Finished_, "User reference eleaked due to unfinished request (RequestId: %v)",
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
    }

private:
    const TObjectServicePtr Owner_;
    const TCtxExecutePtr RpcContext_;

    NCellMaster::TBootstrap* const Bootstrap_;
    const int TotalSubrequestCount_;
    const TString& UserName_;
    const TRequestId RequestId_;
    const TString CodicilData_;
    const EPeerState TentativePeerState_;

    TDelayedExecutorCookie BackoffAlarmCookie_;

    struct TSubrequest
    {
        int Index = -1;
        bool TentativelyRemote = false;
        EExecutionSessionSubrequestType Type = EExecutionSessionSubrequestType::Undefined;
        IServiceContextPtr RpcContext;
        std::unique_ptr<TMutation> Mutation;
        TFuture<TMutationResponse> AsyncCommitResult;
        NRpc::NProto::TRequestHeader RequestHeader;
        const NYTree::NProto::TYPathHeaderExt* YPathExt = nullptr;
        const NObjectClient::NProto::TPrerequisitesExt* PrerequisitesExt  = nullptr;
        TSharedRefArray RequestMessage;
        TCellTag ForwardedCellTag = InvalidCellTag;
        std::optional<TYPathRewrite> TargetPathRewrite;
        std::optional<SmallVector<TYPathRewrite, TypicalAdditionalPathCount>> AdditionalPathRewrites;
        std::optional<SmallVector<TYPathRewrite, 4>> PrerequisiteRevisionPathRewrites;
        TSharedRefArray RemoteRequestMessage;
        TSharedRefArray ResponseMessage;
        NTracing::TTraceContextPtr TraceContext;
        NHydra::TRevision Revision = NHydra::NullRevision;
        bool Uncertain = false;
        std::atomic<bool> Completed = {false};
        TRequestProfilngCountersPtr ProfilingCounters;
    };

    std::unique_ptr<TSubrequest[]> Subrequests_;
    int CurrentSubrequestIndex_ = 0;
    int ThrottledSubrequestIndex_ = -1;

    IInvokerPtr EpochAutomatonInvoker_;
    TCancelableContextPtr EpochCancelableContext_;
    TUser* User_ = nullptr;
    TCellTagList SyncedWithCellTags_;
    bool NeedsUserAccessValidation_ = true;
    bool RequestQueueSizeIncreased_ = false;

    std::atomic<bool> ReplyScheduled_ = {false};
    std::atomic<bool> FinishScheduled_ = {false};
    bool Finished_ = false;

    std::atomic<bool> LocalExecutionStarted_ = {false};
    std::atomic<bool> LocalExecutionInterrupted_ = {false};
    // If this is locked, the automaton invoker is currently busy serving
    // some local subrequest.
    // NB: only TryAcquire() is called on this lock, never Acquire().
    TSpinLock LocalExecutionLock_;

    // Has the time to backoff come?
    std::atomic<bool> BackoffAlarmTriggered_ = {false};

    // Once this drops to zero, the request can be replied.
    // Starts with one to indicate that the "ultimate" lock is initially held.
    std::atomic<int> ReplyLockCount_ = {1};

    // Set to true when the "ultimate" reply lock is released and
    // RepyLockCount_ is decremented.
    std::atomic<bool> UltimateReplyLockReleased_ = {false};

    // Set to true if we're ready to reply with at least one subresponse.
    std::atomic<bool> SomeSubrequestCompleted_ = {false};

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    const NLogging::TLogger& Logger = ObjectServerLogger;


    void GuardedRunRpc()
    {
        auto codicilGuard = MakeCodicilGuard();

        const auto& request = RpcContext_->Request();

        auto originalRequestId = FromProto<TRequestId>(request.original_request_id());

        RpcContext_->SetRequestInfo("SubrequestCount: %v, SupportsPortals: %v, SuppressUpstreamSync: %v, OriginalRequestId: %v",
            TotalSubrequestCount_,
            request.supports_portals(),
            request.suppress_upstream_sync(),
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
            subrequest.Index =  subrequestIndex;

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

            subrequest.ProfilingCounters = Owner_->ProfilingStatisticsManager_.GetCounters(
                UserName_,
                requestHeader.method());

            // Propagate various parameters to the subrequest.
            if (!requestHeader.has_request_id()) {
                ToProto(requestHeader.mutable_request_id(), RequestId_);
            }
            if (RpcContext_->IsRetry()) {
                requestHeader.set_retry(true);
            }
            if (!requestHeader.has_user()) {
                requestHeader.set_user(UserName_);
            }
            if (!requestHeader.has_timeout()) {
                requestHeader.set_timeout(ToProto<i64>(RpcContext_->GetTimeout().value_or(Owner_->Config_->DefaultExecuteTimeout)));
            }
            if (!requestHeader.has_start_time()) {
                requestHeader.set_start_time(ToProto<ui64>(RpcContext_->GetStartTime().value_or(now)));
            }

            auto* ypathExt = requestHeader.MutableExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
            subrequest.YPathExt = ypathExt;

            const auto& prerequisitesExt = requestHeader.GetExtension(NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
            subrequest.PrerequisitesExt = &prerequisitesExt;

            // Store original path.
            ypathExt->set_original_target_path(ypathExt->target_path());
            *ypathExt->mutable_original_additional_paths() = ypathExt->additional_paths();

            // COMPAT(savrus) Support old mount/unmount/etc interface.
            if (ypathExt->mutating() && (requestHeader.method() == "Mount" ||
                requestHeader.method() == "Unmount" ||
                requestHeader.method() == "Freeze" ||
                requestHeader.method() == "Unfreeze" ||
                requestHeader.method() == "Remount" ||
                requestHeader.method() == "Reshard"))
            {
                ypathExt->set_mutating(false);
                SetSuppressAccessTracking(&requestHeader, true);
            }

            subrequest.RequestMessage = SetRequestHeader(subrequest.RequestMessage, requestHeader);
            subrequest.TraceContext = NRpc::CreateCallTraceContext(
                requestHeader.service(),
                requestHeader.method());

            Profiler.Increment(subrequest.YPathExt->mutating()
                ? subrequest.ProfilingCounters->TotalWriteRequestCounter
                : subrequest.ProfilingCounters->TotalReadRequestCounter);
        }
    }

    TCellTagList CollectCellsToSyncForLocalExecution()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& request = RpcContext_->Request();
        auto cellTags = FromProto<TCellTagList>(request.cell_tags_to_sync_with());
        auto registerTransaction = [&] (TTransactionId transactionId) {
            if (transactionId) {
                cellTags.push_back(CellTagFromId(transactionId));
            }
        };

        for (int subrequestIndex = 0; subrequestIndex < TotalSubrequestCount_; ++subrequestIndex) {
            const auto& subrequest = Subrequests_[subrequestIndex];
            if (subrequest.Type == EExecutionSessionSubrequestType::Undefined && subrequest.TentativelyRemote) {
                // Phase one.
                continue;
            }
            if (subrequest.Type == EExecutionSessionSubrequestType::Remote) {
                // Phase two.
                continue;
            }

            registerTransaction(GetTransactionId(subrequest.RequestHeader));
            for (const auto& prerequisite : subrequest.PrerequisitesExt->transactions()) {
                registerTransaction(FromProto<TTransactionId>(prerequisite.transaction_id()));
            }
            for (const auto& prerequisite : subrequest.PrerequisitesExt->revisions()) {
                registerTransaction(FromProto<TTransactionId>(prerequisite.transaction_id()));
            }
        }

        SortUnique(cellTags);
        return cellTags;
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

    bool RegisterCellToSyncWith(TCellTag cellTag)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (std::find(SyncedWithCellTags_.begin(), SyncedWithCellTags_.end(), cellTag) != SyncedWithCellTags_.end()) {
            // Already synced with this cell.
            return false;
        }

        if (cellTag == Bootstrap_->GetCellTag()) {
            // No need to sync with self.
            return false;
        }

        if (Bootstrap_->IsSecondaryMaster() && cellTag == Bootstrap_->GetPrimaryCellTag()) {
            // IHydraManager::SyncWithUpstream will take care of this.
            return false;
        }

        SyncedWithCellTags_.push_back(cellTag);
        return true;
    }

    TFuture<void> StartSync(bool syncWithUpstream)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        const auto& request = RpcContext_->Request();
        if (request.suppress_upstream_sync()) {
            return {};
        }

        std::vector<TFuture<void>> syncFutures;
        auto addAsyncResult = [&] (TFuture<void> future) {
            if (!future.IsSet() || !future.Get().IsOK()) {
                syncFutures.push_back(std::move(future));
            }
        };

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        const auto& hiveManager = Bootstrap_->GetHiveManager();

        if (syncWithUpstream) {
            addAsyncResult(hydraManager->SyncWithUpstream());
        }

        TCellTagList syncCellTags;
        for (auto cellTag : CollectCellsToSyncForLocalExecution()) {
            if (!RegisterCellToSyncWith(cellTag)) {
                continue;
            }
            auto cellId = Bootstrap_->GetCellId(cellTag);
            addAsyncResult(hiveManager->SyncWith(cellId, true));
            syncCellTags.push_back(cellTag);
        }

        if (syncFutures.empty()) {
            return {};
        }

        YT_LOG_DEBUG_UNLESS(syncCellTags.empty(), "Request will synchronize with another cells (RequestId: %v, CellTags: %v)",
            RequestId_,
            syncCellTags);

        return Combine(syncFutures);
    }

    void RunSyncPhaseOne()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto future = StartSync(true);
        if (future) {
            future.Subscribe(BIND(&TExecuteSession::OnSyncPhaseOneCompleted, MakeStrong(this)));
        } else {
            OnSyncPhaseOneCompleted();
        }
    }

    void OnSyncPhaseOneCompleted(const TError& error = {})
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!error.IsOK()) {
            Reply(error);
            return;
        }

        // Re-check remote requests to see if the cache resolve is still OK.
        DecideSubrequestTypes();

        ForwardRemoteRequests();

        RunSyncPhaseTwo();
    }

    void MarkSubrequestLocal(TSubrequest* subrequest)
    {
        if (subrequest->YPathExt->mutating() && TentativePeerState_ != EPeerState::Leading) {
            MarkSubrequestRemoteIntraCell(subrequest);
            return;
        }

        subrequest->RpcContext = CreateYPathContext(
            subrequest->RequestMessage,
            ObjectServerLogger,
            NLogging::ELogLevel::Debug);

        if (subrequest->YPathExt->mutating()) {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            subrequest->Mutation = objectManager->CreateExecuteMutation(UserName_, subrequest->RpcContext);
            subrequest->Mutation->SetMutationId(subrequest->RpcContext->GetMutationId(), subrequest->RpcContext->IsRetry());
            subrequest->Type = EExecutionSessionSubrequestType::LocalWrite;
            Profiler.Increment(subrequest->ProfilingCounters->LocalWriteRequestCounter);
        } else {
            subrequest->Type = EExecutionSessionSubrequestType::LocalRead;
            Profiler.Increment(subrequest->ProfilingCounters->LocalReadRequestCounter);
        }
    }

    void MarkSubrequestRemoteIntraCell(TSubrequest* subrequest)
    {
        subrequest->ForwardedCellTag = Bootstrap_->GetCellTag();
        subrequest->RemoteRequestMessage = subrequest->RequestMessage;
        subrequest->Type = EExecutionSessionSubrequestType::Remote;
        Profiler.Increment(subrequest->ProfilingCounters->IntraCellForwardingRequestCounter);
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

        subrequest->ForwardedCellTag = forwardedCellTag;
        subrequest->RemoteRequestMessage = SetRequestHeader(subrequest->RequestMessage, remoteRequestHeader);
        subrequest->Type = EExecutionSessionSubrequestType::Remote;
        Profiler.Increment(subrequest->ProfilingCounters->CrossCellForwardingRequestCounter);
    }

    void DecideSubrequestType(TSubrequest* subrequest)
    {
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

    void RunSyncPhaseTwo()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto future = StartSync(true);
        if (future) {
            future.Subscribe(BIND(&TExecuteSession::OnSyncPhaseTwoCompleted, MakeStrong(this)));
        } else {
            OnSyncPhaseTwoCompleted();
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

    void OnSyncPhaseTwoCompleted(const TError& error = {})
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
                const auto& multicellManager = Bootstrap_->GetMulticellManager();
                auto channel = multicellManager->GetMasterChannelOrThrow(cellTag, peerKind);
                TObjectServiceProxy proxy(std::move(channel));
                auto batchReq = proxy.ExecuteBatchNoBackoffRetries();
                batchReq->SetOriginalRequestId(RequestId_);
                batchReq->SetTimeout(ComputeForwardingTimeout(RpcContext_, Owner_->Config_));
                batchReq->SetUser(RpcContext_->GetUser());
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

            YT_LOG_DEBUG("Forwarding object request (RequestId: %v -> %v, Method: %v:%v, "
                "%v%v%vUser: %v, Mutating: %v, CellTag: %v, PeerKind: %v)",
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
                UserName_,
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
                        YT_LOG_DEBUG(batchRspOrError, "Forwarded request failed (RequestId: %v -> %v, SubrequestIndexes: %v)",
                            RequestId_,
                            batch.BatchReq->GetRequestId(),
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
            User_ = securityManager->GetUserByNameOrThrow(UserName_);
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
                auto error = TError(
                    NSecurityClient::EErrorCode::RequestQueueSizeLimitExceeded,
                    "User %Qv has exceeded its request queue size limit",
                    User_->GetName())
                    << TErrorAttribute("limit", User_->GetRequestQueueSizeLimit(Bootstrap_->GetCellTag()))
                    << TErrorAttribute("cell_tag", Bootstrap_->GetCellTag());
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
            auto workloadType = Subrequests_[CurrentSubrequestIndex_].Mutation
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
                TTryGuard<TSpinLock> guard(LocalExecutionLock_);
                if (!guard.WasAcquired()) {
                    Reschedule();
                    break;
                }

                if (LocalExecutionInterrupted_.load()) {
                    break;
                }

                ExecuteCurrentSubrequest();
            }
        }

        if (CurrentSubrequestIndex_ >= TotalSubrequestCount_) {
            ReleaseUltimateReplyLock();
        }
    }

    void ExecuteCurrentSubrequest()
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        auto& subrequest = Subrequests_[CurrentSubrequestIndex_++];

        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        subrequest.Revision = hydraManager->GetAutomatonVersion().ToRevision();

        if (subrequest.TraceContext) {
            subrequest.TraceContext->ResetStartTime();
        }

        switch (subrequest.Type) {
            case EExecutionSessionSubrequestType::LocalRead:
                ExecuteReadSubrequest(&subrequest);
                break;

            case EExecutionSessionSubrequestType::LocalWrite:
                ExecuteWriteSubrequest(&subrequest);
                break;

            case EExecutionSessionSubrequestType::Remote:
                break;

            default:
                YT_ABORT();
        }
    }

    void ExecuteWriteSubrequest(TSubrequest* subrequest)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AcquireReplyLock();

        TCounterIncrementingTimingGuard<TWallTimer> timingGuard(Profiler, &subrequest->ProfilingCounters->LocalMutationScheduleTimeCounter);

        subrequest->AsyncCommitResult = subrequest->Mutation->Commit();
        subrequest->AsyncCommitResult.Subscribe(
            BIND(&TExecuteSession::OnMutationCommitted, MakeStrong(this), subrequest));
    }

    void ForwardSubrequestToLeader(TSubrequest* subrequest)
    {
        YT_LOG_DEBUG("Performing leader fallback (RequestId: %v)",
            RequestId_);

        Profiler.Increment(subrequest->ProfilingCounters->LeaderFallbackRequestCounter);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto asyncSubresponse = objectManager->ForwardObjectRequest(
            subrequest->RequestMessage,
            Bootstrap_->GetCellTag(),
            EPeerKind::Leader,
            RpcContext_->GetTimeout());

        SubscribeToSubresponse(subrequest, std::move(asyncSubresponse));
    }

    void ExecuteReadSubrequest(TSubrequest* subrequest)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        AcquireReplyLock();

        TWallTimer timer;

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        TAuthenticatedUserGuard userGuard(securityManager, User_);

        NTracing::TTraceContextGuard traceContextGuard(subrequest->TraceContext);

        const auto& context = subrequest->RpcContext;
        try {
            const auto& objectManager = Bootstrap_->GetObjectManager();
            auto rootService = objectManager->GetRootService();
            ExecuteVerb(rootService, context);
        } catch (const TLeaderFallbackException&) {
            ForwardSubrequestToLeader(subrequest);
        }

        // NB: Even if the user was just removed the instance is still valid but not alive.
        if (IsObjectAlive(User_) && !EpochCancelableContext_->IsCanceled()) {
            securityManager->ChargeUser(User_, {EUserWorkloadType::Read, 1, timer.GetElapsedTime()});
        }

        WaitForSubresponse(subrequest);
    }

    void OnMutationCommitted(TSubrequest* subrequest, const TErrorOr<TMutationResponse>& responseOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();

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
        subrequest.Uncertain = true;
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

        if (error.IsOK()) {
            auto& request = RpcContext_->Request();
            auto& response = RpcContext_->Response();
            auto& attachments = response.Attachments();

            // COMPAT(babenko)
            int effectiveSubrequestCount = TotalSubrequestCount_;
            if (!request.supports_portals()) {
                effectiveSubrequestCount = 0;
                for (auto index = 0; index < TotalSubrequestCount_; ++index) {
                    const auto& subrequest = Subrequests_[index];
                    if (!subrequest.Completed) {
                        break;
                    }
                    effectiveSubrequestCount = index + 1;
                }
            }

            for (auto index = 0; index < effectiveSubrequestCount; ++index) {
                const auto& subrequest = Subrequests_[index];
                if (!subrequest.Completed) {
                    YT_ASSERT(request.supports_portals());
                    continue;
                }

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

            for (int index = 0; index < TotalSubrequestCount_; ++index) {
                if (Subrequests_[index].Uncertain) {
                    response.add_uncertain_subrequest_indexes(index);
                }
            }

            if (response.subresponses_size() == 0) {
                YT_LOG_DEBUG("Dropping request since no subresponses are available (RequestId: %v)",
                    RequestId_);
                return;
            }

            RpcContext_->SetResponseInfo("SubresponseCount: %v, UncertainSubrequestIndexes : %v",
                response.subresponses_size(),
                response.uncertain_subrequest_indexes());
        }

        RpcContext_->Reply(error);
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
                BIND(&TObjectService::TExecuteSession::OnBackoffAlarm, MakeStrong(this))
                    .Via(TObjectService::GetRpcInvoker()),
                backoffDelay);
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

        TTryGuard<TSpinLock> guard(LocalExecutionLock_);
        if (!guard.WasAcquired()) {
            TObjectService::GetRpcInvoker()->Invoke(
                BIND(&TObjectService::TExecuteSession::ScheduleReplyIfNeeded, MakeStrong(this)));
            return false;
        }

        if (BackoffAlarmTriggered_ && LocalExecutionStarted_ && SomeSubrequestCompleted_) {
            YT_LOG_DEBUG("Local execution interrupted due to backoff alarm (RequestId: %v)",
                RequestId_);
            LocalExecutionInterrupted_.store(true);
            return ReleaseUltimateReplyLock();
        }

        if (ReplyLockCount_.load() > 0) {
            return false;
        }

        Reply();
        return true;
    }


    TCodicilGuard MakeCodicilGuard()
    {
        VERIFY_THREAD_AFFINITY_ANY();

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
    bucket->ExcessTime = actualExcessTime + workload.Time;
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

    const auto& userName = context->GetUser();
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
