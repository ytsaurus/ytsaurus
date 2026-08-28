#include "yt_wrapper.h"
#include "node_id_allocator.h"

#include <util/thread/pool.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/join.h>
#include <util/string/strip.h>

#include <util/system/env.h>

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>

#include <contrib/ydb/library/yql/providers/dq/common/attrs.h>
#include <contrib/ydb/library/yql/providers/dq/actors/actor_helpers.h>
#include <contrib/ydb/library/yql/providers/dq/actors/events/events.h>
#include <yt/yql/providers/dq/actors/yt/resource_manager.h>
#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <library/cpp/yson/node/node_io.h>

#include <yt/cpp/mapreduce/interface/fluent.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <limits>

namespace NYql {

#define RM_LOG(A) YQL_CLOG(A, ProviderDq) << ClusterName << ": "

    namespace NCommonJobVars {
        const TString ACTOR_PORT("ACTOR_PORT");
        const TString ACTOR_NODE_ID("ACTOR_NODE_ID");
        const TString ADDRESS_RESOLVER_CONFIG("ADDRESS_RESOLVER_CONFIG");
        const TString UDFS_PATH("UDFS_PATH");
        const TString OPERATION_SIZE("OPERATION_SIZE");
        const TString YT_COORDINATOR("YT_COORDINATOR");
        const TString YT_BACKEND("YT_BACKEND");
        const TString YT_FORCE_IPV4("YT_FORCE_IPV4");
    }

    constexpr TStringBuf YqlWorkerTaskPrefix = "yql_worker_";

    using namespace NActors;

    struct TEvDropOperation
        : NActors::TEventLocal<TEvDropOperation, TDqEvents::ES_OTHER1> {
        TEvDropOperation() = default;
        TEvDropOperation(const TString& operationId, const TString& mutationId)
            : OperationId(operationId)
            , MutationId(mutationId)
        { }

        TString OperationId;
        TString MutationId;
    };

    class TYtVanillaOperation: public TActor<TYtVanillaOperation> {
    public:
        static constexpr char ActorName[] = "YT_OPERATION";

        TYtVanillaOperation(const TString& clusterName, TActorId ytWrapper, TActorId parentId, TString operationId, TString mutationId, TIntrusivePtr<NMonitoring::TDynamicCounters> counters)
            : TActor<TYtVanillaOperation>(&TYtVanillaOperation::Handler)
            , ClusterName(clusterName)
            , YtWrapper(ytWrapper)
            , ParentId(parentId)
            , OperationId(NYT::NScheduler::TOperationId(NYT::TGuid::FromString(operationId)))
            , MutationId(mutationId)
            , Counters(counters)
        { }

        ~TYtVanillaOperation()
        {
            auto counters = Counters->GetSubgroup("operation", "brief_progress");
            for (const auto& [k, v] : Status) {
                *counters->GetCounter(k) += -v;
            }
        }

    private:
        STRICT_STFUNC(Handler, {
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            HFunc(TEvGetOperationResponse, OnGetOperationResponse);
        })

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& /*parentId*/) override {
            return new IEventHandle(YtWrapper, self, new TEvGetOperation(OperationId, NYT::NApi::TGetOperationOptions()), 0);
        }

        void OnGetOperationResponse(TEvGetOperationResponse::TPtr& ev, const NActors::TActorContext& ) {
            auto result = std::get<0>(*ev->Get());
            bool stopWatcher = false;
            if (!result.IsOK() && result.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                stopWatcher = true;
            }

            if (result.IsOK()) {
                auto attributesMap = NYT::NodeFromYsonString(result.Value()).AsMap();

                try {
                    if (attributesMap.contains("result")) {
                        RM_LOG(DEBUG) << "Result " << NYT::NodeToYsonString(attributesMap["result"]);
                        stopWatcher = true;
                    }

                    if (attributesMap.contains("brief_progress")) {
                        auto statusMap = attributesMap["brief_progress"].AsMap()["jobs"].AsMap();

                        auto counters = Counters->GetSubgroup("operation", "brief_progress");
                        for (const auto& [k, v] : statusMap) {
                            auto& oldStatus = Status[k];
                            auto newStatus = v.AsInt64();
                            *counters->GetCounter(k) += newStatus - oldStatus;
                            oldStatus = newStatus;
                        }
                    }

                } catch (...) {
                    RM_LOG(DEBUG) << CurrentExceptionMessage();
                }
            }

            if (stopWatcher) {
                RM_LOG(DEBUG) << "Stop watching operation (1) " << ToString(OperationId) << " " << ToString(result);
                Send(YtWrapper, new TEvPrintJobStderr(OperationId));
                Send(ParentId, new TEvDropOperation(ToString(OperationId), MutationId));
                PassAway();
            } else {
                TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
                TActivationContext::Schedule(TDuration::Seconds(5),
                    new IEventHandle(YtWrapper, SelfId(), new TEvGetOperation(OperationId, NYT::NApi::TGetOperationOptions()), 0),
                    TimerCookieHolder.Get());
            }
        }

        const TString ClusterName;
        const TActorId YtWrapper;
        const TActorId ParentId;
        const NYT::NScheduler::TOperationId OperationId;
        const TString MutationId;
        NActors::TSchedulerCookieHolder TimerCookieHolder;
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;
        THashMap<TString, i64> Status;
    };


    class TYtResourceManager: public TRichActor<TYtResourceManager> {
    public:
        static constexpr char ActorName[] = "YTRM";

        TYtResourceManager(
            const TResourceManagerOptions& options,
            const ICoordinationHelper::TPtr& coordinator)
            : TRichActor<TYtResourceManager>(&TYtResourceManager::Follower)
            , Options(options)
            , Counters(Options.Counters)
            , RecoveryQuarantinedRecordCount(Counters->GetCounter("recovery_quarantined_record_count"))
            , RecoveryConflictingClaimCount(Counters->GetCounter("recovery_conflicting_claim_count"))
            , QuarantinedOwnerCount(Counters->GetCounter("quarantined_owner_count"))
            , QuarantinedClaimCount(Counters->GetCounter("quarantined_claim_count"))
            , CapacityBlockedByQuarantine(Counters->GetCounter("capacity_blocked_by_quarantine"))
            , OverlappingClaimCount(Counters->GetCounter("overlapping_claim_count"))
            , IncompleteQuarantinedClaimRecordCount(Counters
                ->GetCounter("incomplete_quarantined_claim_record_count"))
            , PendingOperationIdUpdateCount(Counters->GetCounter("pending_operation_id_update_count"))
            , UnknownStartOutcomeCount(Counters
                ->GetCounter("unknown_start_outcome_count", /*derivative*/ true))
            , UnknownStartOutcomeClaimCount(Counters
                ->GetCounter("unknown_start_outcome_claim_count", /*derivative*/ true))
            , ClusterName(Options.YtBackend.GetClusterName())
            , ClusterOperationsPath(Options.YtBackend.GetPrefix() + "/operations/" + Options.YtBackend.GetClusterName())
            , Coordinator(coordinator)
            , CoordinatorConfig(Coordinator->GetConfig())
            , CoordinatorWrapper(Coordinator->GetWrapper())
            , NodeIdAllocator(Options.YtBackend.GetMinNodeId(), Options.YtBackend.GetMaxNodeId())
        {
            ResetHealthCounters();
        }

    private:
        struct TRecoveredOperation;

        // States: Follower <-> (ListOperations -> Leader)

        void StartFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);

            auto leaderAttributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();
            LeaderTransactionId = NYT::NObjectClient::TTransactionId();

            RM_LOG(INFO) << "Become follower, leader=" << leaderAttributes.at(NCommonAttrs::ACTOR_NODEID_ATTR).AsUint64()
                         << " RunningOperations=" << RunningOperations.size()
                         << " PendingNodeReleases=" << PendingNodeReleases.size();
            for (const auto& [k, v] : RunningOperations) {
                UnregisterChild(v.ActorId);
            }
            RunningOperations.clear();
            PendingStartOperationRequests.clear();
            PendingOperationIdUpdates.clear();
            OperationIdUpdateRequests.clear();
            ListOperationsRequestId.Clear();
            RecoveredOperations.clear();
            ClaimReconciliationRequests.clear();
            QuarantinedOwners.clear();
            QuarantinedClaims = 0;
            IncompleteQuarantinedClaimRecords = 0;
            NodeIdAllocator.Clear();
            PendingNodeReleases.clear();
            ResetHealthCounters();
            Become(&TYtResourceManager::Follower);
        }

        void StartLeader(TEvBecomeLeader::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            RM_LOG(INFO) << "Become leader, epoch=" << ev->Get()->LeaderEpoch;

            LeaderTransactionId = NYT::NObjectClient::TTransactionId::FromString(ev->Get()->LeaderTransaction);
            RecoveredOperations.clear();
            ClaimReconciliationRequests.clear();

            ListOperations();
            Tick();
            Become(&TYtResourceManager::ListOperationsState);
        }

        STRICT_STFUNC(Follower, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvBecomeLeader, StartLeader)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            CFunc(TEvents::TEvBootstrap::EventType, Bootstrap)

            IgnoreFunc(TEvTick)
            IgnoreFunc(TEvDropOperation)
            IgnoreFunc(TEvListNodeResponse)
            IgnoreFunc(TEvStartOperationResponse)
            IgnoreFunc(TEvCreateNodeResponse)
            IgnoreFunc(TEvGetOperationResponse)
            IgnoreFunc(TEvSetNodeResponse)
            IgnoreFunc(TEvRemoveNodeResponse)
        })

        STRICT_STFUNC(ListOperationsState, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvDropOperation, OnDropOperation)
            HFunc(TEvListNodeResponse, OnListOperations)
            HFunc(TEvGetOperationResponse, OnClaimReconciliationResponse)
            HFunc(TEvStartOperationResponse, OnStartOperationResponse)
            HFunc(TEvRemoveNodeResponse, OnRemoveNodeResponse)
            cFunc(TEvTick::EventType, OnRecoveryTick)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            IgnoreFunc(TEvCreateNodeResponse)
            IgnoreFunc(TEvSetNodeResponse)
        })

        STRICT_STFUNC(Leader, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvDropOperation, OnDropOperation)
            HFunc(TEvListNodeResponse, OnListResponse)
            IgnoreFunc(TEvGetOperationResponse)
            HFunc(TEvStartOperationResponse, OnStartOperationResponse)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            cFunc(TEvTick::EventType, OnLeaderTick)
            HFunc(TEvCreateNodeResponse, OnCreateNode)
            HFunc(TEvRemoveNodeResponse, OnRemoveNodeResponse)
            HFunc(TEvSetNodeResponse, OnSetNodeResponse)
        })

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
            return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            YtWrapper = Coordinator->GetWrapper(
                ctx.ActorSystem(),
                Options.YtBackend.GetProxyAddress(),
                Options.YtBackend.GetUser(),
                Options.YtBackend.GetToken());
            RegisterChild(Coordinator->CreateLockOnCluster(YtWrapper, Options.YtBackend.GetPrefix(), Options.LockName, false));
        }

        void OnRecoveryTick()
        {
            if (!ListOperationsRequestId && ClaimReconciliationRequests.empty()) {
                ListOperations();
            }
            Tick();
        }

        void OnLeaderTick()
        {
            RetryOperationIdUpdates();
            ListWorkers();
            Tick();
        }

        void Tick() {
            TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
            Schedule(Options.TickInterval, new TEvTick(), TimerCookieHolder.Get());
        }

        void AddQuarantinedClaims(const TString& owner, i64 claimCount)
        {
            Y_ABORT_UNLESS(!owner.empty());
            Y_ABORT_UNLESS(claimCount > 0);
            if (QuarantinedOwners.insert(owner).second) {
                QuarantinedClaims += claimCount;
            }
        }

        void UpdateHealthCounters()
        {
            const i64 claimCount = NodeIdAllocator.GetClaimCount();
            const i64 managedClaimCount = claimCount - QuarantinedClaims;
            Y_ABORT_UNLESS(managedClaimCount >= 0);

            // Measures only capacity that would be available without quarantined claims.
            const i64 maxJobs = Options.YtBackend.GetMaxJobs();
            const i64 availableWithoutQuarantine = Max<i64>(maxJobs - managedClaimCount, 0);
            const i64 blockedCapacity = Min(QuarantinedClaims, availableWithoutQuarantine);

            *QuarantinedOwnerCount = ssize(QuarantinedOwners);
            *QuarantinedClaimCount = QuarantinedClaims;
            *CapacityBlockedByQuarantine = blockedCapacity;
            *OverlappingClaimCount = claimCount - NodeIdAllocator.GetClaimedNodeIdCount();
            *IncompleteQuarantinedClaimRecordCount = IncompleteQuarantinedClaimRecords;
            *PendingOperationIdUpdateCount = ssize(PendingOperationIdUpdates);
        }

        void ResetHealthCounters()
        {
            *RecoveryQuarantinedRecordCount = 0;
            *RecoveryConflictingClaimCount = 0;
            *QuarantinedOwnerCount = 0;
            *QuarantinedClaimCount = 0;
            *CapacityBlockedByQuarantine = 0;
            *OverlappingClaimCount = 0;
            *IncompleteQuarantinedClaimRecordCount = 0;
            *PendingOperationIdUpdateCount = 0;
        }

        void CreateCoreTable(ui32 tableNumber)
        {
            NYT::NApi::TCreateNodeOptions options;
            options.Recursive = true;
            options.IgnoreExisting = true;

            YQL_CLOG(DEBUG, ProviderDq) << "Creating core table: " << Options.UploadPrefix + "/CoreTable-" + ToString(tableNumber);

            Send(YtWrapper, new TEvCreateNode(
                static_cast<ui64>(-1),
                Options.UploadPrefix + "/CoreTable-" + ToString(tableNumber),
                NYT::NObjectClient::EObjectType::Table,
                options));

            YQL_CLOG(DEBUG, ProviderDq) << "Creating stderr table: " << Options.UploadPrefix + "/StderrTable-" + ToString(tableNumber);

            Send(YtWrapper, new TEvCreateNode(
                static_cast<ui64>(-1),
                Options.UploadPrefix + "/StderrTable-" + ToString(tableNumber),
                NYT::NObjectClient::EObjectType::Table,
                options));
        }

        void StartOperationWatcher(const TString& operationId, const TString& mutationId, const NActors::TActorContext& ctx)
        {
            Y_UNUSED(ctx);
            RM_LOG(DEBUG) << "StartOperationWatcher " << operationId << "|" << mutationId;
            auto operation = RunningOperations.find(mutationId);
            Y_ABORT_UNLESS(operation != RunningOperations.end());
            auto actorId = RegisterChild(new TYtVanillaOperation(ClusterName, YtWrapper, SelfId(), operationId, mutationId, Counters));
            operation->second.ActorId = actorId;
        }

        void MaybeStartOperations(const NActors::TActorContext& ctx)
        {
            // to avoid races do nothing if there is PendingStartOperationRequests
            if (!PendingStartOperationRequests.empty()) {
                RM_LOG(DEBUG) << "PendingStartOperationRequests contains " << PendingStartOperationRequests.size() << " requests ";
                for (const auto& [k, _]: PendingStartOperationRequests) {
                    RM_LOG(DEBUG) << "RequestId " << k;
                }
                return;
            }

            for (const auto& [k, v] : RunningOperations) {
                RM_LOG(DEBUG) << "Operation: " << k << " " << v.Nodes.size() << " ";
            }
            const i64 potentialJobCount = NodeIdAllocator.GetClaimCount();
            const i64 maxJobs = Options.YtBackend.GetMaxJobs();

            RM_LOG(DEBUG) << "Potential/Max jobs: " << potentialJobCount << "/" << maxJobs;

            const i64 needToStart = maxJobs - potentialJobCount;
            RM_LOG(DEBUG) << "Need to start: " << needToStart;
            if (needToStart > 0) {
                StartOperations(needToStart, ctx);
            }
        }

        void DropRunningOperation(const TString& mutationId) {
            ForgetOperationIdUpdate(mutationId);

            TVector<ui32> currentNodes;
            const auto it = RunningOperations.find(mutationId);
            if (it != RunningOperations.end()) {
                currentNodes = it->second.Nodes;
                RM_LOG(DEBUG) << "DropRunningOperation mutation=" << mutationId
                              << " operationId=" << it->second.OperationId
                              << " node_ids=[" << JoinSeq(",", currentNodes) << "]";
                RunningOperations.erase(it);
            } else {
                RM_LOG(DEBUG) << "DropRunningOperation mutation=" << mutationId << " (not in RunningOperations)";
            }

            auto removePath = ClusterOperationsPath + "/" + mutationId;
            RM_LOG(DEBUG) << "Removing operation node " << removePath
                          << " LeaderTxn=" << ToString(LeaderTransactionId)
                          << " deferring release of node_ids=[" << JoinSeq(",", currentNodes) << "]";
            NYT::NApi::TRemoveNodeOptions removeNodeOptions;
            removeNodeOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);
            removeNodeOptions.Force = true;
            PendingNodeReleases[YtRequestId] = {mutationId, currentNodes};
            Send(YtWrapper, new TEvRemoveNode(YtRequestId++, removePath, removeNodeOptions));
        }

        void OnRemoveNodeResponse(TEvRemoveNodeResponse::TPtr& ev, const NActors::TActorContext&) {
            auto requestId = ev->Get()->RequestId;
            auto result = std::get<0>(*ev->Get());

            auto it = PendingNodeReleases.find(requestId);
            if (it == PendingNodeReleases.end()) {
                return;
            }

            auto [mutationId, nodeIds] = it->second;
            PendingNodeReleases.erase(it);

            if (result.IsOK()) {
                RM_LOG(DEBUG) << "Operation node removed: mutation=" << mutationId
                              << " freeing node_ids=[" << JoinSeq(",", nodeIds) << "]";
                if (!NodeIdAllocator.Release(mutationId)) {
                    RM_LOG(ERROR) << "Cannot release node IDs for mutation=" << mutationId;
                }
                UpdateHealthCounters();
            } else {
                RM_LOG(ERROR) << "OPERATION NODE REMOVE FAILED: mutation=" << mutationId
                              << " node_ids=[" << JoinSeq(",", nodeIds) << "] remain blocked until next epoch"
                              << " LeaderTxn=" << ToString(LeaderTransactionId)
                              << " error=" << ToString(result);
            }
        }

        void OnDropOperation(TEvDropOperation::TPtr& ev, const NActors::TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto operationId = ev->Get()->OperationId;
            auto mutationId = ev->Get()->MutationId;
            auto maybeOperation = RunningOperations.find(mutationId);
            if (maybeOperation != RunningOperations.end()) {
                if (ev->Sender != maybeOperation->second.ActorId ||
                    operationId != maybeOperation->second.OperationId)
                {
                    RM_LOG(WARN) << "Ignoring stale operation callback " << operationId << "|" << mutationId;
                    return;
                }

                UnregisterChild(maybeOperation->second.ActorId);

                RM_LOG(DEBUG) << "Stop operation " << operationId << "|" << mutationId;
                DropRunningOperation(mutationId);
            } else {
                RM_LOG(WARN) << "Unknown operation " << operationId << "|" << mutationId;
            }
        }

        void OnListResponse(TEvListNodeResponse::TPtr& ev, const NActors::TActorContext& ctx) {
            auto result = std::get<0>(*ev->Get());

            try {
                MaybeStartOperations(ctx);
            } catch (...) {
                RM_LOG(ERROR) << "Error on list node " << CurrentExceptionMessage();
            }
        }

        void SetClaimError(TRecoveredOperation* operation, TString error) const
        {
            if (operation->ClaimError.empty()) {
                operation->ClaimError = std::move(error);
            }
        }

        void ParseRecoveredClaim(const NYT::TNode& opNode, TRecoveredOperation* operation) const
        {
            try {
                operation->Owner = opNode.AsString();
                if (operation->Owner.empty()) {
                    SetClaimError(operation, "owner is empty");
                    return;
                }

                const auto& attributes = opNode.GetAttributes().AsMap();
                const auto operationSizeIt = attributes.find(NCommonAttrs::OPERATIONSIZE_ATTR);
                if (operationSizeIt == attributes.end()) {
                    SetClaimError(operation, "operation size is missing");
                } else {
                    try {
                        const i64 operationSize = operationSizeIt->second.IntCast<i64>();
                        if (operationSize <= 0) {
                            SetClaimError(operation, TStringBuilder() << "operation size " << operationSize
                                << " is not positive");
                        } else {
                            operation->OperationSize = operationSize;
                        }
                    } catch (...) {
                        SetClaimError(operation, TStringBuilder() << "operation size is invalid: "
                            << CurrentExceptionMessage());
                    }
                }

                const auto nodeIdsIt = attributes.find(NCommonAttrs::ACTOR_NODEID_ATTR);
                if (nodeIdsIt == attributes.end()) {
                    SetClaimError(operation, "node IDs are missing");
                    return;
                }

                const auto& nodeList = nodeIdsIt->second.AsList();
                if (nodeList.empty()) {
                    SetClaimError(operation, "node ID list is empty");
                    return;
                }

                THashSet<ui32> uniqueNodeIds;
                uniqueNodeIds.reserve(nodeList.size());
                operation->Nodes.reserve(nodeList.size());
                for (const auto& node : nodeList) {
                    try {
                        const ui64 nodeId = node.IntCast<ui64>();
                        if (nodeId > std::numeric_limits<ui32>::max()) {
                            SetClaimError(operation, TStringBuilder() << "node ID " << nodeId << " does not fit ui32");
                            continue;
                        }

                        const auto typedNodeId = static_cast<ui32>(nodeId);
                        if (!uniqueNodeIds.insert(typedNodeId).second) {
                            SetClaimError(operation, TStringBuilder() << "node ID " << nodeId << " is duplicated");
                            continue;
                        }

                        operation->Nodes.push_back(typedNodeId);
                    } catch (...) {
                        SetClaimError(operation, TStringBuilder() << "node ID " << NYT::NodeToYsonString(node)
                            << " is not an unsigned integer: " << CurrentExceptionMessage());
                    }
                }

                if (operation->OperationSize && *operation->OperationSize != ssize(operation->Nodes)) {
                    SetClaimError(operation, TStringBuilder() << "operation size " << *operation->OperationSize
                        << " does not match node ID count " << ssize(operation->Nodes));
                }
            } catch (...) {
                SetClaimError(operation, TStringBuilder() << "cannot parse record "
                    << NYT::NodeToYsonString(opNode) << ": " << CurrentExceptionMessage());
            }
        }

        void ParseRecoveredMetadata(const NYT::TNode& opNode, TRecoveredOperation* operation) const
        {
            auto setMetadataError = [&] (TString error) {
                if (operation->MetadataError.empty()) {
                    operation->MetadataError = std::move(error);
                }
            };

            try {
                const auto& attributes = opNode.GetAttributes().AsMap();
                if (const auto operationIdIt = attributes.find(NCommonAttrs::OPERATIONID_ATTR);
                    operationIdIt != attributes.end())
                {
                    const auto operationId = operationIdIt->second.AsString();
                    NYT::TGuid operationGuid;
                    if (NYT::TGuid::FromString(operationId, &operationGuid)) {
                        operation->OperationId = operationId;
                    } else {
                        setMetadataError(TStringBuilder() << "operation ID " << operationId << " is not a GUID");
                    }
                }

                const auto mutationIdIt = attributes.find("yql_mutation_id");
                if (mutationIdIt == attributes.end()) {
                    setMetadataError("mutation ID is missing");
                } else if (mutationIdIt->second.AsString() != operation->Owner) {
                    setMetadataError("mutation ID does not match owner");
                }

                NYT::TGuid ownerGuid;
                if (!NYT::TGuid::FromString(operation->Owner, &ownerGuid)) {
                    setMetadataError("owner is not a GUID");
                }

                if (const auto clusterNameIt = attributes.find(NCommonAttrs::CLUSTERNAME_ATTR);
                    clusterNameIt != attributes.end())
                {
                    const auto recoveredClusterName = clusterNameIt->second.AsString();
                    if (recoveredClusterName != ClusterName) {
                        setMetadataError(TStringBuilder() << "cluster name " << recoveredClusterName
                            << " does not match " << ClusterName);
                    }
                }
            } catch (...) {
                setMetadataError(CurrentExceptionMessage());
            }
        }

        bool TryParseProvidedSpecClaim(
            const TString& operationYson,
            TVector<ui32>* nodes,
            TString* error) const
        {
            try {
                const auto operationNode = NYT::NodeFromYsonString(operationYson);
                const auto& operation = operationNode.AsMap();
                const auto providedSpecIt = operation.find("provided_spec");
                if (providedSpecIt == operation.end()) {
                    *error = "provided spec is missing";
                    return false;
                }

                const auto& spec = providedSpecIt->second.AsMap();
                const auto tasksIt = spec.find("tasks");
                if (tasksIt == spec.end() || !tasksIt->second.IsMap() || tasksIt->second.AsMap().empty()) {
                    *error = "provided spec has no tasks";
                    return false;
                }

                const auto& tasks = tasksIt->second.AsMap();
                THashSet<ui32> uniqueNodeIds;
                TMaybe<i64> operationSize;
                int taskCountWithOperationSize = 0;
                nodes->clear();
                nodes->reserve(tasks.size());

                for (const auto& [taskName, taskNode] : tasks) {
                    if (!taskName.StartsWith(YqlWorkerTaskPrefix)) {
                        *error = TStringBuilder() << "unexpected task " << taskName;
                        return false;
                    }

                    const auto& task = taskNode.AsMap();
                    const auto jobCountIt = task.find("job_count");
                    if (jobCountIt == task.end() || jobCountIt->second.IntCast<i64>() != 1) {
                        *error = TStringBuilder() << "task " << taskName << " must have one job";
                        return false;
                    }

                    const auto environmentIt = task.find("environment");
                    if (environmentIt == task.end()) {
                        *error = TStringBuilder() << "task " << taskName << " has no environment";
                        return false;
                    }
                    const auto& environment = environmentIt->second.AsMap();
                    const auto nodeIdIt = environment.find(NCommonJobVars::ACTOR_NODE_ID);
                    if (nodeIdIt == environment.end()) {
                        *error = TStringBuilder() << "task " << taskName << " has no actor node ID";
                        return false;
                    }

                    const ui64 nodeId = FromString<ui64>(nodeIdIt->second.AsString());
                    if (nodeId > std::numeric_limits<ui32>::max()) {
                        *error = TStringBuilder() << "actor node ID " << nodeId << " does not fit ui32";
                        return false;
                    }
                    const auto typedNodeId = static_cast<ui32>(nodeId);
                    if (taskName != YqlWorkerTaskPrefix + ToString(typedNodeId)) {
                        *error = TStringBuilder() << "task " << taskName << " does not match actor node ID " << nodeId;
                        return false;
                    }
                    if (!uniqueNodeIds.insert(typedNodeId).second) {
                        *error = TStringBuilder() << "actor node ID " << nodeId << " is duplicated";
                        return false;
                    }
                    nodes->push_back(typedNodeId);

                    if (const auto sizeIt = environment.find(NCommonJobVars::OPERATION_SIZE);
                        sizeIt != environment.end())
                    {
                        ++taskCountWithOperationSize;
                        const i64 currentOperationSize = FromString<i64>(sizeIt->second.AsString());
                        if (operationSize && *operationSize != currentOperationSize) {
                            *error = "operation size differs between tasks";
                            return false;
                        }
                        operationSize = currentOperationSize;
                    }
                }

                if (taskCountWithOperationSize != 0 && taskCountWithOperationSize != ssize(tasks)) {
                    *error = "operation size is missing in some tasks";
                    return false;
                }
                if (operationSize && *operationSize != ssize(*nodes)) {
                    *error = TStringBuilder() << "operation size " << *operationSize
                        << " does not match task count " << ssize(*nodes);
                    return false;
                }

                Sort(*nodes);
                return true;
            } catch (...) {
                *error = CurrentExceptionMessage();
                return false;
            }
        }

        void OnListOperations(TEvListNodeResponse::TPtr& ev, const TActorContext& ctx)
        {
            if (!ListOperationsRequestId ||
                ev->Get()->RequestId != *ListOperationsRequestId)
            {
                RM_LOG(WARN) << "Ignoring unexpected operations list response: request_id=" << ev->Get()->RequestId;
                return;
            }
            ListOperationsRequestId.Clear();

            auto result = std::get<0>(*ev->Get());

            if (!result.IsOK()) {
                if (result.FindMatching(NYT::NYTree::EErrorCode::ResolveError)) {
                    Become(&TYtResourceManager::Leader);
                } else {
                    RM_LOG(ERROR) << "Error on list node " << ToString(result);
                }
                Tick();
                return;
            }

            TVector<NYT::TNode> operationsList;
            try {
                operationsList = NYT::NodeFromYsonString(result.Value()).AsList();
            } catch (...) {
                RM_LOG(ERROR) << "Cannot parse operations list; retrying on next tick"
                              << " error=" << CurrentExceptionMessage();
                Tick();
                return;
            }
            RM_LOG(DEBUG) << "OnListOperations: " << operationsList.size() << " operation nodes";

            Y_ABORT_UNLESS(ClaimReconciliationRequests.empty());
            RecoveredOperations.clear();
            RecoveredOperations.reserve(operationsList.size());
            for (const auto& opNode : operationsList) {
                TRecoveredOperation operation;
                ParseRecoveredClaim(opNode, &operation);
                ParseRecoveredMetadata(opNode, &operation);
                RecoveredOperations.push_back(std::move(operation));
            }

            for (int index = 0; index < ssize(RecoveredOperations); ++index) {
                const auto& operation = RecoveredOperations[index];
                if (operation.ClaimError.empty() ||
                    !operation.OperationSize ||
                    ssize(operation.Nodes) > *operation.OperationSize ||
                    operation.OperationId.empty() ||
                    operation.Owner.empty())
                {
                    continue;
                }

                NYT::NApi::TGetOperationOptions options;
                options.Attributes = {"provided_spec"};
                options.Timeout = TDuration::Seconds(30);
                const ui64 requestId = YtRequestId++;
                ClaimReconciliationRequests.emplace(requestId, index);
                Send(YtWrapper, new TEvGetOperation(
                    requestId,
                    NYT::NScheduler::TOperationId(NYT::TGuid::FromString(operation.OperationId)),
                    options));
            }

            if (ClaimReconciliationRequests.empty()) {
                FinishRecovery(ctx);
            }
        }

        void OnClaimReconciliationResponse(
            TEvGetOperationResponse::TPtr& ev,
            const NActors::TActorContext& ctx)
        {
            const ui64 requestId = ev->Get()->RequestId;
            const auto requestIt = ClaimReconciliationRequests.find(requestId);
            if (requestIt == ClaimReconciliationRequests.end()) {
                RM_LOG(DEBUG) << "Ignoring stale claim reconciliation response: request_id=" << requestId;
                return;
            }

            const int operationIndex = requestIt->second;
            ClaimReconciliationRequests.erase(requestIt);
            Y_ABORT_UNLESS(operationIndex >= 0 && operationIndex < ssize(RecoveredOperations));
            auto& operation = RecoveredOperations[operationIndex];
            Y_ABORT_UNLESS(operation.OperationSize);
            const i64 operationSize = *operation.OperationSize;

            const auto result = std::get<0>(*ev->Get());
            TVector<ui32> nodes;
            TString error;
            const bool claimRecovered = result.IsOK() &&
                TryParseProvidedSpecClaim(result.Value(), &nodes, &error) &&
                operationSize == ssize(nodes) &&
                AllOf(operation.Nodes, [&] (ui32 nodeId) {
                    return Find(nodes, nodeId) != nodes.end();
                });
            if (claimRecovered) {
                operation.Nodes = std::move(nodes);
                operation.ClaimError.clear();
                RM_LOG(INFO) << "Recovered node ID claim from operation spec: owner=" << operation.Owner
                             << " operation_id=" << operation.OperationId
                             << " node_ids=[" << JoinSeq(",", operation.Nodes) << "]";
            } else {
                if (!result.IsOK()) {
                    error = ToString(result);
                } else if (error.empty()) {
                    if (operationSize != ssize(nodes)) {
                        error = TStringBuilder() << "operation spec node ID count " << ssize(nodes)
                            << " does not match durable operation size " << operationSize;
                    } else {
                        error = TStringBuilder() << "operation spec node IDs [" << JoinSeq(",", nodes)
                            << "] do not contain durable node IDs [" << JoinSeq(",", operation.Nodes) << "]";
                    }
                }
                // Preserving capacity after a failed reconciliation may reuse an unknown live ID.
                RM_LOG(ERROR) << "Cannot reconcile node ID claim: owner=" << operation.Owner
                              << " operation_id=" << operation.OperationId
                              << " error=" << error;
            }

            if (ClaimReconciliationRequests.empty()) {
                FinishRecovery(ctx);
            }
        }

        void FinishRecovery(const NActors::TActorContext& ctx)
        {
            Y_ABORT_UNLESS(ClaimReconciliationRequests.empty());

            QuarantinedOwners.clear();
            QuarantinedClaims = 0;
            IncompleteQuarantinedClaimRecords = 0;

            i64 conflictingClaimCount = 0;
            for (const auto& operation : RecoveredOperations) {
                if (operation.Owner.empty() || operation.Nodes.empty()) {
                    continue;
                }

                const auto claimResult = NodeIdAllocator.RestoreClaim(operation.Owner, operation.Nodes);
                Y_ABORT_UNLESS(claimResult.Valid);
                conflictingClaimCount += ssize(claimResult.ConflictingNodeIds);
                if (!claimResult.ConflictingNodeIds.empty()) {
                    RM_LOG(ERROR) << "Recovered operation has unsafe node ID claims: owner=" << operation.Owner
                                  << " node_ids=[" << JoinSeq(",", operation.Nodes) << "]"
                                  << " conflicting_ids=[" << JoinSeq(",", claimResult.ConflictingNodeIds) << "]";
                }
            }
            *RecoveryConflictingClaimCount = conflictingClaimCount;

            i64 quarantinedRecordCount = 0;
            for (const auto& operation : RecoveredOperations) {
                TString quarantineReason;
                if (!operation.ClaimError.empty()) {
                    quarantineReason = operation.ClaimError;
                    ++IncompleteQuarantinedClaimRecords;
                } else if (!operation.MetadataError.empty()) {
                    quarantineReason = operation.MetadataError;
                } else if (operation.OperationId.empty()) {
                    quarantineReason = "operation ID is missing";
                }

                if (!quarantineReason.empty()) {
                    ++quarantinedRecordCount;
                    if (!operation.Owner.empty() && !operation.Nodes.empty()) {
                        AddQuarantinedClaims(operation.Owner, ssize(operation.Nodes));
                    }
                    RM_LOG(ERROR) << "Quarantining recovered operation: owner=" << operation.Owner
                                  << " operation_id=" << operation.OperationId
                                  << " node_ids=[" << JoinSeq(",", operation.Nodes) << "]"
                                  << " reason=" << quarantineReason;
                    continue;
                }

                RM_LOG(DEBUG) << "Attach to " << operation.OperationId << "|" << operation.Owner
                              << " node_ids=[" << JoinSeq(",", operation.Nodes) << "]";

                auto& status = RunningOperations[operation.Owner];
                status.MutationId = operation.Owner;
                status.OperationId = operation.OperationId;
                status.Nodes = operation.Nodes;

                StartOperationWatcher(operation.OperationId, operation.Owner, ctx);
            }
            *RecoveryQuarantinedRecordCount = quarantinedRecordCount;
            UpdateHealthCounters();
            RecoveredOperations.clear();

            Become(&TYtResourceManager::Leader);
            Tick();
        }

        void SendOperationIdUpdate(const TString& mutationId)
        {
            auto updateIt = PendingOperationIdUpdates.find(mutationId);
            Y_ABORT_UNLESS(updateIt != PendingOperationIdUpdates.end());
            Y_ABORT_UNLESS(!updateIt->second.RequestId);

            const ui64 requestId = YtRequestId++;
            updateIt->second.RequestId = requestId;
            const auto requestInserted = OperationIdUpdateRequests.emplace(requestId, mutationId).second;
            Y_ABORT_UNLESS(requestInserted);

            NYT::NApi::TSetNodeOptions options;
            options.PrerequisiteTransactionIds.push_back(LeaderTransactionId);
            Send(YtWrapper, new TEvSetNode(
                requestId,
                ClusterOperationsPath + "/" + mutationId + "/@" + NCommonAttrs::OPERATIONID_ATTR,
                NYT::NYson::TYsonString(NYT::NodeToYsonString(NYT::TNode(updateIt->second.OperationId))),
                options));
        }

        void ScheduleOperationIdUpdate(const TString& mutationId, const TString& operationId)
        {
            const auto updateInserted = PendingOperationIdUpdates.emplace(
                mutationId,
                TPendingOperationIdUpdate{.OperationId = operationId}).second;
            Y_ABORT_UNLESS(updateInserted);
            UpdateHealthCounters();
            SendOperationIdUpdate(mutationId);
        }

        void ForgetOperationIdUpdate(const TString& mutationId)
        {
            const auto updateIt = PendingOperationIdUpdates.find(mutationId);
            if (updateIt == PendingOperationIdUpdates.end()) {
                return;
            }
            if (updateIt->second.RequestId) {
                OperationIdUpdateRequests.erase(*updateIt->second.RequestId);
            }
            PendingOperationIdUpdates.erase(updateIt);
            UpdateHealthCounters();
        }

        void RetryOperationIdUpdates()
        {
            for (const auto& [mutationId, update] : PendingOperationIdUpdates) {
                if (!update.RequestId) {
                    SendOperationIdUpdate(mutationId);
                }
            }
        }

        void OnSetNodeResponse(TEvSetNodeResponse::TPtr& ev, const NActors::TActorContext&)
        {
            const ui64 requestId = ev->Get()->RequestId;
            const auto requestIt = OperationIdUpdateRequests.find(requestId);
            if (requestIt == OperationIdUpdateRequests.end()) {
                RM_LOG(DEBUG) << "Ignoring stale operation ID update response: request_id=" << requestId;
                return;
            }

            const TString mutationId = requestIt->second;
            OperationIdUpdateRequests.erase(requestIt);

            auto updateIt = PendingOperationIdUpdates.find(mutationId);
            if (updateIt == PendingOperationIdUpdates.end() ||
                updateIt->second.RequestId != requestId)
            {
                RM_LOG(DEBUG) << "Ignoring stale operation ID update response: request_id=" << requestId
                              << " mutation=" << mutationId;
                return;
            }

            const auto operationIt = RunningOperations.find(mutationId);
            if (operationIt == RunningOperations.end() ||
                operationIt->second.OperationId != updateIt->second.OperationId)
            {
                PendingOperationIdUpdates.erase(updateIt);
                UpdateHealthCounters();
                RM_LOG(DEBUG) << "Ignoring stale operation ID update response: request_id=" << requestId
                              << " mutation=" << mutationId;
                return;
            }

            updateIt->second.RequestId.Clear();
            const auto result = std::get<0>(*ev->Get());
            if (result.IsOK()) {
                PendingOperationIdUpdates.erase(updateIt);
                UpdateHealthCounters();
                RM_LOG(DEBUG) << "Operation ID persisted: mutation=" << mutationId;
            } else {
                RM_LOG(ERROR) << "Cannot persist operation ID; retrying on next tick: mutation=" << mutationId
                              << " operation_id=" << operationIt->second.OperationId
                              << " error=" << ToString(result);
            }
        }

        void OnStartOperationResponse(TEvStartOperationResponse::TPtr& ev, const NActors::TActorContext& ctx) {
            auto result = std::get<0>(*ev->Get());
            auto requestId = ev->Get()->RequestId;

            auto maybeJobs = PendingStartOperationRequests.find(requestId);
            if (maybeJobs == PendingStartOperationRequests.end()) {
                RM_LOG(WARN) << "Ignoring stale start operation response: request_id=" << requestId
                    << " result=" << ToString(result);
                return;
            }

            auto mutationId = maybeJobs->second.MutationId;

            if (!result.IsOK()) {
                RunningOperations.erase(mutationId);
                AddQuarantinedClaims(mutationId, ssize(maybeJobs->second.Nodes));
                *UnknownStartOutcomeCount += 1;
                *UnknownStartOutcomeClaimCount += ssize(maybeJobs->second.Nodes);
                UpdateHealthCounters();
                // TODO(lucius): Reconcile ambiguous Start outcomes in DQ-134.
                RM_LOG(WARN) << "Operation start outcome is unknown; node IDs remain blocked: mutation=" << mutationId
                             << " node_ids=[" << JoinSeq(",", maybeJobs->second.Nodes) << "]"
                             << " error=" << ToString(result);
            } else {
                auto operationId = ToString(result.Value());
                Y_ABORT_UNLESS(RunningOperations.contains(mutationId));

                RunningOperations[mutationId].OperationId = operationId;
                ScheduleOperationIdUpdate(mutationId, operationId);
                StartOperationWatcher(operationId, mutationId, ctx);
            }

            PendingStartOperationRequests.erase(maybeJobs);
        }

        void ListOperations() {
            NYT::NApi::TListNodeOptions options;
            options.Attributes = {
                "yql_mutation_id",
                NCommonAttrs::OPERATIONSIZE_ATTR,
                NCommonAttrs::OPERATIONID_ATTR,
                NCommonAttrs::CLUSTERNAME_ATTR,
                NCommonAttrs::ACTOR_NODEID_ATTR,
            };
            auto command = new TEvListNode(ClusterOperationsPath, options);
            ListOperationsRequestId = command->RequestId;
            RM_LOG(DEBUG) << "List " << ClusterOperationsPath;
            Send(YtWrapper, command);
        }

        void ListWorkers() {
            NYT::NApi::TListNodeOptions options;
            options.Attributes = {
                NCommonAttrs::ACTOR_NODEID_ATTR,
                NCommonAttrs::OPERATIONID_ATTR,
                NCommonAttrs::OPERATIONSIZE_ATTR,
                NCommonAttrs::JOBID_ATTR,
                NCommonAttrs::ROLE_ATTR,
                NCommonAttrs::CLUSTERNAME_ATTR,
                "modification_time"
            };
            options.ReadFrom = NYT::NApi::EMasterChannelKind::Cache;
            auto command = new TEvListNode(CoordinatorConfig.GetPrefix() + "/worker_node", options);
            Send(CoordinatorWrapper, command);
        }

        void StartOperations(i64 jobs, const NActors::TActorContext& ctx) {
            const i64 jobsPerOperation = Options.YtBackend.HasJobsPerOperation()
                ? Options.YtBackend.GetJobsPerOperation()
                : Options.YtBackend.GetMaxJobs();

            Y_ABORT_UNLESS(jobsPerOperation > 0);

            i64 startedJobs = 0;
            for (i64 i = 0; i < jobs; i += jobsPerOperation) {
                if (jobs - i >= jobsPerOperation) {
                    if (!StartOperation(jobsPerOperation, ctx)) {
                        break;
                    }
                    startedJobs += jobsPerOperation;
                }
            }

            const i64 remainingJobs = jobs - startedJobs;
            if (remainingJobs > 0) {
                RM_LOG(WARN) << "Some jobs will not be started: started_jobs=" << startedJobs
                    << " requested_jobs=" << jobs
                    << " remaining_jobs=" << remainingJobs
                    << " jobs_per_operation=" << jobsPerOperation;
            }
        }

        TString GetOperationSpec(const TVector<ui32>& nodes, const TString& command, const TMaybe<NYT::TNode>& filePaths) const
        {
            const int actorPort = Options.YtBackend.HasActorStartPort()
                ? Options.YtBackend.GetActorStartPort()
                : 31002;

            const bool samePorts = !Options.YtBackend.HasSameActorPorts() || Options.YtBackend.GetSameActorPorts();

            auto minNodeId = Options.YtBackend.GetMinNodeId();

            Y_ABORT_UNLESS(!nodes.empty());

            TString coordinatorStr;
            TStringOutput output1(coordinatorStr);
            SerializeToTextFormat(CoordinatorConfig, output1);

            TString backendStr;
            TStringOutput output2(backendStr);
            SerializeToTextFormat(Options.YtBackend, output2);
            const ui32 tableNumber = *nodes.begin();
            const TString coreTablePath = TStringBuilder() << Options.UploadPrefix << "/CoreTable-" << tableNumber;
            const TString stderrTablePath = TStringBuilder() << Options.UploadPrefix << "/StderrTable-" << tableNumber;
            const TString fileCache = "file_cache2";

            TVector<std::pair<TString, TString>> initialFileList;
            initialFileList.reserve(Options.Files.size() + Options.YtBackend.GetPortoLayer().size());
            for (const auto& fname : Options.Files) {
                initialFileList.push_back(std::make_pair(Options.UploadPrefix, fname.GetRemoteFileName()));
            }
            for (const auto& layer : Options.YtBackend.GetPortoLayer()) {
                auto pos = layer.rfind('/');
                auto baseName = layer.substr(0, pos);
                auto name = layer.substr(pos + 1);
                initialFileList.push_back(std::make_pair(baseName, name));
            }

            TVector<TString> operationLayersList;
            for (const auto& operationLayer : Options.YtBackend.GetOperationLayer()) {
                operationLayersList.push_back(operationLayer);
            }

            auto operationSpec = NYT::BuildYsonNodeFluently()
                .BeginMap()
                    .DoIf(Options.YtBackend.GetOwner().size() > 0, [&] (NYT::TFluentMap fluent) {
                        fluent.Item("acl").BeginList()
                            .Item()
                                .BeginMap()
                                    .Item("action").Value("allow")
                                    .Item("permissions")
                                        .BeginList()
                                            .Item().Value("read")
                                            .Item().Value("manage")
                                        .EndList()
                                    .Item("subjects")
                                        .BeginList()
                                            .DoFor(Options.YtBackend.GetOwner(), [&] (NYT::TFluentList fluent1, const TString& subject) {
                                                fluent1.Item().Value(subject);
                                            })
                                        .EndList()
                                .EndMap()
                        .EndList();
                    })
                    .Item("secure_vault")
                        .BeginMap()
                            .Item(NCommonJobVars::YT_COORDINATOR).Value(coordinatorStr)
                            .Item(NCommonJobVars::YT_BACKEND).Value(backendStr)
                            .Item(NCommonJobVars::YT_FORCE_IPV4).Value(Options.ForceIPv4)
                            .DoFor(Options.YtBackend.GetVaultEnv(), [&] (NYT::TFluentMap fluent, const NYql::NProto::TDqConfig::TAttr& envVar) { // Добавляем env variables
                                TString tokenValue;
                                try {
                                    tokenValue = StripString(TFileInput(envVar.GetValue()).ReadLine());
                                } catch (...) {
                                    throw yexception() << "Cannot read file " << envVar.GetValue() << " Reason: " << CurrentExceptionMessage();
                                }
                                fluent.Item(envVar.GetName()).Value(tokenValue);
                            })
                        .EndMap()
                    .Item("core_table_path").Value(coreTablePath)
                    .Item("stderr_table_path").Value(stderrTablePath)
                    .Item("try_avoid_duplicating_jobs").Value(true)
                    .DoIf(!Options.YtBackend.GetPool().empty(), [&] (NYT::TFluentMap fluent) {
                        fluent.Item("pool").Value(Options.YtBackend.GetPool());
                    })
                    .DoIf(Options.YtBackend.GetPoolTrees().size() > 0, [&] (NYT::TFluentMap fluent) {
                        fluent.Item("pool_trees")
                            .BeginList()
                                .DoFor(Options.YtBackend.GetPoolTrees(), [&](NYT::TFluentList fluent1, const TString& subject) {
                                    fluent1.Item().Value(subject);
                                })
                            .EndList();
                    })
                    .DoIf(!Options.YtBackend.GetSchedulingTagFilter().empty(), [&] (NYT::TFluentMap fluent) {
                        fluent.Item("scheduling_tag_filter").Value(Options.YtBackend.GetSchedulingTagFilter());
                    })
                    .Item("tasks")
                        .BeginMap()
                            .DoFor(nodes, [&] (NYT::TFluentMap fluent1, const auto& nodeId) {
                                fluent1.Item(YqlWorkerTaskPrefix + ToString(nodeId))
                                    .BeginMap()
                                        .DoIf(Options.YtBackend.GetNetworkProject().size() > 0, [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("network_project").Value(Options.YtBackend.GetNetworkProject());
                                        })
                                        .DoIf(Options.YtBackend.HasEnablePorto(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("enable_porto").Value(Options.YtBackend.GetEnablePorto());
                                        })
                                        .DoIf(Options.YtBackend.HasContainerCpuLimit(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("set_container_cpu_limit").Value(Options.YtBackend.GetContainerCpuLimit());
                                        })
                                        .Item("command").Value(command)
                                        .DoIf(!operationLayersList.empty(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("layer_paths").DoListFor(operationLayersList, [&] (NYT::TFluentList list, const TString& operationLayer) {
                                                list.Item().Value(operationLayer);
                                            });
                                        })
                                        .Item("environment")
                                            .BeginMap()
                                                .Item(NCommonJobVars::ACTOR_PORT).Value(ToString(
                                                    samePorts
                                                        ? actorPort
                                                        : actorPort + nodeId - minNodeId))
                                                .Item(NCommonJobVars::OPERATION_SIZE).Value(ToString(nodes.size()))
                                                .Item(NCommonJobVars::UDFS_PATH).Value(fileCache)
                                                .Item(NCommonJobVars::ACTOR_NODE_ID).Value(ToString(nodeId))
                                                .DoIf(!!Options.AddressResolverConfig, [&](NYT::TFluentMap fluent) {
                                                    fluent.Item(NCommonJobVars::ADDRESS_RESOLVER_CONFIG).Value(ToString(NYT::NYson::ConvertToYsonString(Options.AddressResolverConfig, NYT::NYson::EYsonFormat::Text)));
                                                })
                                                .DoIf(!!GetEnv("YQL_DETERMINISTIC_MODE"), [&](NYT::TFluentMap fluent) {
                                                    fluent.Item("YQL_DETERMINISTIC_MODE").Value("1");
                                                })
                                            .EndMap()
                                        .DoIf(!Options.YtBackend.GetProxyAddress().StartsWith("localhost") && filePaths.Empty(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("file_paths").DoListFor(initialFileList, [&] (NYT::TFluentList list, const std::pair<TString, TString>& item) {
                                                auto baseName = item.second;
                                                list.Item()
                                                    .BeginAttributes()
                                                        .Item("executable").Value(true)
                                                        .Item("file_name").Value(baseName)
                                                    .EndAttributes()
                                                    .Value(item.first + "/" + baseName);
                                            });
                                        })
                                        .DoIf(!filePaths.Empty(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("file_paths").Value(*filePaths);
                                        })
                                        .Item("job_count").Value(1)
                                        .Item("port_count").Value(1)
                                        .DoIf(Options.YtBackend.HasMemoryLimit(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("memory_limit").Value(Options.YtBackend.GetMemoryLimit());
                                        })
                                        .DoIf(Options.YtBackend.HasCpuLimit(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("cpu_limit").Value(Options.YtBackend.GetCpuLimit());
                                        })
                                        .DoIf(Options.YtBackend.HasUseTmpFs() && Options.YtBackend.GetUseTmpFs(), [&] (NYT::TFluentMap fluent) {
                                            fluent.Item("tmpfs_path").Value(fileCache);
                                        })
                                        .DoIf(Options.YtBackend.HasDiskRequest(), [&] (NYT::TFluentMap fluent) {
                                            auto& diskRequest = Options.YtBackend.GetDiskRequest();
                                            NYT::TNode diskNode = NYT::TNode::CreateMap();
                                            if (diskRequest.HasAdditionalSpecYson()) {
                                                diskNode = NYT::NodeFromYsonString(diskRequest.GetAdditionalSpecYson());
                                            }
                                            if (diskRequest.HasDiskSpace()) {
                                                diskNode["disk_space"] = diskRequest.GetDiskSpace();
                                            }
                                            if (diskRequest.HasInodeCount()) {
                                                diskNode["inode_count"] = diskRequest.GetInodeCount();
                                            }
                                            if (diskRequest.HasAccount()) {
                                                diskNode["account"] = diskRequest.GetAccount();
                                            }
                                            if (diskRequest.HasMediumName()) {
                                                diskNode["medium_name"] = diskRequest.GetMediumName();
                                            }
                                            fluent.Item("disk_request").Value(diskNode);
                                        })
                                    .EndMap();
                            })
                        .EndMap()
                .EndMap();

            return NYT::NodeToYsonString(operationSpec);
        }

        bool StartOperation(i64 jobs, const NActors::TActorContext& ctx) {
            Y_UNUSED(ctx);

            RM_LOG(INFO) << "Creating " << jobs << " workers ";

            TString executableName = (Options.YtBackend.GetProxyAddress().StartsWith("localhost"))
                ? Options.Files[0].LocalFileName
                : TString("./") + Options.Files[0].GetRemoteFileName();

            RM_LOG(INFO) << "Executable " << executableName;

            TString command = Options.YtBackend.GetVanillaJobCommand();

            RM_LOG(INFO) << "Vanilla job command " << command;

            TVector<ui32> nodes;

            auto startOperationOptions = NYT::NApi::TStartOperationOptions();

            startOperationOptions.MutationId = startOperationOptions.GetOrGenerateMutationId();
            const auto mutationIdStr = ToString(startOperationOptions.MutationId);
            if (!NodeIdAllocator.Allocate(mutationIdStr, jobs, &nodes)) {
                RM_LOG(WARN) << "Cannot allocate node IDs: jobs=" << jobs
                    << " mutation_id=" << mutationIdStr;
                return false;
            }

            startOperationOptions.Retry = true;

            TString operationSpec;
            NYT::NApi::TCreateNodeOptions createOptions;
            try {
                operationSpec = GetOperationSpec(nodes, command, TMaybe<NYT::TNode>());

                createOptions.IgnoreExisting = true;
                createOptions.Recursive = true;

                auto filesAttribute = Options.Files;
                if (Options.YtBackend.GetProxyAddress().StartsWith("localhost")) {
                    filesAttribute.clear();
                }

                // Keep launch inputs for rollback compatibility and DQ-134 reconciliation.
                auto attributes = NYT::BuildYsonNodeFluently()
                    .BeginMap()
                        .Item("yql_mutation_id").Value(mutationIdStr)
                        .Item(NCommonAttrs::OPERATIONSIZE_ATTR).Value(jobs)
                        .Item("yql_command").Value(command)
                        .Item("yql_file_paths")
                            .DoListFor(filesAttribute, [&] (NYT::TFluentList list, const TResourceFile& item) {
                                auto baseName = item.GetRemoteFileName();
                                list.Item()
                                    .BeginAttributes()
                                        .Item("executable").Value(true)
                                        .Item("file_name").Value(baseName)
                                    .EndAttributes()
                                    .Value(Options.UploadPrefix + "/" + baseName);
                            })
                        .Item(NCommonAttrs::ROLE_ATTR).Value("worker_node")
                        .Item(NCommonAttrs::ACTOR_NODEID_ATTR)
                            .BeginList()
                                .DoFor(nodes, [&] (NYT::TFluentList fluent1, const auto& nodeId) {
                                    fluent1.Item().Value(nodeId);
                                })
                            .EndList()
                        .Item(NCommonAttrs::CLUSTERNAME_ATTR).Value(ClusterName)
                    .EndMap();

                createOptions.Attributes = NYT::NYTree::IAttributeDictionary::FromMap(
                    NYT::NYTree::ConvertToNode(NYT::NYson::TYsonString(NYT::NodeToYsonString(attributes)))->AsMap());

                createOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);

                CreateCoreTable(*nodes.begin());
            } catch (...) {
                NodeIdAllocator.Release(mutationIdStr);
                RM_LOG(ERROR) << "Cannot prepare operation: mutation_id=" << mutationIdStr
                    << " error=" << CurrentExceptionMessage();
                return false;
            }

            RM_LOG(DEBUG) << "Start operation with mutationId " << mutationIdStr;

            auto& state = RunningOperations[mutationIdStr];
            state.MutationId = mutationIdStr;
            state.Nodes = nodes;

            RM_LOG(DEBUG) << "Creating operation with mutationId " << mutationIdStr
                          << " node_ids=[" << JoinSeq(",", nodes) << "]";

            Send(YtWrapper, new TEvCreateNode(
                YtRequestId,
                ClusterOperationsPath + "/" + mutationIdStr,
                NYT::NObjectClient::EObjectType::StringNode,
                createOptions));

            PendingStartOperationRequests[YtRequestId] = {
                nodes,
                mutationIdStr,
                MakeHolder<TEvStartOperation>(
                    YtRequestId + 1,
                    NYT::NScheduler::EOperationType::Vanilla,
                    operationSpec,
                    startOperationOptions)
            };
            YtRequestId += 2;
            return true;
        }

        void OnCreateNode(TEvCreateNodeResponse::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto requestId = ev->Get()->RequestId;
            auto result = std::get<0>(*ev->Get());
            if (requestId == static_cast<ui64>(-1)) {
                // CoreTable
                if (!result.IsOK()) {
                    YQL_CLOG(DEBUG, ProviderDq) << "Error on creating core table " << ToString(result);
                }
                return;
            }
            const auto it = PendingStartOperationRequests.find(requestId);
            if (it == PendingStartOperationRequests.end()) {
                return;
            }
            auto& op = it->second;
            if (result.IsOK()) {
                const auto startOperationRequestId = requestId + 1;
                Y_ABORT_UNLESS(!PendingStartOperationRequests.contains(startOperationRequestId));
                PendingStartOperationRequests[startOperationRequestId] = {
                    op.Nodes,
                    op.MutationId,
                    THolder<TEvStartOperation>()
                };
                Send(YtWrapper, op.Ev.Release());
            } else if (RunningOperations.contains(op.MutationId)) {
                YQL_CLOG(DEBUG, ProviderDq) << "Error on create node " << ToString(result);
                DropRunningOperation(op.MutationId);
            }
            PendingStartOperationRequests.erase(it);
            // retry in ListOperations
        }

private:
        const TResourceManagerOptions Options;
        TIntrusivePtr<NMonitoring::TDynamicCounters> Counters;

        const NMonitoring::TDynamicCounters::TCounterPtr RecoveryQuarantinedRecordCount;
        const NMonitoring::TDynamicCounters::TCounterPtr RecoveryConflictingClaimCount;
        const NMonitoring::TDynamicCounters::TCounterPtr QuarantinedOwnerCount;
        const NMonitoring::TDynamicCounters::TCounterPtr QuarantinedClaimCount;
        const NMonitoring::TDynamicCounters::TCounterPtr CapacityBlockedByQuarantine;
        const NMonitoring::TDynamicCounters::TCounterPtr OverlappingClaimCount;
        const NMonitoring::TDynamicCounters::TCounterPtr IncompleteQuarantinedClaimRecordCount;
        const NMonitoring::TDynamicCounters::TCounterPtr PendingOperationIdUpdateCount;
        const NMonitoring::TDynamicCounters::TCounterPtr UnknownStartOutcomeCount;
        const NMonitoring::TDynamicCounters::TCounterPtr UnknownStartOutcomeClaimCount;

        const TString ClusterName;
        const TString ClusterOperationsPath;

        const ICoordinationHelper::TPtr Coordinator;

        const NProto::TDqConfig::TYtCoordinator CoordinatorConfig;

        TActorId YtWrapper;
        const TActorId CoordinatorWrapper;

        NYT::NObjectClient::TTransactionId LeaderTransactionId;

        TMaybe<ui64> ListOperationsRequestId;
        TNodeIdAllocator NodeIdAllocator;

        struct TRecoveredOperation {
            TString Owner;
            TString OperationId;
            TVector<ui32> Nodes;
            TMaybe<i64> OperationSize;
            TString ClaimError;
            TString MetadataError;
        };

        TVector<TRecoveredOperation> RecoveredOperations;
        THashMap<ui64, int> ClaimReconciliationRequests;

        struct TOperationStatus {
            TString OperationId;
            TString MutationId;
            TActorId ActorId;
            TVector<ui32> Nodes;
        };

        // mutationId -> operation
        THashMap<TString, TOperationStatus> RunningOperations;

        THashSet<TString> QuarantinedOwners;
        i64 QuarantinedClaims = 0;
        i64 IncompleteQuarantinedClaimRecords = 0;

        // RequestId -> Jobs
        struct TPendingStartOperation {
            TVector<ui32> Nodes;
            TString MutationId;
            THolder<TEvStartOperation> Ev;
        };
        THashMap<ui64, TPendingStartOperation> PendingStartOperationRequests;

        struct TPendingOperationIdUpdate {
            TString OperationId;
            TMaybe<ui64> RequestId;
        };
        THashMap<TString, TPendingOperationIdUpdate> PendingOperationIdUpdates;
        THashMap<ui64, TString> OperationIdUpdateRequests;

        struct TPendingNodeRelease {
            TString MutationId;
            TVector<ui32> Nodes;
        };
        THashMap<ui64, TPendingNodeRelease> PendingNodeReleases;

        ui64 YtRequestId = 1;
        NActors::TSchedulerCookieHolder TimerCookieHolder;
    };

    IActor* CreateYtResourceManager(
        const TResourceManagerOptions& options,
        const ICoordinationHelper::TPtr& coordinator)
    {
        Y_ABORT_UNLESS(!options.YtBackend.GetProxyAddress().empty());
        Y_ABORT_UNLESS(!options.YtBackend.GetUser().empty());
        Y_ABORT_UNLESS(options.YtBackend.HasMinNodeId());
        Y_ABORT_UNLESS(options.YtBackend.HasMaxNodeId());
        Y_ABORT_UNLESS(options.YtBackend.HasPrefix());
        Y_ABORT_UNLESS(!options.Files.empty());
        Y_ABORT_UNLESS(!options.UploadPrefix.empty());

        return new TYtResourceManager(options, coordinator);
    }
} // namespace NYql
