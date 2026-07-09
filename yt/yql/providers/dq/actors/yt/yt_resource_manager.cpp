#include "yt_wrapper.h"
#include "node_id_allocator.h"

#include <util/thread/pool.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
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
            , ClusterName(Options.YtBackend.GetClusterName())
            , ClusterOperationsPath(Options.YtBackend.GetPrefix() + "/operations/" + Options.YtBackend.GetClusterName())
            , Coordinator(coordinator)
            , CoordinatorConfig(Coordinator->GetConfig())
            , CoordinatorWrapper(Coordinator->GetWrapper())
            , NodeIdAllocator(Options.YtBackend.GetMinNodeId(), Options.YtBackend.GetMaxNodeId())
        {
        }

    private:
        // States: Follower <-> (ListOperations -> Leader)

        void StartFollower(TEvBecomeFollower::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);

            auto leaderAttributes = NYT::NodeFromYsonString(ev->Get()->Attributes).AsMap();
            LeaderTransactionId = NYT::NObjectClient::TTransactionId();

            RM_LOG(INFO) << "Become follower, leader=" << leaderAttributes.at(NCommonAttrs::ACTOR_NODEID_ATTR).AsUint64()
                         << " RunningOperations=" << RunningOperations.size()
                         << " MutationsCache=" << MutationsCache.size()
                         << " PendingNodeReleases=" << PendingNodeReleases.size();
            if (!MutationsCache.empty()) {
                for (const auto& [mid, nodes] : MutationsCache) {
                    RM_LOG(WARN) << "Dropping stale MutationsCache entry on follower transition: mutation=" << mid << " nodes=[" << JoinSeq(",", nodes) << "]";
                }
                MutationsCache.clear();
            }
            for (const auto& [k, v] : RunningOperations) {
                UnregisterChild(v.ActorId);
            }
            RunningOperations.clear();
            NodeIdAllocator.Clear();
            PendingNodeReleases.clear();
            Become(&TYtResourceManager::Follower);
        }

        void StartLeader(TEvBecomeLeader::TPtr& ev, const TActorContext& ctx) {
            Y_UNUSED(ctx);
            RM_LOG(INFO) << "Become leader, epoch=" << ev->Get()->LeaderEpoch
                         << " MutationsCache=" << MutationsCache.size();
            for (const auto& [mid, nodes] : MutationsCache) {
                RM_LOG(DEBUG) << "  MutationsCache entry: mutation=" << mid << " nodes=[" << JoinSeq(",", nodes) << "]";
            }

            LeaderTransactionId = NYT::NObjectClient::TTransactionId::FromString(ev->Get()->LeaderTransaction);

            ListOperations();
            Become(&TYtResourceManager::ListOperationsState);
        }

        STRICT_STFUNC(Follower, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvBecomeLeader, StartLeader)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            CFunc(TEvents::TEvBootstrap::EventType, Bootstrap)

            IgnoreFunc(TEvTick)
            IgnoreFunc(TEvDropOperation)
            IgnoreFunc(TEvStartOperationResponse)
            IgnoreFunc(TEvCreateNodeResponse)
            IgnoreFunc(TEvSetNodeResponse)
            IgnoreFunc(TEvRemoveNodeResponse)
        })

        STRICT_STFUNC(ListOperationsState, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvDropOperation, OnDropOperation)
            HFunc(TEvListNodeResponse, OnListOperations)
            HFunc(TEvStartOperationResponse, OnStartOperationResponse)
            HFunc(TEvRemoveNodeResponse, OnRemoveNodeResponse)
            cFunc(TEvTick::EventType, ListOperations)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            IgnoreFunc(TEvCreateNodeResponse)
            IgnoreFunc(TEvSetNodeResponse)
        })

        STRICT_STFUNC(Leader, {
            HFunc(TEvBecomeFollower, StartFollower)
            HFunc(TEvDropOperation, OnDropOperation)
            HFunc(TEvListNodeResponse, OnListResponse)
            HFunc(TEvStartOperationResponse, OnStartOperationResponse)
            cFunc(TEvents::TEvPoison::EventType, PassAway)
            cFunc(TEvTick::EventType, [this]() {
                ListWorkers();
                Tick();
            })
            HFunc(TEvCreateNodeResponse, OnCreateNode)
            HFunc(TEvRemoveNodeResponse, OnRemoveNodeResponse)
            IgnoreFunc(TEvSetNodeResponse)
        })

        TAutoPtr<IEventHandle> AfterRegister(const TActorId& self, const TActorId& parentId) override {
            return new IEventHandle(self, parentId, new TEvents::TEvBootstrap, 0);
        }

        void Bootstrap(const NActors::TActorContext& ctx) {
            YtWrapper = Coordinator->GetWrapper(
                ctx.ActorSystem(),
                Options.YtBackend.GetClusterName(),
                Options.YtBackend.GetUser(),
                Options.YtBackend.GetToken());
            RegisterChild(Coordinator->CreateLockOnCluster(YtWrapper, Options.YtBackend.GetPrefix(), Options.LockName, false));
        }

        void Tick() {
            TimerCookieHolder.Reset(NActors::ISchedulerCookie::Make2Way());
            Schedule(Options.TickInterval, new TEvTick(), TimerCookieHolder.Get());
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

            int totalJobs = 0;
            for (const auto& [k, v] : RunningOperations) {
                totalJobs += v.Nodes.size();
                RM_LOG(DEBUG) << "Operation: " << k << " " << v.Nodes.size() << " ";
            }

            RM_LOG(DEBUG) << "Running/Max jobs: " << totalJobs << "/" << Options.YtBackend.GetMaxJobs();

            int needToStart = Options.YtBackend.GetMaxJobs() - totalJobs;
            RM_LOG(DEBUG) << "Need to start: " << needToStart;
            if (needToStart > 0) {
                StartOperations(needToStart, ctx);
            }
        }

        void DropRunningOperation(const TString& mutationId, const TVector<ui32>& preserve = {}) {
            TVector<ui32> currentNodes;
            const auto it = RunningOperations.find(mutationId);
            if (it != RunningOperations.end()) {
                currentNodes = it->second.Nodes;
                RM_LOG(DEBUG) << "DropRunningOperation mutation=" << mutationId
                              << " operationId=" << it->second.OperationId
                              << " node_ids=[" << JoinSeq(",", currentNodes) << "]"
                              << (preserve.empty() ? " action=remove_coordinator_node" : " action=preserve_in_cache");
                RunningOperations.erase(it);
            } else {
                RM_LOG(DEBUG) << "DropRunningOperation mutation=" << mutationId << " (not in RunningOperations)"
                              << (preserve.empty() ? " action=remove_coordinator_node" : " action=preserve_in_cache");
            }

            if (!preserve.empty()) {
                // Freeing current nodes then re-allocating preserve nodes (may be same set).
                NodeIdAllocator.Deallocate(currentNodes);
                MutationsCache[mutationId] = preserve;
                NodeIdAllocator.Allocate(preserve);
                RM_LOG(WARN) << "Operation in unknown state, preserve mutation=" << mutationId
                             << " node_ids=[" << JoinSeq(",", preserve) << "] MutationsCache.size=" << MutationsCache.size();
            } else {
                // Do NOT free node_ids yet — they stay allocated until the operation node
                // is confirmed removed from Cypress. This prevents a new operation from reusing
                // the same node_ids while the old operation node is still in Cypress
                // (which would happen if TEvRemoveNode fails silently).
                auto removePath = ClusterOperationsPath + "/" + mutationId;
                RM_LOG(DEBUG) << "Removing operation node " << removePath
                              << " LeaderTxn=" << ToString(LeaderTransactionId)
                              << " deferring release of node_ids=[" << JoinSeq(",", currentNodes) << "]";
                NYT::NApi::TRemoveNodeOptions removeNodeOptions;
                removeNodeOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);
                PendingNodeReleases[YtRequestId] = {mutationId, currentNodes};
                Send(YtWrapper, new TEvRemoveNode(YtRequestId++, removePath, removeNodeOptions));
            }
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
                NodeIdAllocator.Deallocate(nodeIds);
            } else {
                RM_LOG(ERROR) << "OPERATION NODE REMOVE FAILED: mutation=" << mutationId
                              << " node_ids=[" << JoinSeq(",", nodeIds) << "] remain blocked until next epoch"
                              << " LeaderTxn=" << ToString(LeaderTransactionId)
                              << " error=" << ToString(result);
                // node_ids intentionally left allocated — the operation node may still exist in
                // Cypress with these IDs. They will be freed by NodeIdAllocator.Clear() when
                // leadership is lost, and the new leader will re-evaluate via OnListOperations.
            }
        }

        void OnDropOperation(TEvDropOperation::TPtr& ev, const NActors::TActorContext& ctx) {
            Y_UNUSED(ctx);
            auto operationId = ev->Get()->OperationId;
            auto mutationId = ev->Get()->MutationId;
            auto maybeOperation = RunningOperations.find(mutationId);
            if (maybeOperation != RunningOperations.end()) {
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

        void OnListOperations(TEvListNodeResponse::TPtr& ev, const TActorContext& ctx)
        {
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

            const auto operationsList = NYT::NodeFromYsonString(result.Value()).AsList();
            RM_LOG(DEBUG) << "OnListOperations: " << operationsList.size() << " operation nodes";

            // Pass 1: attach to operations that are already running in YT (have OPERATIONID_ATTR).
            // Running operations claim their node_ids first so that pass 2 can detect conflicts.
            for (const auto& opNode : operationsList) {
                const auto& attributes = opNode.GetAttributes().AsMap();

                if (attributes.find(NCommonAttrs::OPERATIONID_ATTR) == attributes.end()) {
                    continue; // handled in pass 2
                }

                auto mutationIdStr = attributes.find("yql_mutation_id")->second.AsString();
                auto nodeIdsIt = attributes.find(NCommonAttrs::ACTOR_NODEID_ATTR);

                if (nodeIdsIt == attributes.end()) {
                    DropRunningOperation(mutationIdStr);
                    continue;
                }

                const auto& nodeList = nodeIdsIt->second.AsList();
                TVector<ui32> nodeIds;
                nodeIds.reserve(nodeList.size());
                for (const auto& n : nodeList) {
                    nodeIds.push_back(n.AsUint64());
                }

                auto operationId = attributes.find(NCommonAttrs::OPERATIONID_ATTR)->second.AsString();
                RM_LOG(DEBUG) << "Attach to " << operationId << "|" << mutationIdStr
                              << " node_ids=[" << JoinSeq(",", nodeIds) << "]";

                auto dups = NodeIdAllocator.Allocate(nodeIds);
                if (!dups.empty()) {
                    RM_LOG(ERROR) << "NODE_ID CONFLICT among running operations: mutation=" << mutationIdStr
                                  << " conflicting ids=[" << JoinSeq(",", dups) << "]";
                }

                auto& status = RunningOperations[mutationIdStr];
                status.MutationId = mutationIdStr;
                status.OperationId = operationId;
                status.Nodes = nodeIds;

                StartOperationWatcher(operationId, mutationIdStr, ctx);
            }

            // Pass 2: start pending operations (no OPERATIONID_ATTR).
            // If their node_ids conflict with a running operation from pass 1, the operation node
            // is stale (TEvRemoveNode failed previously) — remove it from Cypress and skip.
            for (const auto& opNode : operationsList) {
                const auto& attributes = opNode.GetAttributes().AsMap();

                if (attributes.find(NCommonAttrs::OPERATIONID_ATTR) != attributes.end()) {
                    continue; // already handled in pass 1
                }

                auto mutationIdStr = attributes.find("yql_mutation_id")->second.AsString();
                auto mutationId = NYT::TGuid::FromString(mutationIdStr);
                auto command = attributes.find("yql_command")->second.AsString();
                auto filePaths = attributes.find("yql_file_paths")->second;
                auto nodeIdsIt = attributes.find(NCommonAttrs::ACTOR_NODEID_ATTR);

                if (nodeIdsIt == attributes.end()) {
                    DropRunningOperation(mutationIdStr);
                    continue;
                }

                const auto& nodeList = nodeIdsIt->second.AsList();
                TVector<ui32> nodeIds;
                nodeIds.reserve(nodeList.size());
                for (const auto& n : nodeList) {
                    nodeIds.push_back(n.AsUint64());
                }

                RM_LOG(DEBUG) << "Pending mutation " << mutationIdStr
                              << " node_ids=[" << JoinSeq(",", nodeIds) << "]";

                if (!NodeIdAllocator.TryAllocate(nodeIds)) {
                    // node_ids are already claimed by a running operation from pass 1.
                    // This operation node is stale: TEvRemoveNode must have failed previously,
                    // leaving it in Cypress while its node_ids were reused.
                    RM_LOG(ERROR) << "STALE OPERATION NODE: mutation=" << mutationIdStr
                                  << " node_ids=[" << JoinSeq(",", nodeIds) << "] conflict with running operation"
                                  << " => removing stale operation node";
                    // The stale mutation was never added to RunningOperations in this pass,
                    // so DropRunningOperation will not touch any node_ids — they remain owned
                    // by the running operation that claimed them in pass 1.
                    Y_ABORT_UNLESS(!RunningOperations.contains(mutationIdStr),
                        "Stale mutation unexpectedly found in RunningOperations");
                    DropRunningOperation(mutationIdStr);
                    continue;
                }

                RM_LOG(DEBUG) << "Start or attach to " << mutationIdStr;
                StartOrAttachOperation(mutationId, nodeIds, command, filePaths);
            }

            if (PendingStartOperationRequests.empty() && CurrentStateFunc() != &TYtResourceManager::Leader) {
                Become(&TYtResourceManager::Leader);
                Tick();
            }
        }

        void OnStartOperationResponse(TEvStartOperationResponse::TPtr& ev, const NActors::TActorContext& ctx) {
            auto result = std::get<0>(*ev->Get());
            auto requestId = ev->Get()->RequestId;

            auto maybeJobs = PendingStartOperationRequests.find(requestId);

            Y_ABORT_UNLESS(maybeJobs != PendingStartOperationRequests.end());

            auto mutationId = maybeJobs->second.MutationId;

            if (!result.IsOK()) {
                // TODO: Check response code
                RM_LOG(WARN) << "Failed to start operation " << ToString(result);
                DropRunningOperation(mutationId, maybeJobs->second.Nodes);
            } else {
                auto operationId = ToString(result.Value());
                Y_ABORT_UNLESS(RunningOperations.contains(mutationId));

                RunningOperations[mutationId].OperationId = operationId;

                NYT::NApi::TSetNodeOptions setNodeOptions;
                setNodeOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);

                Send(YtWrapper, new TEvSetNode(
                    ClusterOperationsPath + "/" + mutationId + "/@" + NCommonAttrs::OPERATIONID_ATTR,
                    NYT::NYson::TYsonString(NYT::NodeToYsonString(NYT::TNode(operationId))),
                    setNodeOptions));
                StartOperationWatcher(operationId, mutationId, ctx);
            }

            PendingStartOperationRequests.erase(maybeJobs);

            if (PendingStartOperationRequests.empty() && CurrentStateFunc() != &TYtResourceManager::Leader) {
                Become(&TYtResourceManager::Leader);
                Tick();
            }
        }

        void ListOperations() {
            NYT::NApi::TListNodeOptions options;
            options.Attributes = {
                "yql_mutation_id",
                NCommonAttrs::OPERATIONSIZE_ATTR,
                NCommonAttrs::OPERATIONID_ATTR,
                NCommonAttrs::CLUSTERNAME_ATTR,
                NCommonAttrs::ACTOR_NODEID_ATTR,
                "yql_command",
                "yql_file_paths"
            };
            auto command = new TEvListNode(ClusterOperationsPath, options);
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

        void StartOperations(int jobs, const NActors::TActorContext& ctx) {
            int jobsPerOperation = Options.YtBackend.HasJobsPerOperation()
                ? Options.YtBackend.GetJobsPerOperation()
                : Options.YtBackend.GetMaxJobs();

            Y_ABORT_UNLESS(jobsPerOperation > 0);

            for (int i = 0; i < jobs; i += jobsPerOperation) {
                if (jobs - i >= jobsPerOperation) {
                    StartOperation(jobsPerOperation, ctx);
                }
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
                                fluent1.Item("yql_worker_" + ToString(nodeId))
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
                                        .DoIf((Options.YtBackend.GetClusterName().find("localhost") != 0) && filePaths.Empty(), [&] (NYT::TFluentMap fluent) {
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

        void StartOrAttachOperation(
            const NYT::TGuid& mutationId,
            const TVector<ui32>& nodes,
            const TString& command,
            const NYT::TNode& filePaths)
        {
            auto mutationIdStr = ToString(mutationId);
            RM_LOG(INFO) << "Creating " << nodes.size() << " workers " << mutationIdStr;

            auto operationSpec = GetOperationSpec(nodes, command, TMaybe<NYT::TNode>(filePaths));

            auto startOperationOptions = NYT::NApi::TStartOperationOptions();
            startOperationOptions.MutationId = mutationId;
            startOperationOptions.Retry = true;

            auto& state = RunningOperations[mutationIdStr];
            state.MutationId = mutationIdStr;
            state.Nodes = nodes;

            RM_LOG(DEBUG) << "Attaching to operation with mutationId " << mutationIdStr;

            Y_ENSURE(!nodes.empty());
            CreateCoreTable(*nodes.begin());

            Send(YtWrapper, MakeHolder<TEvStartOperation>(
                YtRequestId,
                NYT::NScheduler::EOperationType::Vanilla,
                operationSpec,
                startOperationOptions).Release());

            PendingStartOperationRequests[YtRequestId++] = {nodes,
                mutationIdStr, THolder<TEvStartOperation>()};
        }

        void StartOperation(int jobs, const NActors::TActorContext& ctx) {
            Y_UNUSED(ctx);

            RM_LOG(INFO) << "Creating " << jobs << " workers ";

            TString executableName = (Options.YtBackend.GetClusterName().find("localhost") == 0)
                ? Options.Files[0].LocalFileName
                : TString("./") + Options.Files[0].GetRemoteFileName();

            RM_LOG(INFO) << "Executable " << executableName;

            TString command = Options.YtBackend.GetVanillaJobCommand();

            RM_LOG(INFO) << "Executable " << command;

            TVector<ui32> nodes;

            auto startOperationOptions = NYT::NApi::TStartOperationOptions();

            if (MutationsCache.empty()) {
                startOperationOptions.MutationId = startOperationOptions.GetOrGenerateMutationId();
                NodeIdAllocator.Allocate(nodes, jobs);
            } else {
                const auto cachedMutationIt = MutationsCache.begin();
                const auto& cachedMutationIdStr = cachedMutationIt->first;
                startOperationOptions.MutationId = NYT::TGuid::FromString(cachedMutationIdStr);
                nodes = cachedMutationIt->second;
                {
                    auto dups = NodeIdAllocator.Allocate(nodes);
                    RM_LOG(INFO) << "Get mutation from cache mutation=" << cachedMutationIdStr
                                 << " nodes=" << nodes.size() << " node_ids=[" << JoinSeq(",", nodes) << "]";
                    if (!dups.empty()) {
                        RM_LOG(ERROR) << "NODE_ID CONFLICT restoring from MutationsCache: mutation=" << cachedMutationIdStr
                                      << " conflicting ids=[" << JoinSeq(",", dups) << "]";
                    }
                }
                MutationsCache.erase(cachedMutationIt);
            }

            if (nodes.empty()) {
                RM_LOG(WARN) << "Cannot allocate node ids for " << jobs << " jobs";
                return;
            }

            startOperationOptions.Retry = true;

            auto operationSpec = GetOperationSpec(nodes, command, TMaybe<NYT::TNode>());

            auto mutationId = startOperationOptions.MutationId;
            auto mutationIdStr = ToString(mutationId);

            RM_LOG(DEBUG) << "Start operation with mutationId " << mutationIdStr;

            auto& state = RunningOperations[mutationIdStr];
            state.MutationId = mutationIdStr;
            state.Nodes = nodes;

            NYT::NApi::TCreateNodeOptions createOptions;
            createOptions.IgnoreExisting = true;
            createOptions.Recursive = true;

            RM_LOG(DEBUG) << "Creating operation with mutationId " << mutationIdStr
                          << " node_ids=[" << JoinSeq(",", nodes) << "]";

            auto filesAttribute = Options.Files;
            if (Options.YtBackend.GetClusterName().find("localhost") == 0) {
                filesAttribute.clear();
            }

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
                NYT::NYTree::ConvertToNode(NYT::NYson::TYsonString(NYT::NodeToYsonString(attributes)))->AsMap()
            );

            createOptions.PrerequisiteTransactionIds.push_back(LeaderTransactionId);

            Y_ENSURE(!nodes.empty());
            CreateCoreTable(*nodes.begin());

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
        const TString ClusterName;
        const TString ClusterOperationsPath;

        const ICoordinationHelper::TPtr Coordinator;

        const NProto::TDqConfig::TYtCoordinator CoordinatorConfig;

        TActorId YtWrapper;
        const TActorId CoordinatorWrapper;

        NYT::NObjectClient::TTransactionId LeaderTransactionId;

        TNodeIdAllocator NodeIdAllocator;

        struct TOperationStatus {
            TString OperationId;
            TString MutationId;
            TActorId ActorId;
            TVector<ui32> Nodes;
        };

        // mutationId -> operation
        THashMap<TString, TOperationStatus> RunningOperations;

        THashMap<TString, TVector<ui32>> MutationsCache;

        // RequestId -> Jobs
        struct TPendingStartOperation {
            TVector<ui32> Nodes;
            TString MutationId;
            THolder<TEvStartOperation> Ev;
        };
        THashMap<ui64, TPendingStartOperation> PendingStartOperationRequests;

        // RequestId -> (mutationId, nodeIds): tracks in-flight TEvRemoveNode for operation nodes in Cypress.
        // node_ids are freed only after the removal is confirmed, preventing reuse of stale IDs.
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
        Y_ABORT_UNLESS(!options.YtBackend.GetClusterName().empty());
        Y_ABORT_UNLESS(!options.YtBackend.GetUser().empty());
        Y_ABORT_UNLESS(options.YtBackend.HasMinNodeId());
        Y_ABORT_UNLESS(options.YtBackend.HasMaxNodeId());
        Y_ABORT_UNLESS(options.YtBackend.HasPrefix());
        Y_ABORT_UNLESS(!options.Files.empty());
        Y_ABORT_UNLESS(!options.UploadPrefix.empty());

        return new TYtResourceManager(options, coordinator);
    }
} // namespace NYql
