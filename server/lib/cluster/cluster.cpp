#include "cluster.h"

#include "account.h"
#include "allocation_statistics.h"
#include "cluster_reader.h"
#include "config.h"
#include "daemon_set.h"
#include "internet_address.h"
#include "ip4_address_pool.h"
#include "node.h"
#include "node_segment.h"
#include "object_filter_cache.h"
#include "object_filter_evaluator.h"
#include "pod.h"
#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "resource.h"
#include "resource_capacities.h"
#include "topology_zone.h"

#include <yp/server/lib/objects/object_filter.h>
#include <yp/server/lib/objects/type_info.h>

#include <yt/core/ytree/convert.h>

namespace NYP::NServer::NCluster {

using namespace NObjects;

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TCluster::TImpl
    : public TRefCounted
{
public:
    TImpl(
        NLogging::TLogger logger,
        NProfiling::TProfiler profiler,
        IClusterReaderPtr reader,
        IObjectFilterEvaluatorPtr nodeFilterEvaluator)
        : Logger(std::move(logger))
        , Profiler(std::move(profiler))
        , Reader_(std::move(reader))
        , NodeFilterEvaluator_(std::move(nodeFilterEvaluator))
    { }

    #define IMPLEMENT_EXTENDED_ACCESSORS(type, name, pluralName) \
        std::vector<T##type*> Get##pluralName() \
        { \
            std::vector<T##type*> result; \
            result.reserve(name##Map_.size()); \
            for (const auto& [id, object] : name##Map_) { \
                result.push_back(object.get()); \
            } \
            return result; \
        } \
        \
        T##type* Find##name(const TObjectId& id) \
        { \
            if (!id) { \
                return nullptr; \
            } \
            auto it = name##Map_.find(id); \
            return it == name##Map_.end() ? nullptr : it->second.get(); \
        } \
        \
        T##type* Get##name##OrThrow(const TObjectId& id) \
        { \
            if (!id) { \
                THROW_ERROR_EXCEPTION("%v id cannot be null", \
                    GetCapitalizedHumanReadableTypeName(EObjectType::type)); \
            } \
            auto* object = Find##name(id); \
            if (!object) { \
                THROW_ERROR_EXCEPTION( \
                    NClient::NApi::EErrorCode::NoSuchObject, \
                    "No such %v %Qv", \
                    GetHumanReadableTypeName(EObjectType::type), \
                    id); \
            } \
            return object; \
        }

    #define IMPLEMENT_ACCESSORS(name, pluralName) \
        IMPLEMENT_EXTENDED_ACCESSORS(name, name, pluralName)


    IMPLEMENT_ACCESSORS(Node, Nodes)
    IMPLEMENT_ACCESSORS(NodeSegment, NodeSegments)
    IMPLEMENT_ACCESSORS(PodDisruptionBudget, PodDisruptionBudgets)
    IMPLEMENT_ACCESSORS(PodSet, PodSets)
    IMPLEMENT_ACCESSORS(Pod, Pods)
    IMPLEMENT_ACCESSORS(InternetAddress, InternetAddresses)
    IMPLEMENT_ACCESSORS(IP4AddressPool, IP4AddressPools)
    IMPLEMENT_ACCESSORS(Account, Accounts)
    IMPLEMENT_ACCESSORS(Resource, Resources)

    #undef IMPLEMENT_ACCESSORS

    std::vector<TPod*> GetSchedulablePods()
    {
        auto pods = GetPods();
        pods.erase(
            std::remove_if(
                pods.begin(),
                pods.end(),
                [] (auto* pod) {
                    return !pod->GetEnableScheduling();
                }),
            pods.end());
        return pods;
    }

    TPod* FindSchedulablePod(const TObjectId& id)
    {
        auto* pod = FindPod(id);
        return !pod || !pod->GetEnableScheduling() ? nullptr : pod;
    }

    TTimestamp GetSnapshotTimestamp() const
    {
        return Timestamp_;
    }

    void LoadSnapshot(TClusterConfigPtr config)
    {
        try {
            YT_LOG_INFO("Started loading cluster snapshot");

            PROFILE_TIMING("/time/clear") {
                Clear();
            }

            Config_ = std::move(config);

            YT_LOG_INFO("Starting snapshot transaction");

            PROFILE_TIMING("/time/start_transaction") {
                Timestamp_ = Reader_->StartTransaction();
            }

            YT_LOG_INFO("Snapshot transaction started (Timestamp: %llx)",
                Timestamp_);

            PROFILE_TIMING("/time/read_ip4_address_pools") {
                Reader_->ReadIP4AddressPools(
                    [this] (std::unique_ptr<TIP4AddressPool> ip4AddressPool) {
                        RegisterObject(IP4AddressPoolMap_, std::move(ip4AddressPool));
                    });
            }

            PROFILE_TIMING("/time/read_internet_addresses") {
                Reader_->ReadInternetAddresses(
                    [this] (std::unique_ptr<TInternetAddress> internetAddress) {
                        RegisterObject(InternetAddressMap_, std::move(internetAddress));
                    });
            }

            InitializeInternetAddresses();

            PROFILE_TIMING("/time/read_nodes") {
                Reader_->ReadNodes(
                    [this] (std::unique_ptr<TNode> node) {
                        RegisterObject(NodeMap_, std::move(node));
                    });
            }

            InitializeNodeTopologyZones();

            PROFILE_TIMING("/time/read_accounts") {
                Reader_->ReadAccounts(
                    [this] (std::unique_ptr<TAccount> account) {
                        RegisterObject(AccountMap_, std::move(account));
                    });
            }

            InitializeAccountsHierarchy();

            PROFILE_TIMING("/time/read_node_segments") {
                Reader_->ReadNodeSegments(
                    [this] (std::unique_ptr<TNodeSegment> nodeSegment) {
                        RegisterObject(NodeSegmentMap_, std::move(nodeSegment));
                    });
            }

            InitializeNodeSegmentNodes();

            PROFILE_TIMING("/time/read_pod_disruption_budgets") {
                Reader_->ReadPodDisruptionBudgets(
                    [this] (std::unique_ptr<TPodDisruptionBudget> podDisruptionBudget) {
                        RegisterObject(PodDisruptionBudgetMap_, std::move(podDisruptionBudget));
                    });
            }

            PROFILE_TIMING("/time/read_pod_sets") {
                Reader_->ReadPodSets(
                    [this] (std::unique_ptr<TPodSet> podSet) {
                        RegisterObject(PodSetMap_, std::move(podSet));
                    });
            }

            InitializePodSets();

            PROFILE_TIMING("/time/read_pods") {
                Reader_->ReadPods(
                    [this] (std::unique_ptr<TPod> pod) {
                        RegisterObject(PodMap_, std::move(pod));
                    });
            }

            InitializePods();

            PROFILE_TIMING("/time/read_resources") {
                Reader_->ReadResources(
                    [this] (std::unique_ptr<TResource> resource) {
                        RegisterObject(ResourceMap_, std::move(resource));
                    });
            }

            InitializeResources();

            PROFILE_TIMING("/time/read_daemon_sets") {
                Reader_->ReadDaemonSets(
                    [this] (std::unique_ptr<TDaemonSet> daemonSet) {
                        RegisterObject(DaemonSetMap_, std::move(daemonSet));
                    });
            }

            InitializeDaemonSets();

            InitializeNodeResources();
            InitializeNodePods();
            InitializePodSetPods();
            InitializeAccountPods();
            InitializeAntiaffinityVacancies();
            InitializeIP4AddressPools();
            InitializePodIP4AddressPools();
            InitializePodSetDaemonSets();
            InitializeNodeDaemonSetPods();

            YT_LOG_INFO(
                "Finished loading cluster snapshot ("
                "NodeCount: %v, "
                "PodCount: %v, "
                "PodDisruptionBudgetCount: %v, "
                "PodSetCount: %v, "
                "NodeSegmentCount: %v, "
                "AccountCount: %v, "
                "InternetAddressCount: %v, "
                "IP4AddressPoolCount: %v, "
                "ResourceCount: %v, "
                "TopologyZoneCount: %v, "
                "DaemonSetCount: %v)",
                NodeMap_.size(),
                PodMap_.size(),
                PodDisruptionBudgetMap_.size(),
                PodSetMap_.size(),
                NodeSegmentMap_.size(),
                AccountMap_.size(),
                InternetAddressMap_.size(),
                IP4AddressPoolMap_.size(),
                ResourceMap_.size(),
                TopologyZoneMap_.size(),
                DaemonSetMap_.size());
        } catch (const std::exception& ex) {
            Clear();
            THROW_ERROR_EXCEPTION("Error loading cluster snapshot")
                << ex;
        }
    }

private:
    const NLogging::TLogger Logger;
    const NProfiling::TProfiler Profiler;
    const IClusterReaderPtr Reader_;
    const IObjectFilterEvaluatorPtr NodeFilterEvaluator_;

    TClusterConfigPtr Config_;

    NObjects::TTimestamp Timestamp_ = NObjects::NullTimestamp;
    THashMap<TObjectId, std::unique_ptr<TAccount>> AccountMap_;
    THashMap<TObjectId, std::unique_ptr<TDaemonSet>> DaemonSetMap_;
    THashMap<TObjectId, std::unique_ptr<TIP4AddressPool>> IP4AddressPoolMap_;
    THashMap<TObjectId, std::unique_ptr<TInternetAddress>> InternetAddressMap_;
    THashMap<TObjectId, std::unique_ptr<TNode>> NodeMap_;
    THashMap<TObjectId, std::unique_ptr<TNodeSegment>> NodeSegmentMap_;
    THashMap<TObjectId, std::unique_ptr<TPod>> PodMap_;
    THashMap<TObjectId, std::unique_ptr<TPodDisruptionBudget>> PodDisruptionBudgetMap_;
    THashMap<TObjectId, std::unique_ptr<TPodSet>> PodSetMap_;
    THashMap<TObjectId, std::unique_ptr<TResource>> ResourceMap_;

    THashMap<std::pair<TString, TString>, std::unique_ptr<TTopologyZone>> TopologyZoneMap_;
    THashMultiMap<TString, TTopologyZone*> TopologyKeyZoneMap_;


    template <class T>
    void RegisterObject(THashMap<TObjectId, std::unique_ptr<T>>& map, std::unique_ptr<T> object)
    {
        // NB! It is crucial to construct this argument in a separate code line
        //     to overcome UB due to unspecified order between
        //     T::GetId call and std::unique_ptr<T> move constructor.
        auto id = object->GetId();
        YT_VERIFY(map.emplace(
            std::move(id),
            std::move(object)).second);
    }

    void OnValidationError()
    {
        if (!Config_->BypassValidationErrors) {
            THROW_ERROR_EXCEPTION("Error validating cluster snapshot invariants");
        }
    }

    void InitializeDaemonSets()
    {
        std::vector<TObjectId> invalidDaemonSetIds;
        for (const auto& [daemonSetId, daemonSet] : DaemonSetMap_) {
            const auto& podSetId = daemonSet->PodSetId();
            if (!podSetId) {
                continue;
            }

            auto* podSet = FindPodSet(podSetId);
            if (!podSet) {
                YT_LOG_WARNING(
                    "Daemon set refers to an unknown pod set (DaemonSetId: %v, PodSetId: %v)",
                    daemonSetId,
                    podSetId);
                invalidDaemonSetIds.push_back(daemonSetId);
                OnValidationError();
                continue;
            }

            daemonSet->SetPodSet(podSet);
        }

        for (const auto& invalidDaemonSetId : invalidDaemonSetIds) {
            YT_VERIFY(DaemonSetMap_.erase(invalidDaemonSetId) > 0);
        }
    }

    void InitializeInternetAddresses()
    {
        std::vector<TObjectId> invalidInternetAddressIds;
        for (const auto& [internetAddressId, internetAddress] : InternetAddressMap_) {
            const auto& ip4AddressPoolId = internetAddress->ParentId();
            auto* ip4AddressPool = FindIP4AddressPool(ip4AddressPoolId);
            if (!ip4AddressPool) {
                YT_LOG_WARNING("Internet address refers to an unknown IP4 address pool (InternetAddressId: %v, IP4AddressPoolId: %v)",
                    internetAddressId,
                    ip4AddressPoolId);
                invalidInternetAddressIds.push_back(internetAddressId);
                OnValidationError();
                continue;
            }
        }
        for (const auto& invalidInternetAddressId : invalidInternetAddressIds) {
            YT_VERIFY(InternetAddressMap_.erase(invalidInternetAddressId) > 0);
        }
    }

    void InitializeNodeTopologyZones()
    {
        for (const auto& [nodeId, node] : NodeMap_) {
            node->TopologyZones() = ParseTopologyZones(nodeId, node->ParseLabels());
        }
    }

    void InitializeAccountsHierarchy()
    {
        for (const auto& [accountId, account] : AccountMap_) {
            const auto& parentId = account->ParentId();
            if (!parentId) {
                continue;
            }
            auto* parent = FindAccount(parentId);
            if (!parent) {
                YT_LOG_WARNING("Account refers to an unknown parent (AccountId: %v, ParentId: %v)",
                    account->GetId(),
                    parentId);
                OnValidationError();
                continue;
            }
            account->SetParent(parent);
            YT_VERIFY(parent->Children().insert(account.get()).second);
        }
    }

    void InitializeNodeSegmentNodes()
    {
        auto allNodeFilterCache = std::make_unique<TObjectFilterCache<TNode>>(
            NodeFilterEvaluator_,
            GetNodes());

        auto allSchedulableNodeFilterCache = std::make_unique<TObjectFilterCache<TNode>>(
            NodeFilterEvaluator_,
            GetSchedulableNodes());

        std::vector<TObjectId> invalidNodeSegmentIds;
        for (const auto& [nodeSegmentId, nodeSegment] : NodeSegmentMap_) {
            NObjects::TObjectFilter nodeSegmentFilter{nodeSegment->NodeFilter()};

            auto nodesOrError = allNodeFilterCache->Get(nodeSegmentFilter);
            auto schedulableNodesOrError = allSchedulableNodeFilterCache->Get(nodeSegmentFilter);

            bool isInvalidNodeSegment = false;
            if (!nodesOrError.IsOK()) {
                YT_LOG_ERROR(nodesOrError, "Error filtering nodes (NodeSegmentId: %v, NodeSegmentFilter: %v)",
                    nodeSegmentId,
                    nodeSegmentFilter);
                isInvalidNodeSegment = true;
            }
            if (!schedulableNodesOrError.IsOK()) {
                YT_LOG_ERROR(schedulableNodesOrError, "Error filtering schedulable nodes (NodeSegmentId: %v, NodeSegmentFilter: %v)",
                    nodeSegmentId,
                    nodeSegmentFilter);
                isInvalidNodeSegment = true;
            }
            if (isInvalidNodeSegment) {
                YT_LOG_ERROR("Invalid node segment; scheduling for this segment is disabled (NodeSegmentId: %v)",
                    nodeSegmentId);
                invalidNodeSegmentIds.push_back(nodeSegmentId);
                OnValidationError();
                continue;
            }

            auto nodeFilterCache = std::make_unique<TObjectFilterCache<TNode>>(
                NodeFilterEvaluator_,
                nodesOrError.Value());

            auto schedulableNodeFilterCache = std::make_unique<TObjectFilterCache<TNode>>(
                NodeFilterEvaluator_,
                schedulableNodesOrError.Value());

            nodeSegment->Nodes() = std::move(nodesOrError).Value();
            nodeSegment->SchedulableNodes() = std::move(schedulableNodesOrError).Value();
            nodeSegment->SetNodeFilterCache(std::move(nodeFilterCache));
            nodeSegment->SetSchedulableNodeFilterCache(std::move(schedulableNodeFilterCache));
        }
        for (const auto& invalidNodeSegmentId : invalidNodeSegmentIds) {
            YT_VERIFY(NodeSegmentMap_.erase(invalidNodeSegmentId) > 0);
        }
    }

    void InitializePodSets()
    {
        std::vector<TObjectId> invalidPodSetIds;
        for (const auto& [podSetId, podSet] : PodSetMap_) {
            const int antiaffinityConstraintsUniqueBucketCount = GetAntiaffinityConstraintsUniqueBucketCount(
                podSet->AntiaffinityConstraints());
            if (antiaffinityConstraintsUniqueBucketCount > Config_->AntiaffinityConstraintsUniqueBucketLimit) {
                YT_LOG_WARNING("Pod set count of unique antiaffinity constraints buckets exceeds limit (PodSetId: %v, Count: %v, Limit: %v)",
                    podSetId,
                    antiaffinityConstraintsUniqueBucketCount,
                    Config_->AntiaffinityConstraintsUniqueBucketLimit);
                invalidPodSetIds.push_back(podSetId);
                OnValidationError();
                continue;
            }

            const auto& nodeSegmentId = podSet->NodeSegmentId();
            auto* nodeSegment = FindNodeSegment(nodeSegmentId);
            if (!nodeSegment) {
                YT_LOG_WARNING("Pod set refers to an unknown node segment (PodSetId: %v, NodeSegmentId: %v)",
                    podSetId,
                    nodeSegmentId);
                invalidPodSetIds.push_back(podSetId);
                OnValidationError();
                continue;
            }

            const auto& accountId = podSet->AccountId();
            auto* account = FindAccount(accountId);
            if (!account) {
                YT_LOG_WARNING("Pod set refers to an unknown account (PodSetId: %v, AccountId: %v)",
                    podSetId,
                    accountId);
                invalidPodSetIds.push_back(podSetId);
                OnValidationError();
                continue;
            }

            const auto& podDisruptionBudgetId = podSet->PodDisruptionBudgetId();
            auto* podDisruptionBudget = FindPodDisruptionBudget(podDisruptionBudgetId);
            if (podDisruptionBudgetId && !podDisruptionBudget) {
                YT_LOG_WARNING("Pod set refers to an unknown pod disruption budget (PodSetId: %v, PodDisruptionBudgetId: %v)",
                    podSetId,
                    podDisruptionBudgetId);
                invalidPodSetIds.push_back(podSetId);
                OnValidationError();
                continue;
            }

            podSet->SetNodeSegment(nodeSegment);
            podSet->SetAccount(account);
            podSet->SetPodDisruptionBudget(podDisruptionBudget);
        }
        for (const auto& invalidPodSetId : invalidPodSetIds) {
            YT_VERIFY(PodSetMap_.erase(invalidPodSetId) > 0);
        }
    }

    void InitializePods()
    {
        std::vector<TObjectId> invalidPodIds;
        for (const auto& [podId, pod] : PodMap_) {
            const auto& podSetId = pod->PodSetId();
            auto* podSet = FindPodSet(podSetId);
            if (!podSet) {
                YT_LOG_WARNING("Pod refers to an unknown pod set (PodId: %v, PodSetId: %v)",
                    podId,
                    podSetId);
                invalidPodIds.push_back(podId);
                OnValidationError();
                continue;
            }

            const auto& nodeId = pod->NodeId();
            auto* node = FindNode(nodeId);
            if (nodeId && !node) {
                YT_LOG_WARNING("Pod refers to an unknown node (PodId: %v, NodeId: %v)",
                    podId,
                    nodeId);
                invalidPodIds.push_back(podId);
                OnValidationError();
                continue;
            }

            const auto& accountId = pod->AccountId();
            auto* account = FindAccount(accountId);
            if (accountId && !account) {
                YT_LOG_WARNING("Pod refers to an unknown account (PodId: %v, AccountId: %v)",
                    podId,
                    accountId);
                invalidPodIds.push_back(podId);
                OnValidationError();
                continue;
            }

            pod->SetPodSet(podSet);
            pod->SetNode(node);
            pod->SetAccount(account);

            pod->PostprocessAttributes();
        }
        for (const auto& invalidPodId : invalidPodIds) {
            YT_VERIFY(PodMap_.erase(invalidPodId) > 0);
        }
    }

    void InitializeResources()
    {
        std::vector<TObjectId> invalidResourceIds;
        for (const auto& [resourceId, resource] : ResourceMap_) {
            const auto& nodeId = resource->NodeId();
            auto* node = FindNode(nodeId);
            if (!node) {
                YT_LOG_WARNING("Resource refers to an unknown node (ResourceId: %v, NodeId: %v)",
                    resourceId,
                    nodeId);
                invalidResourceIds.push_back(resourceId);
                OnValidationError();
                continue;
            }

            resource->SetNode(node);
        }
        for (const auto& invalidResourceId : invalidResourceIds) {
            YT_VERIFY(ResourceMap_.erase(invalidResourceId) > 0);
        }
    }

    void InitializeNodeResources()
    {
        for (const auto& [resourceId, resource] : ResourceMap_) {
            auto totalCapacities = GetResourceCapacities(resource->Spec());

            auto aggregateAllocations = [&] (const auto& allocations) {
                THashMap<TStringBuf, TAllocationStatistics> podIdToStatistics;
                for (const auto& allocation : allocations) {
                    auto& statistics = podIdToStatistics[allocation.pod_id()];
                    statistics.Capacities += GetAllocationCapacities(allocation);
                    statistics.Used |= true;
                    statistics.UsedExclusively |= GetAllocationExclusive(allocation);
                }
                return podIdToStatistics;
            };

            auto podIdToScheduledStatistics = aggregateAllocations(resource->ScheduledAllocations());
            auto podIdToActualStatistics = aggregateAllocations(resource->ActualAllocations());

            auto podIdToMaxStatistics = podIdToScheduledStatistics;
            for (const auto& [podId, statistics] : podIdToActualStatistics) {
                auto& current = podIdToMaxStatistics[podId];
                current = Max(current, statistics);
            }

            TAllocationStatistics allocatedStatistics;
            for (const auto& [podId, maxStatistics] : podIdToMaxStatistics) {
                allocatedStatistics += maxStatistics;
            }

            auto* node = resource->GetNode();
            YT_VERIFY(node);

            switch (resource->GetKind()) {
                case EResourceKind::Cpu:
                    node->CpuResource() = THomogeneousResource(
                        totalCapacities,
                        allocatedStatistics.Capacities);
                    break;
                case EResourceKind::Memory:
                    node->MemoryResource() = THomogeneousResource(
                        totalCapacities,
                        allocatedStatistics.Capacities);
                    break;
                case EResourceKind::Network:
                    node->NetworkResource() = THomogeneousResource(
                        totalCapacities,
                        allocatedStatistics.Capacities);
                    break;
                case EResourceKind::Slot:
                    node->SlotResource() = THomogeneousResource(
                        totalCapacities,
                        allocatedStatistics.Capacities);
                    break;
                case EResourceKind::Disk: {
                    TDiskVolumePolicyList supportedPolicies;
                    for (auto policy : resource->Spec().disk().supported_policies()) {
                        supportedPolicies.push_back(
                            static_cast<NClient::NApi::NProto::EDiskVolumePolicy>(policy));
                    }
                    node->DiskResources().emplace_back(
                        resource->Spec().disk().storage_class(),
                        supportedPolicies,
                        totalCapacities,
                        allocatedStatistics.Used,
                        allocatedStatistics.UsedExclusively,
                        allocatedStatistics.Capacities);
                    break;
                }
                case EResourceKind::Gpu:
                    node->GpuResources().emplace_back(
                        totalCapacities,
                        allocatedStatistics.Capacities,
                        resource->Spec().gpu().model(),
                        resource->Spec().gpu().total_memory());
                    break;
                default:
                    YT_ABORT();
            }
        }
    }

    void InitializeNodePods()
    {
        for (const auto& [podId, pod] : PodMap_) {
            if (auto* node = pod->GetNode()) {
                YT_VERIFY(node->Pods().insert(pod.get()).second);
            }
        }
    }

    void InitializePodSetPods()
    {
        for (const auto& [podId, pod] : PodMap_) {
            auto* podSet = pod->GetPodSet();
            if (pod->GetEnableScheduling()) {
                YT_VERIFY(podSet->SchedulablePods().insert(pod.get()).second);
            }
        }
    }

    void InitializeAccountPods()
    {
        for (const auto& [podId, pod] : PodMap_) {
            if (pod->GetEnableScheduling()) {
                YT_VERIFY(pod->GetEffectiveAccount()->SchedulablePods().insert(pod.get()).second);
            }
        }
    }

    void InitializeAntiaffinityVacancies()
    {
        for (const auto& [podId, pod] : PodMap_) {
            auto* node = pod->GetNode();
            if (node && pod->GetEnableScheduling()) {
                // NB! Allocates vacancies regardless of the pod validation errors or node overcommit.
                node->AllocateAntiaffinityVacancies(pod.get());
            }
        }
    }

    void InitializeIP4AddressPools()
    {
        for (const auto& [internetAddressId, internetAddress] : InternetAddressMap_) {
            const auto& poolId = internetAddress->ParentId();
            auto* pool = FindIP4AddressPool(poolId);
            ++pool->InternetAddressCount();
            if (internetAddress->Status().has_pod_id()) {
                ++pool->AllocatedInternetAddressCount();
            }
        }
    }

    void InitializePodIP4AddressPools()
    {
        for (auto& [podId, pod] : PodMap_) {
            auto& ip6AddressRequests = pod->IP6AddressRequests();
            for (size_t i = 0; i < ip6AddressRequests.GetSize(); ++i) {
                const auto& request = ip6AddressRequests.ProtoRequests()[i];
                TObjectId poolId = request.ip4_address_pool_id();
                if (poolId.empty() && request.enable_internet()) {
                    poolId = DefaultIP4AddressPoolId;
                }
                if (poolId) {
                    ip6AddressRequests.SetPool(i, FindIP4AddressPool(poolId));
                }
            }
        }
    }

    void InitializePodSetDaemonSets()
    {
        for (const auto& [daemonSetId, daemonSet] : DaemonSetMap_) {
            auto* podSet = daemonSet->GetPodSet();
            if (podSet == nullptr) {
                continue;
            }
            if (podSet->GetDaemonSet() != nullptr) {
                YT_LOG_WARNING(
                    "Pod set has two daemon sets (PodSetId: %v, DaemonSetId1: %v, DaemonSetId2: %v)",
                    podSet->GetId(),
                    podSet->GetDaemonSet()->GetId(),
                    daemonSetId);
                OnValidationError();
                continue;
            }
            podSet->SetDaemonSet(daemonSet.get());
        }
    }

    void InitializeNodeDaemonSetPods()
    {
        for (const auto& [daemonSetId, daemonSet] : DaemonSetMap_) {
            auto* podSet = daemonSet->GetPodSet();
            if (podSet == nullptr) {
                continue;
            }
            auto* nodeSegment = podSet->GetNodeSegment();
            auto* cache = nodeSegment->GetNodeFilterCache();
            NObjects::TObjectFilter filter{podSet->NodeFilter()};
            auto nodesOrError = cache->Get(filter);
            if (!nodesOrError.IsOK()) {
                YT_LOG_WARNING(nodesOrError,
                    "Error filtering nodes for daemon set (DaemonSetId: %v, PodSetId: %v, "
                    "NodeSegmentId: %v, NodeSegmentFilter: %v)",
                    daemonSetId,
                    podSet->GetId(),
                    nodeSegment->GetId(),
                    podSet->NodeFilter());
                continue;
            }
            for (auto* node : nodesOrError.Value()) {
                TPod* daemonSetPod = nullptr;
                for (auto* pod : node->Pods()) {
                    if (pod->GetPodSet() == podSet) {
                        daemonSetPod = pod;
                        break;
                    }
                }
                node->DaemonSetPods()[daemonSet.get()] = daemonSetPod;
            }
        }
    }

    std::vector<TNode*> GetSchedulableNodes()
    {
        std::vector<TNode*> result;
        result.reserve(NodeMap_.size());
        for (const auto& [nodeId, node] : NodeMap_) {
            if (node->IsSchedulable()) {
                result.push_back(node.get());
            }
        }
        return result;
    }


    TTopologyZone* GetOrCreateTopologyZone(const TString& key, const TString& value)
    {
        auto pair = std::make_pair(key, value);
        auto it = TopologyZoneMap_.find(pair);
        if (it == TopologyZoneMap_.end()) {
            auto zone = std::make_unique<TTopologyZone>(key, value);
            TopologyKeyZoneMap_.emplace(key, zone.get());
            it = TopologyZoneMap_.emplace(pair, std::move(zone)).first;
        }
        return it->second.get();
    }

    std::vector<TTopologyZone*> ParseTopologyZones(const TObjectId& nodeId, const IMapNodePtr& labelMap)
    {
        auto topologyNode = labelMap->FindChild(TopologyLabel);
        if (!topologyNode) {
            return {};
        }

        if (topologyNode->GetType() != ENodeType::Map) {
            YT_LOG_WARNING("Invalid %Qv label: expected %Qlv, got %Qlv (NodeId: %v)",
                topologyNode->GetPath(),
                ENodeType::Map,
                topologyNode->GetType(),
                nodeId);
            OnValidationError();
            return {};
        }

        auto topologyMap = topologyNode->AsMap();
        std::vector<TTopologyZone*> zones;
        zones.reserve(topologyMap->GetChildCount());
        for (const auto& [key, valueNode] : topologyMap->GetChildren()) {
            if (valueNode->GetType() != ENodeType::String) {
                YT_LOG_WARNING("Invalid %Qv label: expected %Qlv, got %Qlv (NodeId: %v)",
                    valueNode->GetPath(),
                    ENodeType::String,
                    valueNode->GetType(),
                    nodeId);
                OnValidationError();
                continue;
            }

            const auto& value = valueNode->GetValue<TString>();
            auto* zone = GetOrCreateTopologyZone(key, value);
            zones.push_back(zone);
        }
        return zones;
    }

    int GetAntiaffinityConstraintsUniqueBucketCount(const std::vector<NClient::NApi::NProto::TAntiaffinityConstraint>& constraints)
    {
        using TAntiaffinityConstraintsBucket = std::tuple<TString, TString>;

        THashSet<TAntiaffinityConstraintsBucket> buckets;
        for (const auto& constraint : constraints) {
            buckets.insert(std::make_tuple(constraint.key(), constraint.pod_group_id_path()));
        }

        return buckets.size();
    }


    void Clear()
    {
        AccountMap_.clear();
        DaemonSetMap_.clear();
        IP4AddressPoolMap_.clear();
        InternetAddressMap_.clear();
        NodeMap_.clear();
        NodeSegmentMap_.clear();
        PodDisruptionBudgetMap_.clear();
        PodMap_.clear();
        PodSetMap_.clear();
        ResourceMap_.clear();
        TopologyKeyZoneMap_.clear();
        TopologyZoneMap_.clear();
        Timestamp_ = NullTimestamp;
        Config_.Reset();
    }
};

////////////////////////////////////////////////////////////////////////////////

TCluster::TCluster(
    NLogging::TLogger logger,
    NProfiling::TProfiler profiler,
    IClusterReaderPtr reader,
    IObjectFilterEvaluatorPtr nodeFilterEvaluator)
    : Impl_(New<TImpl>(
        std::move(logger),
        std::move(profiler),
        std::move(reader),
        std::move(nodeFilterEvaluator)))
{ }

TCluster::~TCluster()
{ }

std::vector<TNode*> TCluster::GetNodes()
{
    return Impl_->GetNodes();
}

TNode* TCluster::FindNode(const TObjectId& id)
{
    return Impl_->FindNode(id);
}

TNode* TCluster::GetNodeOrThrow(const TObjectId& id)
{
    return Impl_->GetNodeOrThrow(id);
}

std::vector<TResource*> TCluster::GetResources()
{
    return Impl_->GetResources();
}

TResource* TCluster::FindResource(const TObjectId& id)
{
    return Impl_->FindResource(id);
}

TResource* TCluster::GetResourceOrThrow(const TObjectId& id)
{
    return Impl_->GetResourceOrThrow(id);
}

std::vector<TPod*> TCluster::GetSchedulablePods()
{
    return Impl_->GetSchedulablePods();
}

TPod* TCluster::FindSchedulablePod(const TObjectId& id)
{
    return Impl_->FindSchedulablePod(id);
}

std::vector<TPod*> TCluster::GetPods()
{
    return Impl_->GetPods();
}

TPod* TCluster::FindPod(const TObjectId& id)
{
    return Impl_->FindPod(id);
}

std::vector<TNodeSegment*> TCluster::GetNodeSegments()
{
    return Impl_->GetNodeSegments();
}

TNodeSegment* TCluster::FindNodeSegment(const TObjectId& id)
{
    return Impl_->FindNodeSegment(id);
}

TNodeSegment* TCluster::GetNodeSegmentOrThrow(const TObjectId& id)
{
    return Impl_->GetNodeSegmentOrThrow(id);
}

std::vector<TInternetAddress*> TCluster::GetInternetAddresses()
{
    return Impl_->GetInternetAddresses();
}

std::vector<TIP4AddressPool*> TCluster::GetIP4AddressPools()
{
    return Impl_->GetIP4AddressPools();
}

std::vector<TAccount*> TCluster::GetAccounts()
{
    return Impl_->GetAccounts();
}

TIP4AddressPool* TCluster::FindIP4AddressPool(const TObjectId& id)
{
    return Impl_->FindIP4AddressPool(id);
}

std::vector<TPodSet*> TCluster::GetPodSets()
{
    return Impl_->GetPodSets();
}

std::vector<TPodDisruptionBudget*> TCluster::GetPodDisruptionBudgets()
{
    return Impl_->GetPodDisruptionBudgets();
}

TTimestamp TCluster::GetSnapshotTimestamp() const
{
    return Impl_->GetSnapshotTimestamp();
}

void TCluster::LoadSnapshot(TClusterConfigPtr config)
{
    Impl_->LoadSnapshot(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
