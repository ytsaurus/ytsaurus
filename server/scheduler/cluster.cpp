#include "cluster.h"
#include "node.h"
#include "pod.h"
#include "pod_set.h"
#include "internet_address.h"
#include "network_module.h"
#include "topology_zone.h"
#include "node_segment.h"
#include "account.h"
#include "helpers.h"
#include "label_filter_cache.h"
#include "private.h"

#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/transaction_manager.h>
#include <yp/server/objects/transaction.h>
#include <yp/server/objects/db_schema.h>
#include <yp/server/objects/object.h>

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yt/client/api/rowset.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/convert.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NYT::NConcurrency;
using namespace NYT::NApi;
using namespace NYT::NTableClient;
using namespace NYT::NYson;
using namespace NYT::NYTree;

using namespace NObjects;
using namespace NMaster;

////////////////////////////////////////////////////////////////////////////////

class TCluster::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }


    std::vector<TNode*> GetNodes()
    {
        std::vector<TNode*> result;
        result.reserve(NodeMap_.size());
        for (const auto& pair : NodeMap_) {
            result.push_back(pair.second.get());
        }
        return result;
    }

    TNode* FindNode(const TObjectId& id)
    {
        if (!id) {
            return nullptr;
        }
        auto it = NodeMap_.find(id);
        return it == NodeMap_.end() ? nullptr : it->second.get();
    }

    TNode* GetNodeOrThrow(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION("Node id cannot be null");
        }
        auto* node = FindNode(id);
        if (!node) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::NoSuchObject,
                "No such node %Qv",
                id);
        }
        return node;
    }


    std::vector<TNodeSegment*> GetNodeSegments()
    {
        std::vector<TNodeSegment*> result;
        result.reserve(NodeSegmentMap_.size());
        for (const auto& pair : NodeSegmentMap_) {
            result.push_back(pair.second.get());
        }
        return result;
    }

    TNodeSegment* FindNodeSegment(const TObjectId& id)
    {
        if (!id) {
            return nullptr;
        }
        auto it = NodeSegmentMap_.find(id);
        return it == NodeSegmentMap_.end() ? nullptr : it->second.get();
    }

    TNodeSegment* GetNodeSegmentOrThrow(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::InvalidObjectId,
                "Node segment id cannot be null");
        }
        auto* segment = FindNodeSegment(id);
        if (!segment) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::NoSuchObject,
                "No such node segment %Qv",
                id);
        }
        return segment;
    }


    std::vector<TPodSet*> GetPodSets()
    {
        std::vector<TPodSet*> result;
        result.reserve(PodSetMap_.size());
        for (const auto& pair : PodSetMap_) {
            result.push_back(pair.second.get());
        }
        return result;
    }

    TPodSet* FindPodSet(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION("Pod set id cannot be null");
        }
        auto it = PodSetMap_.find(id);
        return it == PodSetMap_.end() ? nullptr : it->second.get();
    }

    TPodSet* GetPodSetOrThrow(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::InvalidObjectId,
                "Pod set id cannot be null");
        }
        auto* podSet = FindPodSet(id);
        if (!podSet) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::NoSuchObject,
                "No such pod set %Qv",
                id);
        }
        return podSet;
    }


    std::vector<TPod*> GetPods()
    {
        std::vector<TPod*> result;
        result.reserve(PodMap_.size());
        for (const auto& pair : PodMap_) {
            result.push_back(pair.second.get());
        }
        return result;
    }


    TPod* FindPod(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::InvalidObjectId,
                "Pod id cannot be null");
        }
        auto it = PodMap_.find(id);
        return it == PodMap_.end() ? nullptr : it->second.get();
    }

    TPod* GetPodOrThrow(const TObjectId& id)
    {
        if (!id) {
            return nullptr;
        }
        auto* pod = FindPod(id);
        if (!pod) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::NoSuchObject,
                "No such pod %Qv",
                id);
        }
        return pod;
    }


    std::vector<TInternetAddress*> GetInternetAddresses()
    {
        std::vector<TInternetAddress*> result;
        result.reserve(InternetAddressMap_.size());
        for (const auto& pair : InternetAddressMap_) {
            result.push_back(pair.second.get());
        }
        return result;
    }


    std::vector<TAccount*> GetAccounts()
    {
        std::vector<TAccount*> result;
        result.reserve(AccountMap_.size());
        for (const auto& pair : AccountMap_) {
            result.push_back(pair.second.get());
        }
        return result;
    }

    TAccount* FindAccount(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION("Account id cannot be null");
        }
        auto it = AccountMap_.find(id);
        return it == AccountMap_.end() ? nullptr : it->second.get();
    }

    TAccount* GetAccount(const TObjectId& id)
    {
        auto* account = FindAccount(id);
        YCHECK(account);
        return account;
    }

    TAccount* GetAccountThrow(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION("Account id cannot be null");
        }
        auto* account = FindAccount(id);
        if (!account) {
            THROW_ERROR_EXCEPTION("No such account %Qv", id);
        }
        return account;
    }


    TNetworkModule* FindNetworkModule(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION("Network module id cannot be null");
        }
        auto it = NetworkModuleMap_.find(id);
        return it == NetworkModuleMap_.end() ? nullptr : it->second.get();
    }


    void LoadSnapshot()
    {
        try {
            LOG_INFO("Started loading cluster snapshot");

            PROFILE_TIMING("/cluster_snapshot/time/clear") {
                Clear();
            }

            LOG_INFO("Starting snapshot transaction");

            TTransactionPtr transaction;
            PROFILE_TIMING("/cluster_snapshot/time/start_transaction") {
                const auto& transactionManager = Bootstrap_->GetTransactionManager();
                transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
                    .ValueOrThrow();
            }

            Timestamp_ = transaction->GetStartTimestamp();

            LOG_INFO("Snapshot transaction started (Timestamp: %llx)",
                Timestamp_);

            auto* session = transaction->GetSession();

            PROFILE_TIMING("/cluster_snapshot/time/load_internet_addresses") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetInternetAddressQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing internet addresses");
                                for (auto row : rowset->GetRows()) {
                                    ParseInternetAddressFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying internet addresses");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_nodes") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetNodeQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing nodes");
                                for (auto row : rowset->GetRows()) {
                                    ParseNodeFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying nodes");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_accounts") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetAccountQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing accounts");
                                for (auto row : rowset->GetRows()) {
                                    ParseAccountFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying accounts");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_accounts_hierarchy") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetAccountHierarchyQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing accounts hierarchy");
                                for (auto row : rowset->GetRows()) {
                                    ParseAccountHierarchyFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying accounts hierarchy");
                session->FlushLoads();
            }

            const auto& objectManager = Bootstrap_->GetObjectManager();
            AllNodeSegmentsLabelFilterCache_ = std::make_unique<TLabelFilterCache<TNode>>(
                Bootstrap_->GetYTConnector(),
                objectManager->GetTypeHandler(EObjectType::Node),
                GetNodes());
            SchedulableNodeSegmentsLabelFilterCache_ = std::make_unique<TLabelFilterCache<TNode>>(
                Bootstrap_->GetYTConnector(),
                objectManager->GetTypeHandler(EObjectType::Node),
                GetSchedulableNodes());

            PROFILE_TIMING("/cluster_snapshot/time/load_node_segments") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetNodeSegmentQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing node segments");
                                for (auto row : rowset->GetRows()) {
                                    ParseNodeSegmentFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying node segments");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_pod_sets") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetPodSetQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing pod sets");
                                for (auto row : rowset->GetRows()) {
                                    ParsePodSetFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying pod sets");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_pods") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetPodQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing pods");
                                for (auto row : rowset->GetRows()) {
                                    ParsePodFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying pods");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_resources") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetResourceQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                LOG_INFO("Parsing resources");
                                for (auto row : rowset->GetRows()) {
                                    ParseResourceFromRow(row);
                                }
                            });
                    });

                LOG_INFO("Querying resources");
                session->FlushLoads();
            }

            InitializeNodePods();
            InitializePodSetPods();
            InitializeAccountPodSets();
            InitializeAntiaffinityVacancies();
            InitializeNetworkModules();

            LOG_INFO("Finished loading cluster snapshot (PodCount: %v, NodeCount: %v, NodeSegmentCount: %v)",
                PodMap_.size(),
                NodeMap_.size(),
                NodeSegmentMap_.size());
        } catch (const std::exception& ex) {
            Clear();
            THROW_ERROR_EXCEPTION("Error loading cluster snapshot")
                << ex;
        }
    }

private:
    NMaster::TBootstrap* const Bootstrap_;

    NObjects::TTimestamp Timestamp_ = NObjects::NullTimestamp;
    THashMap<TObjectId, std::unique_ptr<TNode>> NodeMap_;
    std::unique_ptr<TLabelFilterCache<TNode>> AllNodeSegmentsLabelFilterCache_;
    std::unique_ptr<TLabelFilterCache<TNode>> SchedulableNodeSegmentsLabelFilterCache_;
    THashMap<TObjectId, std::unique_ptr<TPod>> PodMap_;
    THashMap<TObjectId, std::unique_ptr<TPodSet>> PodSetMap_;
    THashMap<TObjectId, std::unique_ptr<TNodeSegment>> NodeSegmentMap_;
    THashMap<TObjectId, std::unique_ptr<TAccount>> AccountMap_;
    THashMap<TObjectId, std::unique_ptr<TInternetAddress>> InternetAddressMap_;
    THashMap<TObjectId, std::unique_ptr<TNetworkModule>> NetworkModuleMap_;

    THashMap<std::pair<TString, TString>, std::unique_ptr<TTopologyZone>> TopologyZoneMap_;
    THashMultiMap<TString, TTopologyZone*> TopologyKeyZoneMap_;


    void InitializeNodePods()
    {
        for (const auto& pair : PodMap_) {
            auto* pod = pair.second.get();
            if (pod->GetNode()) {
                YCHECK(pod->GetNode()->Pods().insert(pod).second);
            }
        }
    }

    void InitializePodSetPods()
    {
        for (const auto& pair : PodMap_) {
            auto* pod = pair.second.get();
            auto* podSet = pod->GetPodSet();
            YCHECK(podSet->Pods().insert(pod).second);
        }
    }

    void InitializeAccountPodSets()
    {
        for (const auto& pair : PodSetMap_) {
            auto* podSet = pair.second.get();
            YCHECK(podSet->GetAccount()->PodSets().insert(podSet).second);
        }
    }

    void InitializeAntiaffinityVacancies()
    {
        for (const auto& pair : PodMap_) {
            const auto* pod = pair.second.get();
            auto* node = pod->GetNode();
            if (node) {
                node->AcquireAntiaffinityVacancies(pod);
            }
        }
    }

    void InitializeNetworkModules()
    {
        for (const auto& pair : InternetAddressMap_) {
            const auto* internetAddress = pair.second.get();
            const auto& networkModuleId = internetAddress->Spec().network_module_id();
            auto* networkModule = GetOrCreateNetworkModule(networkModuleId);
            ++networkModule->InternetAddressCount();
            if (internetAddress->Status().has_pod_id()) {
                ++networkModule->AllocatedInternetAddressCount();
            }
        }
    }

    std::vector<TNode*> GetSchedulableNodes()
    {
        std::vector<TNode*> result;
        result.reserve(NodeMap_.size());
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second.get();
            if (node->IsSchedulable()) {
                result.push_back(node);
            }
        }
        return result;
    }


    TString GetResourceQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            ResourcesTable.Fields.Meta_Id.Name,
            ResourcesTable.Fields.Meta_NodeId.Name,
            ResourcesTable.Fields.Meta_Kind.Name,
            ResourcesTable.Fields.Spec.Name,
            ResourcesTable.Fields.Status_ScheduledAllocations.Name,
            ResourcesTable.Fields.Status_ActualAllocations.Name,
            ytConnector->GetTablePath(&ResourcesTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseResourceFromRow(TUnversionedRow row)
    {
        TObjectId resourceId;
        TObjectId nodeId;
        EResourceKind kind;
        NClient::NApi::NProto::TResourceSpec spec;
        std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> scheduledAllocations;
        std::vector<NClient::NApi::NProto::TResourceStatus_TAllocation> actualAllocations;
        FromUnversionedRow(
            row,
            &resourceId,
            &nodeId,
            &kind,
            &spec,
            &scheduledAllocations,
            &actualAllocations);

        auto* node = FindNode(nodeId);
        if (!node) {
            LOG_WARNING("Resource refers to an unknown node (ResourceId: %v, NodeId: %v)",
                resourceId,
                nodeId);
            return;
        }

        auto totalCapacities = GetResourceCapacities(spec);

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

        auto podIdToScheduledStatistics = aggregateAllocations(scheduledAllocations);
        auto podIdToActualStatistics = aggregateAllocations(actualAllocations);
        
        auto podIdToMaxStatistics = podIdToScheduledStatistics;
        for (const auto& pair : podIdToActualStatistics) {
            auto& current = podIdToMaxStatistics[pair.first];
            current = Max(current, pair.second);
        }

        TAllocationStatistics allocatedStatistics;
        for (const auto& pair : podIdToMaxStatistics) {
            allocatedStatistics += pair.second;
        }

        switch (kind) {
            case EResourceKind::Cpu:
                node->CpuResource() = THomogeneousResource(totalCapacities, allocatedStatistics.Capacities);
                break;
            case EResourceKind::Memory:
                node->MemoryResource() = THomogeneousResource(totalCapacities, allocatedStatistics.Capacities);
                break;
            case EResourceKind::Disk: {
                TDiskVolumePolicyList supportedPolicies;
                for (auto policy : spec.disk().supported_policies()) {
                    supportedPolicies.push_back(static_cast<NClient::NApi::NProto::EDiskVolumePolicy>(policy));
                }
                node->DiskResources().emplace_back(
                    spec.disk().storage_class(),
                    supportedPolicies,
                    totalCapacities,
                    allocatedStatistics.Used,
                    allocatedStatistics.UsedExclusively,
                    allocatedStatistics.Capacities);
                break;
            }
            default:
                Y_UNREACHABLE();
        }
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
            LOG_WARNING("Invalid %Qv label: expected %Qlv, got %Qlv (NodeId: %v)",
                topologyNode->GetPath(),
                ENodeType::Map,
                topologyNode->GetType(),
                nodeId);
            return {};
        }

        auto topologyMap = topologyNode->AsMap();
        std::vector<TTopologyZone*> zones;
        zones.reserve(topologyMap->GetChildCount());
        for (const auto& pair : topologyMap->GetChildren()) {
            const auto& key = pair.first;
            const auto& valueNode = pair.second;
            if (valueNode->GetType() != ENodeType::String) {
                LOG_WARNING("Invalid %Qv label: expected %Qlv, got %Qlv (NodeId: %v)",
                    valueNode->GetPath(),
                    ENodeType::String,
                    valueNode->GetType(),
                    nodeId);
                continue;
            }

            const auto& value = valueNode->GetValue<TString>();
            auto* zone = GetOrCreateTopologyZone(key, value);
            zones.push_back(zone);
        }
        return zones;
    }


    TString GetInternetAddressQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            InternetAddressesTable.Fields.Meta_Id.Name,
            InternetAddressesTable.Fields.Labels.Name,
            InternetAddressesTable.Fields.Spec.Name,
            InternetAddressesTable.Fields.Status.Name,
            ytConnector->GetTablePath(&InternetAddressesTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseInternetAddressFromRow(TUnversionedRow row)
    {
        TObjectId internetAddressId;
        TYsonString labels;
        NClient::NApi::NProto::TInternetAddressSpec spec;
        NClient::NApi::NProto::TInternetAddressStatus status;
        FromUnversionedRow(
            row,
            &internetAddressId,
            &labels,
            &spec,
            &status);

        auto internetAddress = std::make_unique<TInternetAddress>(
            internetAddressId,
            std::move(labels),
            std::move(spec),
            std::move(status));

        YCHECK(InternetAddressMap_.emplace(internetAddressId, std::move(internetAddress)).second);
    }


    TString GetNodeQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            NodesTable.Fields.Meta_Id.Name,
            NodesTable.Fields.Labels.Name,
            NodesTable.Fields.Status_Other.Name,
            NodesTable.Fields.Spec.Name,
            ytConnector->GetTablePath(&NodesTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseNodeFromRow(TUnversionedRow row)
    {
        TObjectId nodeId;
        TYsonString labels;
        NProto::TNodeStatusOther statusOther;
        NClient::NApi::NProto::TNodeSpec spec;
        FromUnversionedRow(
            row,
            &nodeId,
            &labels,
            &statusOther,
            &spec);

        auto labelMap = ConvertTo<IMapNodePtr>(labels);
        auto topologyZones = ParseTopologyZones(nodeId, labelMap);

        auto node = std::make_unique<TNode>(
            nodeId,
            std::move(labels),
            std::move(topologyZones),
            static_cast<EHfsmState>(statusOther.hfsm().state()),
            static_cast<ENodeMaintenanceState>(statusOther.maintenance().state()),
            statusOther.unknown_pod_ids_size(),
            std::move(spec));
        YCHECK(NodeMap_.emplace(node->GetId(), std::move(node)).second);
    }


    TString GetNodeSegmentQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v] from [%v] where is_null([%v])",
            NodeSegmentsTable.Fields.Meta_Id.Name,
            NodesTable.Fields.Labels.Name,
            NodeSegmentsTable.Fields.Spec.Name,
            ytConnector->GetTablePath(&NodeSegmentsTable),
            NodeSegmentsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseNodeSegmentFromRow(TUnversionedRow row)
    {
        TObjectId segmentId;
        TYsonString labels;
        NClient::NApi::NProto::TNodeSegmentSpec spec;
        FromUnversionedRow(
            row,
            &segmentId,
            &labels,
            &spec);

        auto allNodesOrError = AllNodeSegmentsLabelFilterCache_->GetFilteredObjects(spec.node_filter());
        auto schedulableNodesOrError = SchedulableNodeSegmentsLabelFilterCache_->GetFilteredObjects(spec.node_filter());
        if (!allNodesOrError.IsOK() || !schedulableNodesOrError.IsOK()) {
            LOG_ERROR("Invalid node segment node filter; scheduling for this segment is disabled (NodeSegmentId: %v)",
                segmentId);
            return;
        }
        const auto& allNodes = allNodesOrError.Value();
        const auto& schedulableNodes = schedulableNodesOrError.Value();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto schedulableNodeLabelFilterCache = std::make_unique<TLabelFilterCache<TNode>>(
            Bootstrap_->GetYTConnector(),
            objectManager->GetTypeHandler(EObjectType::Node),
            schedulableNodes);

        auto segment = std::make_unique<TNodeSegment>(
            segmentId,
            std::move(labels),
            std::move(allNodes),
            std::move(schedulableNodes),
            std::move(schedulableNodeLabelFilterCache));
        YCHECK(NodeSegmentMap_.emplace(segment->GetId(), std::move(segment)).second);
    }


    TString GetAccountQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v] from [%v] where is_null([%v])",
            AccountsTable.Fields.Meta_Id.Name,
            AccountsTable.Fields.Labels.Name,
            ytConnector->GetTablePath(&AccountsTable),
            AccountsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseAccountFromRow(TUnversionedRow row)
    {
        TObjectId accountId;
        TYsonString labels;
        FromUnversionedRow(
            row,
            &accountId,
            &labels);

        auto account = std::make_unique<TAccount>(
            accountId,
            std::move(labels));
        YCHECK(AccountMap_.emplace(account->GetId(), std::move(account)).second);
    }


    TString GetAccountHierarchyQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v] from [%v] where is_null([%v])",
            AccountsTable.Fields.Meta_Id.Name,
            AccountsTable.Fields.Spec_ParentId.Name,
            ytConnector->GetTablePath(&AccountsTable),
            AccountsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseAccountHierarchyFromRow(TUnversionedRow row)
    {
        TObjectId accountId;
        TObjectId parentId;
        FromUnversionedRow(
            row,
            &accountId,
            &parentId);

        auto* account = GetAccount(accountId);
        if (!parentId) {
            return;
        }

        auto* parent = FindAccount(parentId);
        if (!parent) {
            LOG_WARNING("Account refers to an unknown parent (AccountId: %v, ParentId: %v)",
                accountId,
                parentId);
            return;
        }

        account->SetParent(parent);
        YCHECK(parent->Children().insert(account).second);
    }


    TString GetPodQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v]) and [%v] = true",
            PodsTable.Fields.Meta_Id.Name,
            PodsTable.Fields.Meta_PodSetId.Name,
            PodsTable.Fields.Meta_Other.Name,
            PodsTable.Fields.Spec_NodeId.Name,
            PodsTable.Fields.Spec_Other.Name,
            PodsTable.Fields.Status_Other.Name,
            PodsTable.Fields.Labels.Name,
            ytConnector->GetTablePath(&PodsTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name,
            PodsTable.Fields.Spec_EnableScheduling.Name);
    }

    void ParsePodFromRow(TUnversionedRow row)
    {
        TObjectId podId;
        TObjectId podSetId;
        NServer::NObjects::NProto::TMetaOther metaOther;
        TObjectId nodeId;
        NServer::NObjects::NProto::TPodSpecOther specOther;
        NServer::NObjects::NProto::TPodStatusOther statusOther;
        TYsonString labels;
        FromUnversionedRow(
            row,
            &podId,
            &podSetId,
            &metaOther,
            &nodeId,
            &specOther,
            &statusOther,
            &labels);

        auto* podSet = FindPodSet(podSetId);
        if (!podSet) {
            LOG_WARNING("Pod refers to an unknown pod set (PodId: %v, PodSetId: %v)",
                podId,
                podSetId);
            return;
        }

        auto* node = FindNode(nodeId);
        if (nodeId && !node) {
            LOG_WARNING("Pod refers to an unknown node (PodId: %v, NodeId: %v)",
                podId,
                nodeId);
            return;
        }

        auto pod = std::make_unique<TPod>(
            podId,
            podSet,
            std::move(metaOther),
            node,
            std::move(specOther),
            std::move(statusOther),
            std::move(labels));
        YCHECK(PodMap_.emplace(pod->GetId(), std::move(pod)).second);
    }


    TString GetPodSetQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            PodSetsTable.Fields.Meta_Id.Name,
            PodSetsTable.Fields.Labels.Name,
            PodSetsTable.Fields.Spec_AntiaffinityConstraints.Name,
            PodSetsTable.Fields.Spec_NodeSegmentId.Name,
            PodSetsTable.Fields.Spec_AccountId.Name,
            ytConnector->GetTablePath(&PodSetsTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParsePodSetFromRow(TUnversionedRow row)
    {
        TObjectId podSetId;
        TYsonString labels;
        std::vector<NClient::NApi::NProto::TAntiaffinityConstraint> antiaffinityConstraints;
        TObjectId nodeSegmentId;
        TObjectId accountId;
        FromUnversionedRow(
            row,
            &podSetId,
            &labels,
            &antiaffinityConstraints,
            &nodeSegmentId,
            &accountId);

        auto* nodeSegment = FindNodeSegment(nodeSegmentId);
        if (!nodeSegment) {
            LOG_WARNING("Pod set refers to an unknown node segment (PodSetId: %v, NodeSegmentId: %v)",
                podSetId,
                nodeSegmentId);
            return;
        }

        auto* account = FindAccount(accountId);
        if (!account) {
            LOG_WARNING("Pod set refers to an account (PodSetId: %v, AccountId: %v)",
                podSetId,
                accountId);
            return;
        }

        auto podSet = std::make_unique<TPodSet>(
            podSetId,
            std::move(labels),
            nodeSegment,
            account,
            std::move(antiaffinityConstraints));
        YCHECK(PodSetMap_.emplace(podSet->GetId(), std::move(podSet)).second);
    }


    TNetworkModule* GetOrCreateNetworkModule(const TObjectId& id)
    {
        if (!id) {
            THROW_ERROR_EXCEPTION("Network module id cannot be null");
        }
        auto it = NetworkModuleMap_.find(id);
        if (it == NetworkModuleMap_.end()) {
            it = NetworkModuleMap_.emplace(id, std::make_unique<TNetworkModule>()).first;
        }
        return it->second.get();
    }


    void Clear()
    {
        NodeMap_.clear();
        PodMap_.clear();
        PodSetMap_.clear();
        AccountMap_.clear();
        InternetAddressMap_.clear();
        NetworkModuleMap_.clear();
        TopologyZoneMap_.clear();
        TopologyKeyZoneMap_.clear();
        NodeSegmentMap_.clear();
        AllNodeSegmentsLabelFilterCache_.reset();
        SchedulableNodeSegmentsLabelFilterCache_.reset();
        Timestamp_ = NullTimestamp;
    }
};

////////////////////////////////////////////////////////////////////////////////

TCluster::TCluster(NMaster::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
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

std::vector<TPod*> TCluster::GetPods()
{
    return Impl_->GetPods();
}

TPod* TCluster::FindPod(const TObjectId& id)
{
    return Impl_->FindPod(id);
}

TPod* TCluster::GetPodOrThrow(const TObjectId& id)
{
    return Impl_->GetPodOrThrow(id);
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

std::vector<TAccount*> TCluster::GetAccounts()
{
    return Impl_->GetAccounts();
}

TNetworkModule* TCluster::FindNetworkModule(const TObjectId& id)
{
    return Impl_->FindNetworkModule(id);
}

void TCluster::LoadSnapshot()
{
    Impl_->LoadSnapshot();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

