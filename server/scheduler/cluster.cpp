#include "cluster.h"
#include "node.h"
#include "pod.h"
#include "pod_disruption_budget.h"
#include "pod_set.h"
#include "resource.h"
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
#include <yp/server/objects/helpers.h>
#include <yp/server/objects/type_info.h>

#include <yp/server/objects/proto/autogen.pb.h>
#include <yp/server/objects/proto/objects.pb.h>

#include <yp/server/master/bootstrap.h>
#include <yp/server/master/yt_connector.h>

#include <yt/client/api/rowset.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/convert.h>

namespace NYP::NServer::NScheduler {

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

    #define IMPLEMENT_ACCESSORS(name, pluralName) \
        std::vector<T##name*> Get##pluralName() \
        { \
            std::vector<T##name*> result; \
            result.reserve(name##Map_.size()); \
            for (const auto& [id, object] : name##Map_) { \
                result.push_back(object.get()); \
            } \
            return result; \
        } \
        \
        T##name* Find##name(const TObjectId& id) \
        { \
            if (!id) { \
                return nullptr; \
            } \
            auto it = name##Map_.find(id); \
            return it == name##Map_.end() ? nullptr : it->second.get(); \
        } \
        \
        T##name* Get##name##OrThrow(const TObjectId& id) \
        { \
            if (!id) { \
                THROW_ERROR_EXCEPTION("%v id cannot be null", \
                    GetCapitalizedHumanReadableTypeName(EObjectType::name)); \
            } \
            auto* object = Find##name(id); \
            if (!object) { \
                THROW_ERROR_EXCEPTION( \
                    NClient::NApi::EErrorCode::NoSuchObject, \
                    "No such %v %Qv", \
                    GetHumanReadableTypeName(EObjectType::name), \
                    id); \
            } \
            return object; \
        }


    IMPLEMENT_ACCESSORS(Node, Nodes)
    IMPLEMENT_ACCESSORS(NodeSegment, NodeSegments)
    IMPLEMENT_ACCESSORS(PodDisruptionBudget, PodDisruptionBudgets)
    IMPLEMENT_ACCESSORS(PodSet, PodSets)
    IMPLEMENT_ACCESSORS(Pod, Pods)
    IMPLEMENT_ACCESSORS(InternetAddress, InternetAddresses)
    IMPLEMENT_ACCESSORS(Account, Accounts)
    IMPLEMENT_ACCESSORS(NetworkModule, NetworkModules)
    IMPLEMENT_ACCESSORS(Resource, Resources)

    #undef IMPLEMENT_ACCESSORS

    TTimestamp GetSnapshotTimestamp() const
    {
        return Timestamp_;
    }

    void LoadSnapshot()
    {
        try {
            YT_LOG_INFO("Started loading cluster snapshot");

            PROFILE_TIMING("/cluster_snapshot/time/clear") {
                Clear();
            }

            YT_LOG_INFO("Starting snapshot transaction");

            TTransactionPtr transaction;
            PROFILE_TIMING("/cluster_snapshot/time/start_transaction") {
                const auto& transactionManager = Bootstrap_->GetTransactionManager();
                transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
                    .ValueOrThrow();
            }

            Timestamp_ = transaction->GetStartTimestamp();

            YT_LOG_INFO("Snapshot transaction started (Timestamp: %llx)",
                Timestamp_);

            auto* session = transaction->GetSession();

            PROFILE_TIMING("/cluster_snapshot/time/load_internet_addresses") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetInternetAddressQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing internet addresses");
                                for (auto row : rowset->GetRows()) {
                                    ParseInternetAddressFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying internet addresses");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_nodes") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetNodeQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing nodes");
                                for (auto row : rowset->GetRows()) {
                                    ParseNodeFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying nodes");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_accounts") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetAccountQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing accounts");
                                for (auto row : rowset->GetRows()) {
                                    ParseAccountFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying accounts");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_accounts_hierarchy") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetAccountHierarchyQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing accounts hierarchy");
                                for (auto row : rowset->GetRows()) {
                                    ParseAccountHierarchyFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying accounts hierarchy");
                session->FlushLoads();
            }

            const auto& objectManager = Bootstrap_->GetObjectManager();
            AllNodeSegmentsLabelFilterCache_ = std::make_unique<TLabelFilterCache<TNode>>(
                objectManager->GetTypeHandler(EObjectType::Node),
                GetNodes());
            SchedulableNodeSegmentsLabelFilterCache_ = std::make_unique<TLabelFilterCache<TNode>>(
                objectManager->GetTypeHandler(EObjectType::Node),
                GetSchedulableNodes());

            PROFILE_TIMING("/cluster_snapshot/time/load_node_segments") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetNodeSegmentQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing node segments");
                                for (auto row : rowset->GetRows()) {
                                    ParseNodeSegmentFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying node segments");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_pod_disruption_budgets") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetPodDisruptionBudgetQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing pod disruption budgets");
                                for (auto row : rowset->GetRows()) {
                                    ParsePodDisruptionBudgetFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying pod disruption budgets");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_pod_sets") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetPodSetQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing pod sets");
                                for (auto row : rowset->GetRows()) {
                                    ParsePodSetFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying pod sets");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_pods") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetPodQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing pods");
                                for (auto row : rowset->GetRows()) {
                                    ParsePodFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying pods");
                session->FlushLoads();
            }

            PROFILE_TIMING("/cluster_snapshot/time/load_resources") {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetResourceQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                YT_LOG_INFO("Parsing resources");
                                for (auto row : rowset->GetRows()) {
                                    ParseResourceFromRow(row);
                                }
                            });
                    });

                YT_LOG_INFO("Querying resources");
                session->FlushLoads();
            }

            InitializeNodePods();
            InitializePodSetPods();
            InitializeAccountPods();
            InitializeAntiaffinityVacancies();
            InitializeNetworkModules();

            YT_LOG_INFO("Finished loading cluster snapshot (PodCount: %v, NodeCount: %v, NodeSegmentCount: %v)",
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
    THashMap<TObjectId, std::unique_ptr<TPodDisruptionBudget>> PodDisruptionBudgetMap_;
    THashMap<TObjectId, std::unique_ptr<TPodSet>> PodSetMap_;
    THashMap<TObjectId, std::unique_ptr<TNodeSegment>> NodeSegmentMap_;
    THashMap<TObjectId, std::unique_ptr<TAccount>> AccountMap_;
    THashMap<TObjectId, std::unique_ptr<TInternetAddress>> InternetAddressMap_;
    THashMap<TObjectId, std::unique_ptr<TNetworkModule>> NetworkModuleMap_;
    THashMap<TObjectId, std::unique_ptr<TResource>> ResourceMap_;

    THashMap<std::pair<TString, TString>, std::unique_ptr<TTopologyZone>> TopologyZoneMap_;
    THashMultiMap<TString, TTopologyZone*> TopologyKeyZoneMap_;

    void InitializeNodePods()
    {
        for (const auto& [podId, pod] : PodMap_) {
            if (pod->GetNode()) {
                YT_VERIFY(pod->GetNode()->Pods().insert(pod.get()).second);
            }
        }
    }

    void InitializePodSetPods()
    {
        for (const auto& [podId, pod] : PodMap_) {
            auto* podSet = pod->GetPodSet();
            YT_VERIFY(podSet->Pods().insert(pod.get()).second);
        }
    }

    void InitializeAccountPods()
    {
        for (const auto& [podId, pod] : PodMap_) {
            YT_VERIFY(pod->GetEffectiveAccount()->Pods().insert(pod.get()).second);
        }
    }

    void InitializeAntiaffinityVacancies()
    {
        for (const auto& [podId, pod] : PodMap_) {
            auto* node = pod->GetNode();
            if (node) {
                node->AcquireAntiaffinityVacancies(pod.get());
            }
        }
    }

    void InitializeNetworkModules()
    {
        for (const auto& [internetAddressId, internetAddress] : InternetAddressMap_) {
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
        for (const auto& [nodeId, node] : NodeMap_) {
            if (node->IsSchedulable()) {
                result.push_back(node.get());
            }
        }
        return result;
    }


    TString GetResourceQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            ResourcesTable.Fields.Meta_Id.Name,
            ResourcesTable.Fields.Meta_NodeId.Name,
            ResourcesTable.Fields.Meta_Kind.Name,
            ResourcesTable.Fields.Spec.Name,
            ResourcesTable.Fields.Status_ScheduledAllocations.Name,
            ResourcesTable.Fields.Status_ActualAllocations.Name,
            ResourcesTable.Fields.Labels.Name,
            ytConnector->GetTablePath(&ResourcesTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseResourceFromRow(TUnversionedRow row)
    {
        TObjectId resourceId;
        TObjectId nodeId;
        EResourceKind kind;
        TYsonString labels;
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
            &actualAllocations,
            &labels);

        auto* node = FindNode(nodeId);
        if (!node) {
            YT_LOG_WARNING("Resource refers to an unknown node (ResourceId: %v, NodeId: %v)",
                resourceId,
                nodeId);
            return;
        }

        auto resource = std::make_unique<TResource>(
            resourceId,
            std::move(labels),
            node,
            kind,
            std::move(spec),
            std::move(scheduledAllocations),
            std::move(actualAllocations));

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
        for (const auto& [podId, scheduledStatistics] : podIdToActualStatistics) {
            auto& current = podIdToMaxStatistics[podId];
            current = Max(current, scheduledStatistics);
        }

        TAllocationStatistics allocatedStatistics;
        for (const auto& [podId, maxStatistics] : podIdToMaxStatistics) {
            allocatedStatistics += maxStatistics;
        }

        switch (kind) {
            case EResourceKind::Cpu:
                node->CpuResource() = THomogeneousResource(totalCapacities, allocatedStatistics.Capacities);
                break;
            case EResourceKind::Memory:
                node->MemoryResource() = THomogeneousResource(totalCapacities, allocatedStatistics.Capacities);
                break;
            case EResourceKind::Slot:
                node->SlotResource() = THomogeneousResource(totalCapacities, allocatedStatistics.Capacities);
                break;
            case EResourceKind::Disk: {
                TDiskVolumePolicyList supportedPolicies;
                for (auto policy : resource->Spec().disk().supported_policies()) {
                    supportedPolicies.push_back(static_cast<NClient::NApi::NProto::EDiskVolumePolicy>(policy));
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
            default:
                YT_ABORT();
        }
        YT_VERIFY(ResourceMap_.emplace(resourceId, std::move(resource)).second);
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

        YT_VERIFY(InternetAddressMap_.emplace(internetAddressId, std::move(internetAddress)).second);
    }


    TString GetNodeQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            NodesTable.Fields.Meta_Id.Name,
            NodesTable.Fields.Labels.Name,
            NodesTable.Fields.Status_Etc.Name,
            NodesTable.Fields.Spec.Name,
            ytConnector->GetTablePath(&NodesTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseNodeFromRow(TUnversionedRow row)
    {
        TObjectId nodeId;
        TYsonString labels;
        NProto::TNodeStatusEtc statusEtc;
        NClient::NApi::NProto::TNodeSpec spec;
        FromUnversionedRow(
            row,
            &nodeId,
            &labels,
            &statusEtc,
            &spec);

        auto labelMap = ConvertTo<IMapNodePtr>(labels);
        auto topologyZones = ParseTopologyZones(nodeId, labelMap);

        auto node = std::make_unique<TNode>(
            nodeId,
            std::move(labels),
            std::move(topologyZones),
            static_cast<EHfsmState>(statusEtc.hfsm().state()),
            static_cast<ENodeMaintenanceState>(statusEtc.maintenance().state()),
            statusEtc.unknown_pod_ids_size(),
            std::move(spec));
        YT_VERIFY(NodeMap_.emplace(node->GetId(), std::move(node)).second);
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
            YT_LOG_ERROR("Invalid node segment node filter; scheduling for this segment is disabled (NodeSegmentId: %v)",
                segmentId);
            return;
        }
        const auto& allNodes = allNodesOrError.Value();
        const auto& schedulableNodes = schedulableNodesOrError.Value();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto schedulableNodeLabelFilterCache = std::make_unique<TLabelFilterCache<TNode>>(
            objectManager->GetTypeHandler(EObjectType::Node),
            schedulableNodes);

        auto segment = std::make_unique<TNodeSegment>(
            segmentId,
            std::move(labels),
            std::move(allNodes),
            std::move(schedulableNodes),
            std::move(schedulableNodeLabelFilterCache));
        YT_VERIFY(NodeSegmentMap_.emplace(segment->GetId(), std::move(segment)).second);
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
        YT_VERIFY(AccountMap_.emplace(account->GetId(), std::move(account)).second);
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

        if (!parentId) {
            return;
        }

        auto* account = GetAccountOrThrow(accountId);
        auto* parent = FindAccount(parentId);
        if (!parent) {
            YT_LOG_WARNING("Account refers to an unknown parent (AccountId: %v, ParentId: %v)",
                accountId,
                parentId);
            return;
        }

        account->SetParent(parent);
        YT_VERIFY(parent->Children().insert(account).second);
    }


    TString GetPodDisruptionBudgetQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            PodDisruptionBudgetsTable.Fields.Meta_Id.Name,
            PodDisruptionBudgetsTable.Fields.Labels.Name,
            PodDisruptionBudgetsTable.Fields.Meta_Etc.Name,
            PodDisruptionBudgetsTable.Fields.Spec.Name,
            ytConnector->GetTablePath(&PodDisruptionBudgetsTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParsePodDisruptionBudgetFromRow(TUnversionedRow row)
    {
        TObjectId id;
        TYsonString labels;
        NServer::NObjects::NProto::TMetaEtc metaEtc;
        NClient::NApi::NProto::TPodDisruptionBudgetSpec spec;
        FromUnversionedRow(
            row,
            &id,
            &labels,
            &metaEtc,
            &spec);

        auto podDisruptionBudget = std::make_unique<TPodDisruptionBudget>(
            id,
            std::move(labels),
            std::move(*metaEtc.mutable_uuid()),
            std::move(spec));
        YT_VERIFY(PodDisruptionBudgetMap_.emplace(
            podDisruptionBudget->GetId(),
            std::move(podDisruptionBudget)).second);
    }

    TString GetPodQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v]) and [%v] = true",
            PodsTable.Fields.Meta_Id.Name,
            PodsTable.Fields.Meta_PodSetId.Name,
            PodsTable.Fields.Meta_Etc.Name,
            PodsTable.Fields.Spec_NodeId.Name,
            PodsTable.Fields.Spec_Etc.Name,
            PodsTable.Fields.Spec_AccountId.Name,
            PodsTable.Fields.Status_Etc.Name,
            PodsTable.Fields.Labels.Name,
            ytConnector->GetTablePath(&PodsTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name,
            PodsTable.Fields.Spec_EnableScheduling.Name);
    }

    void ParsePodFromRow(TUnversionedRow row)
    {
        TObjectId podId;
        TObjectId podSetId;
        NServer::NObjects::NProto::TMetaEtc metaEtc;
        TObjectId nodeId;
        NServer::NObjects::NProto::TPodSpecEtc specEtc;
        TObjectId accountId;
        NServer::NObjects::NProto::TPodStatusEtc statusEtc;
        TYsonString labels;
        FromUnversionedRow(
            row,
            &podId,
            &podSetId,
            &metaEtc,
            &nodeId,
            &specEtc,
            &accountId,
            &statusEtc,
            &labels);

        auto* podSet = FindPodSet(podSetId);
        if (!podSet) {
            YT_LOG_WARNING("Pod refers to an unknown pod set (PodId: %v, PodSetId: %v)",
                podId,
                podSetId);
            return;
        }

        auto* node = FindNode(nodeId);
        if (nodeId && !node) {
            YT_LOG_WARNING("Pod refers to an unknown node (PodId: %v, NodeId: %v)",
                podId,
                nodeId);
            return;
        }

        auto* account = FindAccount(accountId);
        if (accountId && !account) {
            YT_LOG_WARNING("Pod refers to an account node (PodId: %v, AccountId: %v)",
                podId,
                accountId);
            return;
        }

        auto pod = std::make_unique<TPod>(
            podId,
            std::move(labels),
            podSet,
            node,
            account,
            std::move(*metaEtc.mutable_uuid()),
            std::move(*specEtc.mutable_resource_requests()),
            // NB! Pass some arguments by const reference due to lack of move semantics support.
            specEtc.disk_volume_requests(),
            specEtc.ip6_address_requests(),
            specEtc.ip6_subnet_requests(),
            std::move(*specEtc.mutable_node_filter()),
            std::move(*statusEtc.mutable_eviction()));
        YT_VERIFY(PodMap_.emplace(pod->GetId(), std::move(pod)).second);
    }


    TString GetPodSetQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            PodSetsTable.Fields.Meta_Id.Name,
            PodSetsTable.Fields.Labels.Name,
            PodSetsTable.Fields.Spec_AntiaffinityConstraints.Name,
            PodSetsTable.Fields.Spec_NodeSegmentId.Name,
            PodSetsTable.Fields.Spec_AccountId.Name,
            PodSetsTable.Fields.Spec_NodeFilter.Name,
            PodSetsTable.Fields.Spec_PodDisruptionBudgetId.Name,
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
        TString nodeFilter;
        TObjectId podDisruptionBudgetId;
        FromUnversionedRow(
            row,
            &podSetId,
            &labels,
            &antiaffinityConstraints,
            &nodeSegmentId,
            &accountId,
            &nodeFilter,
            &podDisruptionBudgetId);

        auto* nodeSegment = FindNodeSegment(nodeSegmentId);
        if (!nodeSegment) {
            YT_LOG_WARNING("Pod set refers to an unknown node segment (PodSetId: %v, NodeSegmentId: %v)",
                podSetId,
                nodeSegmentId);
            return;
        }

        auto* account = FindAccount(accountId);
        if (!account) {
            YT_LOG_WARNING("Pod set refers to an unknown account (PodSetId: %v, AccountId: %v)",
                podSetId,
                accountId);
            return;
        }

        auto* podDisruptionBudget = FindPodDisruptionBudget(podDisruptionBudgetId);
        if (podDisruptionBudgetId && !podDisruptionBudget) {
            YT_LOG_WARNING("Pod set refers to an unknown pod disruption budget (PodSetId: %v, PodDisruptionBudgetId: %v)",
                podSetId,
                podDisruptionBudgetId);
        }

        auto podSet = std::make_unique<TPodSet>(
            podSetId,
            std::move(labels),
            nodeSegment,
            account,
            podDisruptionBudget,
            std::move(antiaffinityConstraints),
            std::move(nodeFilter));
        YT_VERIFY(PodSetMap_.emplace(podSet->GetId(), std::move(podSet)).second);
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
        PodDisruptionBudgetMap_.clear();
        PodSetMap_.clear();
        AccountMap_.clear();
        InternetAddressMap_.clear();
        NetworkModuleMap_.clear();
        TopologyZoneMap_.clear();
        TopologyKeyZoneMap_.clear();
        NodeSegmentMap_.clear();
        ResourceMap_.clear();
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

void TCluster::LoadSnapshot()
{
    Impl_->LoadSnapshot();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler

