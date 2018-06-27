#include "cluster.h"
#include "node.h"
#include "pod.h"
#include "pod_set.h"
#include "topology_zone.h"
#include "node_segment.h"
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

#include <yt/ytlib/api/rowset.h>

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

    int GetNodeCount()
    {
        return static_cast<int>(NodeMap_.size());
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
                "No such node segment %Qv", id);
        }
        return segment;
    }

    int GetNodeSegmentCount()
    {
        return static_cast<int>(NodeSegmentMap_.size());
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
            THROW_ERROR_EXCEPTION("Pod set id cannot be null");
        }
        auto* podSet = FindPodSet(id);
        if (!podSet) {
            THROW_ERROR_EXCEPTION("No such pod set %Qv", id);
        }
        return podSet;
    }

    int GetPodSetCount()
    {
        return static_cast<int>(PodSetMap_.size());
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
            THROW_ERROR_EXCEPTION("Pod id cannot be null");
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
            THROW_ERROR_EXCEPTION("No such pod %Qv", id);
        }
        return pod;
    }

    int GetPodCount()
    {
        return static_cast<int>(PodMap_.size());
    }


    void LoadSnapshot()
    {
        try {
            LOG_DEBUG("Started loading cluster snapshot");

            Clear();

            LOG_DEBUG("Starting snapshot transaction");

            const auto& transactionManager = Bootstrap_->GetTransactionManager();
            auto transaction = WaitFor(transactionManager->StartReadOnlyTransaction())
                .ValueOrThrow();

            Timestamp_ = transaction->GetStartTimestamp();

            LOG_DEBUG("Snapshot transaction started (Timestamp: %llx)",
                Timestamp_);

            auto* session = transaction->GetSession();

            {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetNodeQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                LOG_DEBUG("Parsing nodes");
                                for (auto row : rowset->GetRows()) {
                                    ParseNodeFromRow(row);
                                }
                            });
                    });

                LOG_DEBUG("Querying nodes");
                session->FlushLoads();
            }

            const auto& objectManager = Bootstrap_->GetObjectManager();
            NodeSegmentsLabelFilterCache_ = std::make_unique<TLabelFilterCache<TNode>>(
                Bootstrap_->GetYTConnector(),
                objectManager->GetTypeHandler(EObjectType::Node),
                GetSchedulableNodes());

            {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetNodeSegmentQueryString(),
                            [&](const IUnversionedRowsetPtr& rowset) {
                                LOG_DEBUG("Parsing node segments");
                                for (auto row : rowset->GetRows()) {
                                    ParseNodeSegmentFromRow(row);
                                }
                            });
                    });

                LOG_DEBUG("Querying node segments");
                session->FlushLoads();
            }

            {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetPodSetQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                LOG_DEBUG("Parsing pod sets");
                                for (auto row : rowset->GetRows()) {
                                    ParsePodSetFromRow(row);
                                }
                            });
                    });

                LOG_DEBUG("Querying pod sets");
                session->FlushLoads();
            }

            {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetPodQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                LOG_DEBUG("Parsing pods");
                                for (auto row : rowset->GetRows()) {
                                    ParsePodFromRow(row);
                                }
                            });
                    });

                LOG_DEBUG("Querying pods");
                session->FlushLoads();
            }

            {
                session->ScheduleLoad(
                    [&] (ILoadContext* context) {
                        context->ScheduleSelect(
                            GetResourceQueryString(),
                            [&] (const IUnversionedRowsetPtr& rowset) {
                                LOG_DEBUG("Parsing resources");
                                for (auto row : rowset->GetRows()) {
                                    ParseResourceFromRow(row);
                                }
                            });
                    });

                LOG_DEBUG("Querying resources");
                session->FlushLoads();
            }

            InitNodePods();
            InitializeAntiaffinityVacancies();

            LOG_DEBUG("Finished loading cluster snapshot (PodCount: %v, NodeCount: %v)",
                GetPodCount(),
                GetNodeCount());
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
    std::unique_ptr<TLabelFilterCache<TNode>> NodeSegmentsLabelFilterCache_;
    THashMap<TObjectId, std::unique_ptr<TPod>> PodMap_;
    THashMap<TObjectId, std::unique_ptr<TPodSet>> PodSetMap_;
    THashMap<TObjectId, std::unique_ptr<TNodeSegment>> NodeSegmentMap_;

    THashMap<std::pair<TString, TString>, std::unique_ptr<TTopologyZone>> TopologyZoneMap_;
    THashMultiMap<TString, TTopologyZone*> TopologyKeyZoneMap_;


    void InitNodePods()
    {
        for (const auto& pair : PodMap_) {
            auto* pod = pair.second.get();
            if (pod->GetNode()) {
                YCHECK(pod->GetNode()->Pods().insert(pod).second);
            }
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

    std::vector<TNode*> GetSchedulableNodes()
    {
        std::vector<TNode*> result;
        result.reserve(NodeMap_.size());
        for (const auto& pair : NodeMap_) {
            auto* node = pair.second.get();
            if (node->GetHfsmState() == EHfsmState::Up) {
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
        FromDBRow(
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
            TAllocationStatistics statistics;
            for (const auto& allocation : allocations) {
                statistics.Capacities += GetAllocationCapacities(allocation);
                statistics.Used |= true;
                statistics.UsedExclusively |= GetAllocationExclusive(allocation);
            }
            return statistics;
        };

        auto scheduledStatistics = aggregateAllocations(scheduledAllocations);
        auto actualStatistics = aggregateAllocations(actualAllocations);
        auto maxStatistics = Max(scheduledStatistics, actualStatistics);
        switch (kind) {
            case EResourceKind::Cpu:
                node->CpuResource() = THomogeneousResource(totalCapacities, maxStatistics.Capacities);
                break;
            case EResourceKind::Memory:
                node->MemoryResource() = THomogeneousResource(totalCapacities, maxStatistics.Capacities);
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
                    maxStatistics.Used,
                    maxStatistics.UsedExclusively,
                    maxStatistics.Capacities);
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


    TString GetNodeQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v] from [%v] where is_null([%v])",
            NodesTable.Fields.Meta_Id.Name,
            NodesTable.Fields.Labels.Name,
            NodesTable.Fields.Status_Other.Name,
            ytConnector->GetTablePath(&NodesTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParseNodeFromRow(TUnversionedRow row)
    {
        TObjectId nodeId;
        TYsonString labels;
        NProto::TNodeStatusOther statusOther;
        FromDBRow(
            row,
            &nodeId,
            &labels,
            &statusOther);

        auto labelMap = ConvertTo<IMapNodePtr>(labels);
        auto topologyZones = ParseTopologyZones(nodeId, labelMap);

        auto node = std::make_unique<TNode>(
            nodeId,
            std::move(labels),
            std::move(topologyZones),
            static_cast<EHfsmState>(statusOther.hfsm().state()),
            static_cast<ENodeMaintenanceState>(statusOther.maintenance().state()));
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
        FromDBRow(
            row,
            &segmentId,
            &labels,
            &spec);

        auto nodesOrError = NodeSegmentsLabelFilterCache_->GetFilteredObjects(spec.node_filter());
        if (!nodesOrError.IsOK()) {
            LOG_ERROR("Invalid node segment node filter; scheduling for this segment is disabled (NodeSegmentId: %v)",
                segmentId);
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto nodeLabelFilterCache = std::make_unique<TLabelFilterCache<TNode>>(
            Bootstrap_->GetYTConnector(),
            objectManager->GetTypeHandler(EObjectType::Node),
            nodesOrError.Value());

        auto segment = std::make_unique<TNodeSegment>(
            segmentId,
            std::move(labels),
            std::move(nodeLabelFilterCache));
        YCHECK(NodeSegmentMap_.emplace(segment->GetId(), std::move(segment)).second);
    }


    TString GetPodQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v], [%v], [%v] from [%v] where is_null([%v]) and [%v] = true",
            PodsTable.Fields.Meta_Id.Name,
            PodsTable.Fields.Meta_PodSetId.Name,
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
        TObjectId nodeId;
        NServer::NObjects::NProto::TPodSpecOther specOther;
        NServer::NObjects::NProto::TPodStatusOther statusOther;
        TYsonString labels;
        FromDBRow(
            row,
            &podId,
            &podSetId,
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
            std::move(labels),
            node,
            std::move(specOther),
            std::move(statusOther));
        YCHECK(PodMap_.emplace(pod->GetId(), std::move(pod)).second);
    }


    TString GetPodSetQueryString()
    {
        const auto& ytConnector = Bootstrap_->GetYTConnector();
        return Format(
            "[%v], [%v], [%v], [%v] from [%v] where is_null([%v])",
            PodSetsTable.Fields.Meta_Id.Name,
            PodSetsTable.Fields.Labels.Name,
            PodSetsTable.Fields.Spec_AntiaffinityConstraints.Name,
            PodSetsTable.Fields.Spec_NodeSegmentId.Name,
            ytConnector->GetTablePath(&PodSetsTable),
            ObjectsTable.Fields.Meta_RemovalTime.Name);
    }

    void ParsePodSetFromRow(TUnversionedRow row)
    {
        TObjectId podSetId;
        TYsonString labels;
        std::vector<NClient::NApi::NProto::TPodSetSpec_TAntiaffinityConstraint> antiaffinityConstraints;
        TObjectId nodeSegmentId;
        FromDBRow(
            row,
            &podSetId,
            &labels,
            &antiaffinityConstraints,
            &nodeSegmentId);

        auto* nodeSegment = FindNodeSegment(nodeSegmentId);
        if (nodeSegmentId && !nodeSegment) {
            LOG_WARNING("Pod set refers to an unknown node segment (PodSetId: %v, NodeSegmentId: %v)",
                podSetId,
                nodeSegmentId);
            return;
        }

        auto podSet = std::make_unique<TPodSet>(
            podSetId,
            std::move(labels),
            nodeSegment,
            std::move(antiaffinityConstraints));
        YCHECK(PodSetMap_.emplace(podSet->GetId(), std::move(podSet)).second);
    }


    void Clear()
    {
        NodeMap_.clear();
        PodMap_.clear();
        PodSetMap_.clear();
        TopologyZoneMap_.clear();
        TopologyKeyZoneMap_.clear();
        NodeSegmentMap_.clear();
        NodeSegmentsLabelFilterCache_.reset();
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

int TCluster::GetNodeCount()
{
    return Impl_->GetNodeCount();
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

int TCluster::GetPodCount()
{
    return Impl_->GetPodCount();
}

void TCluster::LoadSnapshot()
{
    Impl_->LoadSnapshot();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP

