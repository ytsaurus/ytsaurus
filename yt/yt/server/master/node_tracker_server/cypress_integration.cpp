#include "cypress_integration.h"
#include "config.h"
#include "node.h"
#include "node_tracker.h"
#include "host.h"
#include "rack.h"
#include "data_center.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>
#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/node_tracker_server/node_discovery_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/tablet_server/tablet_cell.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/core/ytree/exception_helpers.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/server/lib/misc/object_helpers.h>

namespace NYT::NNodeTrackerServer {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NRpc;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

// COMPAT(gritukan)
class TClusterNodeNodeProxy
    : public TMapNodeProxy
{
public:
    TClusterNodeNodeProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TTransaction* transaction,
        TMapNode* trunkNode)
        : TMapNodeProxy(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

    TResolveResult ResolveSelf(
        const TYPath& path,
        const IServiceContextPtr& context) override
    {
        const auto& method = context->GetMethod();
        if (method == "Remove") {
            return TResolveResultThere{GetTargetProxy(), path};
        } else {
            return TMapNodeProxy::ResolveSelf(path, context);
        }
    }

    IYPathService::TResolveResult ResolveAttributes(
        const TYPath& path,
        const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultThere{GetTargetProxy(), "/@" + path};
    }

    void DoWriteAttributesFragment(
        IAsyncYsonConsumer* consumer,
        const std::optional<std::vector<TString>>& attributeKeys,
        bool stable) override
    {
        GetTargetProxy()->WriteAttributesFragment(consumer, attributeKeys, stable);
    }

private:
    IObjectProxyPtr GetTargetProxy() const
    {
        auto address = GetParent()->AsMap()->GetChildKeyOrThrow(this);

        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeByAddressOrThrow(address);
        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(node, nullptr);
    }
};

class TClusterNodeNodeTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TClusterNodeNodeTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandlerImpl(bootstrap)
    { }

    EObjectType GetObjectType() const override
    {
        return EObjectType::ClusterNodeNode;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TClusterNodeNodeProxy>(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateClusterNodeNodeTypeHandler(TBootstrap* bootstrap)
{
    return New<TClusterNodeNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

// COMPAT(gritukan)
class TLegacyClusterNodeMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TLegacyClusterNodeMapTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandlerImpl(bootstrap)
    { }

    EObjectType GetObjectType() const override
    {
        return EObjectType::LegacyClusterNodeMap;
    }

private:
    ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TMapNodeProxy>(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateLegacyClusterNodeMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return New<TLegacyClusterNodeMapTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualNodeMapBase
    : public TVirtualMapBase
{
public:
    TVirtualNodeMapBase(
        TBootstrap* bootstrap,
        INodePtr owningNode,
        std::optional<ENodeFlavor> flavor)
        : TVirtualMapBase(std::move(owningNode))
        , Bootstrap_(bootstrap)
        , Flavor_(flavor)
    { }

protected:
    TBootstrap* const Bootstrap_;

    // std::nullopt stands for all cluster nodes.
    const std::optional<ENodeFlavor> Flavor_;

private:
    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->FindNodeByAddress(TString(key));
        if (!IsObjectAlive(node)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(node);
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TVirtualMapBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Offline)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Registered)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Online)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Unregistered)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Mixed)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::AvailableSpace);
        descriptors->push_back(EInternedAttributeKey::UsedSpace);
        descriptors->push_back(EInternedAttributeKey::AvailableSpacePerMedium);
        descriptors->push_back(EInternedAttributeKey::UsedSpacePerMedium);
        descriptors->push_back(EInternedAttributeKey::ChunkReplicaCount);
        descriptors->push_back(EInternedAttributeKey::OnlineNodeCount);
        descriptors->push_back(EInternedAttributeKey::OfflineNodeCount);
        descriptors->push_back(EInternedAttributeKey::BannedNodeCount);
        descriptors->push_back(EInternedAttributeKey::DecommissionedNodeCount);
        descriptors->push_back(EInternedAttributeKey::WithAlertsNodeCount);
        descriptors->push_back(EInternedAttributeKey::FullNodeCount);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MasterCacheNodes)
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TimestampProviderNodes)
            .SetOpaque(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        auto statistics = nodeTracker->GetAggregatedNodeStatistics();

        switch (key) {
            case EInternedAttributeKey::Offline:
            case EInternedAttributeKey::Registered:
            case EInternedAttributeKey::Online:
            case EInternedAttributeKey::Unregistered:
            case EInternedAttributeKey::Mixed: {
                ENodeState state;
                switch (key) {
                    case EInternedAttributeKey::Offline:
                        state = ENodeState::Offline;
                        break;
                    case EInternedAttributeKey::Registered:
                        state = ENodeState::Registered;
                        break;
                    case EInternedAttributeKey::Online:
                        state = ENodeState::Online;
                        break;
                    case EInternedAttributeKey::Unregistered:
                        state = ENodeState::Unregistered;
                        break;
                    case EInternedAttributeKey::Mixed:
                        state = ENodeState::Mixed;
                        break;
                    default:
                        YT_VERIFY(false);
                }
                BuildYsonFluently(consumer)
                    .DoListFor(nodeTracker->Nodes(), [=] (TFluentList fluent, const std::pair<TObjectId, TNode*>& pair) {
                        auto* node = pair.second;
                        if (node->GetAggregatedState() == state) {
                            fluent.Item().Value(node->GetDefaultAddress());
                        }
                    });
                return true;
            }

            case EInternedAttributeKey::AvailableSpace:
                BuildYsonFluently(consumer)
                    .Value(statistics.TotalSpace.Available);
                return true;

            case EInternedAttributeKey::UsedSpace:
                BuildYsonFluently(consumer)
                    .Value(statistics.TotalSpace.Used);
                return true;

            case EInternedAttributeKey::AvailableSpacePerMedium:
                BuildYsonFluently(consumer)
                    .DoMapFor(statistics.SpacePerMedium.begin(), statistics.SpacePerMedium.end(),
                        [&] (TFluentMap fluent, NChunkClient::TMediumMap<TDiskSpaceStatistics>::const_iterator it) {
                            auto mediumIndex = it->first;
                            const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                            if (!medium || medium->GetCache()) {
                                return;
                            }
                            fluent
                                .Item(medium->GetName()).Value(it->second.Available);
                        });
                return true;

            case EInternedAttributeKey::UsedSpacePerMedium:
                BuildYsonFluently(consumer)
                    .DoMapFor(statistics.SpacePerMedium.begin(), statistics.SpacePerMedium.end(),
                        [&] (TFluentMap fluent, NChunkClient::TMediumMap<TDiskSpaceStatistics>::const_iterator it) {
                            auto mediumIndex = it->first;
                            const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                            if (!medium) {
                                return;
                            }
                            fluent
                                .Item(medium->GetName()).Value(it->second.Used);
                        });
                return true;

            case EInternedAttributeKey::ChunkReplicaCount:
                BuildYsonFluently(consumer)
                    .Value(statistics.ChunkReplicaCount);
                return true;

            case EInternedAttributeKey::OnlineNodeCount:
                BuildYsonFluently(consumer)
                    .Value(statistics.OnlineNodeCount);
                return true;

            case EInternedAttributeKey::OfflineNodeCount:
                BuildYsonFluently(consumer)
                    .Value(statistics.OfflineNodeCount);
                return true;

            case EInternedAttributeKey::BannedNodeCount:
                BuildYsonFluently(consumer)
                    .Value(statistics.BannedNodeCount);
                return true;

            case EInternedAttributeKey::DecommissionedNodeCount:
                BuildYsonFluently(consumer)
                    .Value(statistics.DecommissinedNodeCount);
                return true;

            case EInternedAttributeKey::WithAlertsNodeCount:
                BuildYsonFluently(consumer)
                    .Value(statistics.WithAlertsNodeCount);
                return true;

            case EInternedAttributeKey::FullNodeCount:
                BuildYsonFluently(consumer)
                    .Value(statistics.FullNodeCount);
                return true;

            case EInternedAttributeKey::MasterCacheNodes:
                BuildYsonFluently(consumer)
                    .Value(Bootstrap_->GetNodeTracker()->GetNodeAddressesForRole(NNodeTrackerClient::ENodeRole::MasterCache));
                return true;

            case EInternedAttributeKey::TimestampProviderNodes:
                BuildYsonFluently(consumer)
                    .Value(Bootstrap_->GetNodeTracker()->GetNodeAddressesForRole(NNodeTrackerClient::ENodeRole::TimestampProvider));
                return true;

            default:
                break;
        }

        return TVirtualMapBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualClusterNodeMap
    : public TVirtualNodeMapBase
{
public:
    TVirtualClusterNodeMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualNodeMapBase(bootstrap, std::move(owningNode), /*flavor*/ std::nullopt)
    { }

private:
    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TString> keys;
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& nodes = nodeTracker->Nodes();
        keys.reserve(std::min<i64>(nodes.size(), sizeLimit));
        for (auto [nodeId, node] : nodes) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            keys.push_back(node->GetDefaultAddress());
            if (std::ssize(keys) >= sizeLimit) {
                break;
            }
        }

        return keys;
    }

    i64 GetSize() const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->Nodes().GetSize();
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateClusterNodeMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ClusterNodeMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualClusterNodeMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualFlavoredNodeMap
    : public TVirtualNodeMapBase
{
public:
    TVirtualFlavoredNodeMap(
        TBootstrap* bootstrap,
        INodePtr owningNode,
        EObjectType objectType)
        : TVirtualNodeMapBase(
            bootstrap,
            std::move(owningNode),
            GetNodeFlavorByMapObjectType(objectType))
    { }

private:
    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TString> keys;
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& nodes = nodeTracker->GetNodesWithFlavor(*Flavor_);
        keys.reserve(std::min<i64>(nodes.size(), sizeLimit));
        for (auto* node : nodes) {
            if (!IsObjectAlive(node)) {
                continue;
            }
            keys.push_back(node->GetDefaultAddress());
            if (std::ssize(keys) >= sizeLimit) {
                break;
            }
        }

        return keys;
    }

    i64 GetSize() const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->GetNodesWithFlavor(*Flavor_).size();
    }

    static ENodeFlavor GetNodeFlavorByMapObjectType(EObjectType objectType)
    {
        switch (objectType) {
            case EObjectType::DataNodeMap:
                return ENodeFlavor::Data;
            case EObjectType::ExecNodeMap:
                return ENodeFlavor::Exec;
            case EObjectType::TabletNodeMap:
                return ENodeFlavor::Tablet;
            case EObjectType::ChaosNodeMap:
                return ENodeFlavor::Chaos;
            default:
                YT_ABORT();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateFlavoredNodeMapTypeHandler(TBootstrap* bootstrap, EObjectType objectType)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        objectType,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualFlavoredNodeMap>(bootstrap, owningNode, objectType);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualHostMap
    : public TVirtualMapBase
{
public:
    TVirtualHostMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(std::move(owningNode))
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& hostMap = nodeTracker->Hosts();

        std::vector<TString> keys;
        keys.reserve(std::min<i64>(sizeLimit, hostMap.GetSize()));
        for (auto [hostId, host] : hostMap) {
            keys.push_back(host->GetName());
            if (std::ssize(keys) >= sizeLimit) {
                break;
            }
        }

        return keys;
    }

    i64 GetSize() const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->Hosts().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* host = nodeTracker->FindHostByName(TString(key));
        if (!IsObjectAlive(host)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(host);
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateHostMapTypeHandler(TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::HostMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualHostMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualRackMap
    : public TVirtualMapBase
{
public:
    TVirtualRackMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 /*sizeLimit*/) const override
    {
        std::vector<TString> keys;
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto [rackId, rack] : nodeTracker->Racks()) {
            keys.push_back(rack->GetName());
        }
        return keys;
    }

    i64 GetSize() const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->Racks().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        auto* rack = nodeTracker->FindRackByName(TString(key));
        if (!IsObjectAlive(rack)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(rack);
    }
};

INodeTypeHandlerPtr CreateRackMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::RackMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualRackMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualDataCenterMap
    : public TVirtualMapBase
{
public:
    TVirtualDataCenterMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 /*sizeLimit*/) const override
    {
        std::vector<TString> keys;
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        for (auto [dataCenterId, dataCenter] : nodeTracker->DataCenters()) {
            keys.push_back(dataCenter->GetName());
        }
        return keys;
    }

    i64 GetSize() const override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->DataCenters().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto* dc = nodeTracker->FindDataCenterByName(TString(key));
        if (!IsObjectAlive(dc)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(dc);
    }
};

INodeTypeHandlerPtr CreateDataCenterMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::DataCenterMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualDataCenterMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
