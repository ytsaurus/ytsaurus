#include "cypress_integration.h"
#include "config.h"
#include "node.h"
#include "node_tracker.h"
#include "rack.h"
#include "data_center.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/chunk_server/chunk_manager.h>
#include <yt/server/chunk_server/medium.h>

#include <yt/server/cypress_server/node_proxy_detail.h>
#include <yt/server/cypress_server/virtual.h>

#include <yt/server/object_server/object.h>

#include <yt/server/tablet_server/tablet_cell.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <yt/core/ytree/exception_helpers.h>
#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/virtual.h>

#include <yt/server/misc/object_helpers.h>

namespace NYT {
namespace NNodeTrackerServer {

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

    virtual TResolveResult ResolveSelf(
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

    virtual IYPathService::TResolveResult ResolveAttributes(
        const TYPath& path,
        const IServiceContextPtr& /*context*/) override
    {
        return TResolveResultThere{GetTargetProxy(), "/@" + path};
    }

    virtual void DoWriteAttributesFragment(
        IAsyncYsonConsumer* consumer,
        const TNullable<std::vector<TString>>& attributeKeys,
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
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::ClusterNodeNode;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
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

class TClusterNodeMapProxy
    : public TMapNodeProxy
{
public:
    TClusterNodeMapProxy(
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

private:
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TMapNodeProxy::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor("offline")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("registered")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("online")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("unregistered")
            .SetOpaque(true));
        descriptors->push_back(TAttributeDescriptor("mixed")
            .SetOpaque(true));
        descriptors->push_back("available_space");
        descriptors->push_back("used_space");
        descriptors->push_back("available_space_per_medium");
        descriptors->push_back("used_space_per_medium");
        descriptors->push_back("chunk_replica_count");
        descriptors->push_back("online_node_count");
        descriptors->push_back("offline_node_count");
        descriptors->push_back("banned_node_count");
        descriptors->push_back("decommissioned_node_count");
        descriptors->push_back("with_alerts_node_count");
        descriptors->push_back("full_node_count");
    }

    virtual bool GetBuiltinAttribute(const TString& key, IYsonConsumer* consumer) override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        if (key == "offline" || key == "registered" || key == "online" || key == "unregistered" || key == "mixed") {
            auto state =
                    key == "offline"      ? ENodeState::Offline :
                    key == "registered"   ? ENodeState::Registered :
                    key == "online"       ? ENodeState::Online :
                    key == "unregistered" ? ENodeState::Unregistered :
                    /* key == "mixed" */    ENodeState::Mixed;
            BuildYsonFluently(consumer)
                .DoListFor(nodeTracker->Nodes(), [=] (TFluentList fluent, const std::pair<const TObjectId&, TNode*>& pair) {
                    auto* node = pair.second;
                    if (node->GetAggregatedState() == state) {
                        fluent.Item().Value(node->GetDefaultAddress());
                    }
                });
            return true;
        }

        auto statistics = nodeTracker->GetTotalNodeStatistics();

        if (key == "available_space") {
            BuildYsonFluently(consumer)
                .Value(statistics.TotalSpace.Available);
            return true;
        }

        if (key == "used_space") {
            BuildYsonFluently(consumer)
                .Value(statistics.TotalSpace.Used);
            return true;
        }

        if (key == "available_space_per_medium") {
            BuildYsonFluently(consumer)
                .DoMapFor(chunkManager->Media(),
                    [&] (TFluentMap fluent, const std::pair<const TMediumId&, TMedium*>& pair) {
                        const auto* medium = pair.second;
                        if (medium->GetCache()) {
                            return;
                        }
                        fluent
                            .Item(medium->GetName()).Value(statistics.SpacePerMedium[medium->GetIndex()].Available);
                    });
            return true;
        }

        if (key == "used_space_per_medium") {
            BuildYsonFluently(consumer)
                .DoMapFor(chunkManager->Media(),
                    [&] (TFluentMap fluent, const std::pair<const TMediumId&, TMedium*>& pair) {
                        const auto* medium = pair.second;
                        fluent
                            .Item(medium->GetName()).Value(statistics.SpacePerMedium[medium->GetIndex()].Used);
                    });
            return true;
        }

        if (key == "chunk_replica_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.ChunkReplicaCount);
            return true;
        }

        if (key == "online_node_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.OnlineNodeCount);
            return true;
        }

        if (key == "offline_node_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.OfflineNodeCount);
            return true;
        }

        if (key == "banned_node_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.BannedNodeCount);
            return true;
        }

        if (key == "decommissioned_node_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.DecommissinedNodeCount);
            return true;
        }

        if (key == "with_alerts_node_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.WithAlertsNodeCount);
            return true;
        }

        if (key == "full_node_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.FullNodeCount);
            return true;
        }

        return TMapNodeProxy::GetBuiltinAttribute(key, consumer);
    }
};

class TClusterNodeMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TClusterNodeMapTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() const override
    {
        return EObjectType::ClusterNodeMap;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TClusterNodeMapProxy>(
            Bootstrap_,
            &Metadata_,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateClusterNodeMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return New<TClusterNodeMapTypeHandler>(bootstrap);
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

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TString> keys;
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Racks()) {
            const auto* rack = pair.second;
            keys.push_back(rack->GetName());
        }
        return keys;
    }

    virtual i64 GetSize() const override
    {
        const auto& nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->Racks().GetSize();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
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
    YCHECK(bootstrap);

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

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        std::vector<TString> keys;
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->DataCenters()) {
            const auto* dc = pair.second;
            keys.push_back(dc->GetName());
        }
        return keys;
    }

    virtual i64 GetSize() const override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->DataCenters().GetSize();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto* dc = nodeTracker->FindDataCenterByName(TString(key));
        if (!IsObjectAlive(dc)) {
            return nullptr;
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(dc);
    }
};

INodeTypeHandlerPtr CreateDataCenterMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::DataCenterMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualDataCenterMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
