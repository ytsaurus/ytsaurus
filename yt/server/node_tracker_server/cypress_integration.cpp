#include "stdafx.h"
#include "cypress_integration.h"
#include "node.h"
#include "rack.h"
#include "node_tracker.h"
#include "config.h"

#include <core/ytree/virtual.h>
#include <core/ytree/fluent.h>
#include <core/ytree/exception_helpers.h>

#include <server/cypress_server/virtual.h>
#include <server/cypress_server/node_proxy_detail.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/object_server/object_detail.h>
#include <server/object_server/type_handler_detail.h>

#include <server/cell_master/bootstrap.h>

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
using namespace NNodeTrackerClient::NProto;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TClusterNodeNodeProxy
    : public TMapNodeProxy
{
public:
    TClusterNodeNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TMapNode* trunkNode)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

    virtual TResolveResult ResolveSelf(const TYPath& path, IServiceContextPtr context) override
    {
        const auto& method = context->GetMethod();
        if (method == "Remove") {
            return TResolveResult::There(GetTargetProxy(), path);
        } else {
            return TMapNodeProxy::ResolveSelf(path, context);
        }
    }

    virtual IYPathService::TResolveResult ResolveAttributes(
        const TYPath& path,
        IServiceContextPtr /*context*/) override
    {
        return TResolveResult::There(
            GetTargetProxy(),
            "/@" + path);
    }

    virtual void WriteAttributesFragment(
        IYsonConsumer* consumer,
        const TAttributeFilter& filter,
        bool sortKeys) override
    {
        GetTargetProxy()->WriteAttributesFragment(consumer, filter, sortKeys);
    }

private:
    IObjectProxyPtr GetTargetProxy() const
    {
        auto address = GetParent()->AsMap()->GetChildKey(this);

        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto* node = nodeTracker->GetNodeByAddressOrThrow(address);
        auto objectManager = Bootstrap_->GetObjectManager();
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

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::ClusterNodeNode;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TClusterNodeNodeProxy>(
            this,
            Bootstrap_,
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
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        TTransaction* transaction,
        TMapNode* trunkNode)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    virtual bool IsLeaderReadRequired() const override
    {
        // Needed due to "chunk_replicator_enabled" attribute.
        return true;
    }

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TMapNodeProxy::ListSystemAttributes(descriptors);

        descriptors->push_back("offline");
        descriptors->push_back("registered");
        descriptors->push_back("online");
        descriptors->push_back("available_space");
        descriptors->push_back("used_space");
        descriptors->push_back("chunk_count");
        descriptors->push_back("online_node_count");
        descriptors->push_back("chunk_replicator_enabled");
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto chunkManager = Bootstrap_->GetChunkManager();

        if (key == "offline") {
            BuildYsonFluently(consumer)
                .DoListFor(GetKeys(), [=] (TFluentList fluent, Stroka address) {
                    if (!nodeTracker->FindNodeByAddress(address)) {
                        fluent.Item().Value(address);
                    }
                });
            return true;
        }

        if (key == "registered" || key == "online") {
            auto expectedState = key == "registered" ? ENodeState::Registered : ENodeState::Online;
            BuildYsonFluently(consumer)
                .DoListFor(nodeTracker->Nodes(), [=] (TFluentList fluent, const std::pair<const TObjectId&, TNode*>& pair) {
                    auto* node = pair.second;
                    if (node->GetLocalState() == expectedState) {
                        fluent.Item().Value(node->GetDefaultAddress());
                    }
                });
            return true;
        }

        auto statistics = nodeTracker->GetTotalNodeStatistics();
        if (key == "available_space") {
            BuildYsonFluently(consumer)
                .Value(statistics.AvailableSpace);
            return true;
        }

        if (key == "used_space") {
            BuildYsonFluently(consumer)
                .Value(statistics.UsedSpace);
            return true;
        }

        if (key == "chunk_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.ChunkCount);
            return true;
        }

        if (key == "online_node_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.OnlineNodeCount);
            return true;
        }

        if (key == "chunk_replicator_enabled") {
            BuildYsonFluently(consumer)
                .Value(chunkManager->IsReplicatorEnabled());
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

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::ClusterNodeMap;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TClusterNodeMapProxy>(
            this,
            Bootstrap_,
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
    explicit TVirtualRackMap(TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    virtual std::vector<Stroka> GetKeys(i64 sizeLimit) const override
    {
        std::vector<Stroka> keys;
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        for (const auto& pair : nodeTracker->Racks()) {
            const auto* rack = pair.second;
            keys.push_back(rack->GetName());
        }
        return keys;
    }

    virtual i64 GetSize() const override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        return nodeTracker->Racks().GetSize();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        auto* rack = nodeTracker->FindRackByName(Stroka(key));
        if (!IsObjectAlive(rack)) {
            return nullptr;
        }

        auto objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(rack);
    }
};

INodeTypeHandlerPtr CreateRackMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualRackMap>(bootstrap);
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::RackMap,
        service,
        EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
