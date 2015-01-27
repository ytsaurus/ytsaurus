#include "stdafx.h"
#include "cypress_integration.h"
#include "node.h"
#include "rack.h"
#include "node_tracker.h"
#include "config.h"

#include <core/ytree/virtual.h>
#include <core/ytree/fluent.h>
#include <core/ytree/exception_helpers.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/cypress_server/virtual.h>
#include <server/cypress_server/node_proxy_detail.h>

#include <server/chunk_server/chunk_manager.h>

#include <server/tablet_server/tablet_cell.h>

#include <server/object_server/object.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NNodeTrackerServer {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NNodeTrackerClient::NProto;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
std::vector<Stroka> ToNames(const std::vector<T>& objects)
{
    std::vector<Stroka> names;
    names.reserve(objects.size());
    for (const auto* object : objects) {
        names.push_back(object->GetName());
    }
    return names;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCellNodeProxy
    : public TMapNodeProxy
{
public:
    TCellNodeProxy(
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
    TNode* FindNode() const
    {
        auto parent = GetParent();
        if (!parent) {
            return nullptr;
        }
        auto address = parent->AsMap()->GetChildKey(this);
        auto nodeTracker = Bootstrap->GetNodeTracker();
        return nodeTracker->FindNodeByAddress(address);
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        const auto* node = FindNode();
        attributes->push_back("state");
        attributes->push_back(TAttributeInfo("register_time", node));
        attributes->push_back(TAttributeInfo("transaction_id", node && node->GetTransaction()));
        attributes->push_back(TAttributeInfo("statistics", node));
        attributes->push_back(TAttributeInfo("addresses", node));
        attributes->push_back(TAttributeInfo("alerts", node));
        attributes->push_back(TAttributeInfo("stored_replica_count", node));
        attributes->push_back(TAttributeInfo("cached_replica_count", node));
        attributes->push_back(TAttributeInfo("tablet_slots", node));
        TMapNodeProxy::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        const auto* node = FindNode();

        if (key == "state") {
            auto state = node ? node->GetState() : ENodeState::Offline;
            BuildYsonFluently(consumer)
                .Value(FormatEnum(state));
            return true;
        }

        if (node) {
            if (key == "register_time") {
                BuildYsonFluently(consumer)
                    .Value(node->GetRegisterTime());
                return true;
            }

            if (key == "transaction_id" && node->GetTransaction()) {
                BuildYsonFluently(consumer)
                    .Value(node->GetTransaction()->GetId());
                return true;
            }

            if (key == "statistics") {
                const auto& statistics = node->Statistics();
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("total_available_space").Value(statistics.total_available_space())
                        .Item("total_used_space").Value(statistics.total_used_space())
                        .Item("total_chunk_count").Value(statistics.total_chunk_count())
                        .Item("total_session_count").Value(node->GetTotalSessionCount())
                        .Item("full").Value(statistics.full())
                        .Item("accepted_chunk_types").Value(FromProto<EObjectType, std::vector<EObjectType>>(statistics.accepted_chunk_types()))
                        .Item("locations").DoListFor(statistics.locations(), [] (TFluentList fluent, const TLocationStatistics& locationStatistics) {
                            fluent
                                .Item().BeginMap()
                                    .Item("available_space").Value(locationStatistics.available_space())
                                    .Item("used_space").Value(locationStatistics.used_space())
                                    .Item("chunk_count").Value(locationStatistics.chunk_count())
                                    .Item("session_count").Value(locationStatistics.session_count())
                                    .Item("full").Value(locationStatistics.full())
                                    .Item("enabled").Value(locationStatistics.enabled())
                                .EndMap();
                        })
                    .EndMap();
                return true;
            }

            if (key == "alerts") {
                BuildYsonFluently(consumer)
                    .Value(node->Alerts());
                return true;
            }

            if (key == "addresses") {
                BuildYsonFluently(consumer)
                    .Value(node->GetDescriptor().Addresses());
                return true;
            }

            if (key == "stored_replica_count") {
                BuildYsonFluently(consumer)
                    .Value(node->StoredReplicas().size());
                return true;
            }

            if (key == "cached_replica_count") {
                BuildYsonFluently(consumer)
                    .Value(node->CachedReplicas().size());
                return true;
            }

            if (key == "tablet_slots") {
                BuildYsonFluently(consumer)
                    .DoListFor(node->TabletSlots(), [] (TFluentList fluent, const TNode::TTabletSlot& slot) {
                        fluent
                            .Item().BeginMap()
                                .Item("state").Value(slot.PeerState)
                                .DoIf(slot.Cell, [&] (TFluentMap fluent) {
                                    fluent
                                        .Item("cell_id").Value(slot.Cell->GetId())
                                        .Item("peer_id").Value(slot.PeerId);
                                })
                            .EndMap();
                    });
                return true;
            }
        }

        return TMapNodeProxy::GetBuiltinAttribute(key, consumer);
    }

    virtual void ValidateCustomAttributeUpdate(
        const Stroka& key,
        const TNullable<TYsonString>& /*oldValue*/,
        const TNullable<TYsonString>& newValue) override
    {
        if (key == "rack") {
            // Validate rack name.
            if (newValue) {
                auto name = ConvertTo<Stroka>(*newValue);
                auto nodeTracker = Bootstrap->GetNodeTracker();
                nodeTracker->GetRackByNameOrThrow(name);
            }
        } else {
            // Forbid to remove configuration attributes.
            static auto nodeConfigKeys = New<TNodeConfig>()->GetRegisteredKeys();
            if (!newValue &&
                std::find(nodeConfigKeys.begin(), nodeConfigKeys.end(), key) != nodeConfigKeys.end())
            {
                ThrowCannotRemoveAttribute(key);
            }

            // Update the attributes and check if they still deserialize OK.
            auto attributes = Attributes().Clone();
            if (newValue) {
                attributes->Set(key, *newValue);
            } else {
                attributes->Remove(key);
            }
            ConvertTo<TNodeConfigPtr>(attributes->ToMap());
        }
    }

    virtual void OnCustomAttributesUpdated() override
    {
        auto* node = FindNode();
        if (!node)
            return;

        auto nodeTracker = Bootstrap->GetNodeTracker();
        nodeTracker->RefreshNodeConfig(node);
    }
};

class TCellNodeTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TCellNodeTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::CellNode;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TCellNodeProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateCellNodeTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return New<TCellNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TCellNodeMapProxy
    : public TMapNodeProxy
{
public:
    TCellNodeMapProxy(
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

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("offline");
        attributes->push_back("registered");
        attributes->push_back("online");
        attributes->push_back("available_space");
        attributes->push_back("used_space");
        attributes->push_back("chunk_count");
        attributes->push_back("online_node_count");
        attributes->push_back("chunk_replicator_enabled");
        TMapNodeProxy::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, IYsonConsumer* consumer) override
    {
        auto nodeTracker = Bootstrap->GetNodeTracker();
        auto chunkManager = Bootstrap->GetChunkManager();

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
                .DoListFor(nodeTracker->Nodes(), [=] (TFluentList fluent, const std::pair<TNodeId, TNode*>& pair) {
                    auto* node = pair.second;
                    if (node->GetState() == expectedState) {
                        fluent.Item().Value(node->GetAddress());
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

class TCellNodeMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TCellNodeMapTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::CellNodeMap;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TCellNodeMapProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

};

INodeTypeHandlerPtr CreateCellNodeMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return New<TCellNodeMapTypeHandler>(bootstrap);
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
    TBootstrap* Bootstrap_;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto nodeTracker = Bootstrap_->GetNodeTracker();
        return ToNames(GetValues(nodeTracker->Racks(), sizeLimit));
    }

    virtual size_t GetSize() const override
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
