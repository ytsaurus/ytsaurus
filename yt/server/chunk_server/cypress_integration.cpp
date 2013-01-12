#include "stdafx.h"
#include "cypress_integration.h"
#include "node.h"
#include "node_statistics.h"
#include "private.h"
#include "chunk.h"
#include "chunk_list.h"

#include <ytlib/misc/string.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/actions/bind.h>

#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/ytree/yson_string.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <server/cypress_server/virtual.h>
#include <server/cypress_server/node_proxy_detail.h>

#include <server/chunk_server/chunk_manager.h>
#include <server/chunk_server/node_authority.h>

#include <server/orchid/cypress_integration.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NYPath;
using namespace NCypressServer;
using namespace NCypressClient;
using namespace NMetaState;
using namespace NOrchid;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(TBootstrap* bootstrap, EObjectType objectType)
        : Bootstrap(bootstrap)
        , ObjectType(objectType)
    { }

private:
    TBootstrap* Bootstrap;
    EObjectType ObjectType;

    const yhash_set<TChunkId>& GetFilteredChunkIds() const
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        switch (ObjectType) {
            case EObjectType::LostChunkMap:
                return chunkManager->LostChunkIds();
            case EObjectType::LostVitalChunkMap:
                return chunkManager->LostVitalChunkIds();
            case EObjectType::OverreplicatedChunkMap:
                return chunkManager->OverreplicatedChunkIds();
            case EObjectType::UnderreplicatedChunkMap:
                return chunkManager->UnderreplicatedChunkIds();
            default:
                YUNREACHABLE();
        }
    }

    bool CheckFilter(const TChunkId& id) const
    {
        if (TypeFromId(id) != EObjectType::Chunk) {
            return nullptr;
        }

        if (ObjectType == EObjectType::ChunkMap) {
            return true;
        }

        const auto& ids = GetFilteredChunkIds();
        return ids.find(id) != ids.end();
    }

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        if (ObjectType == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap->GetChunkManager();
            auto ids = ToObjectIds(chunkManager->GetChunks(sizeLimit));
            // NB: No size limit is needed here.
            return ConvertToStrings(ids.begin(), ids.end());
        } else {
            const auto& ids = GetFilteredChunkIds();
            return ConvertToStrings(ids.begin(), ids.end(), sizeLimit);
        }
    }

    virtual size_t GetSize() const override
    {
        if (ObjectType == EObjectType::ChunkMap) {
            auto chunkManager = Bootstrap->GetChunkManager();
            return chunkManager->GetChunkCount();
        } else {
            return GetFilteredChunkIds().size();
        }
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto id = TChunkId::FromString(key);
        if (!CheckFilter(id)) {
            return nullptr;
        }

        auto chunkManager = Bootstrap->GetChunkManager();
        auto* chunk = chunkManager->FindChunk(id);
        if (!chunk || !chunk->IsAlive()) {
            return nullptr;
        }
        
        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(chunk);
    }
};

INodeTypeHandlerPtr CreateChunkMapTypeHandler(
    TBootstrap* bootstrap,
    EObjectType objectType)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualChunkMap>(bootstrap, objectType);
    return CreateVirtualTypeHandler(bootstrap, objectType, service, true);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkListMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualChunkListMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto ids = ToObjectIds(chunkManager->GetChunkLists(sizeLimit));
        // NB: No size limit is needed here.
        return ConvertToStrings(ids.begin(), ids.end());
    }

    virtual size_t GetSize() const override
    {
        return Bootstrap->GetChunkManager()->GetChunkListCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        auto id = TChunkListId::FromString(key);
        auto* chunkList = chunkManager->FindChunkList(id);
        if (!chunkList || !chunkList->IsAlive()) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(chunkList);
    }
};

INodeTypeHandlerPtr CreateChunkListMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkListMap,
        New<TVirtualChunkListMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

class TNodeAuthority
    : public INodeAuthority
{
public:
    explicit TNodeAuthority(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual bool IsAuthorized(const Stroka& address) override
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        auto resolver = cypressManager->CreateResolver();
        auto nodesNode = resolver->ResolvePath("//sys/nodes");
        if (!nodesNode) {
            LOG_ERROR("Missing //sys/nodes");
            return false;
        }

        auto nodesMap = nodesNode->AsMap();
        auto nodeNode = nodesMap->FindChild(address);

        if (!nodeNode) {
            // New node.
            return true;
        }

        bool banned = nodeNode->Attributes().Get<bool>("banned", false);
        return !banned;
    }
    
private:
    TBootstrap* Bootstrap;

};

INodeAuthorityPtr CreateNodeAuthority(TBootstrap* bootstrap)
{
    return New<TNodeAuthority>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TDataNodeProxy
    : public TMapNodeProxy
{
public:
    TDataNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TMapNode* trunkNode)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    TDataNode* GetNode() const
    {
        auto address = GetParent()->AsMap()->GetChildKey(this);
        return Bootstrap->GetChunkManager()->FindNodeByAddress(address);
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const override
    {
        const auto* node = GetNode();
        attributes->push_back(TAttributeInfo("state"));
        attributes->push_back(TAttributeInfo("confirmed", node));
        attributes->push_back(TAttributeInfo("incarnation_id", node));
        attributes->push_back(TAttributeInfo("statistics", node));
        TMapNodeProxy::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const override
    {
        const auto* node = GetNode();

        if (key == "state") {
            auto state = node ? node->GetState() : ENodeState(ENodeState::Offline);
            BuildYsonFluently(consumer)
                .Value(FormatEnum(state));
            return true;
        }

        if (node) {
            if (key == "confirmed") {
                ValidateActiveLeader();
                BuildYsonFluently(consumer)
                    .Value(FormatBool(Bootstrap->GetChunkManager()->IsNodeConfirmed(node)));
                return true;
            }

            if (key == "incarnation_id") {
                BuildYsonFluently(consumer)
                    .Value(node->GetIncarnationId());
                return true;
            }

            if (key == "statistics") {
                const auto& nodeStatistics = node->Statistics();
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("total_available_space").Value(nodeStatistics.total_available_space())
                        .Item("total_used_space").Value(nodeStatistics.total_used_space())
                        .Item("total_chunk_count").Value(nodeStatistics.total_chunk_count())
                        .Item("total_session_count").Value(node->GetTotalSessionCount())
                        .Item("full").Value(nodeStatistics.full())
                        .Item("locations").DoListFor(nodeStatistics.locations(), [] (TFluentList fluent, const NProto::TLocationStatistics& locationStatistics) {
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
        }

        return TMapNodeProxy::GetSystemAttribute(key, consumer);
    }

    virtual void ValidateUserAttributeUpdate(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue) override
    {
        UNUSED(oldValue);

        if (key == "banned") {
            if (newValue) {
                ConvertTo<bool>(*newValue);
            }
        }
    }
};

class TNodeTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TNodeTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::Node;
    }

private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TDataNodeProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }
};

INodeTypeHandlerPtr CreateNodeTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return New<TNodeTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class TNodeMapBehavior
    : public TNodeBehaviorBase<TMapNode, TMapNodeProxy>
{
public:
    TNodeMapBehavior(TBootstrap* bootstrap, TMapNode* trunkNode)
        : TNodeBehaviorBase<TMapNode, TMapNodeProxy>(bootstrap, trunkNode)
    {
        bootstrap->GetChunkManager()->SubscribeNodeRegistered(BIND(
            &TNodeMapBehavior::OnRegistered,
            MakeWeak(this)));
    }

private:
    void OnRegistered(const TDataNode* node)
    {
        Stroka address = node->GetAddress();

        auto metaStateFacade = Bootstrap->GetMetaStateFacade();

        // We're already in the state thread but need to postpone the planned changes and enqueue a callback.
        // Doing otherwise will turn node registration and Cypress update into a single
        // logged change, which is undesirable.
        BIND(&TNodeMapBehavior::CreateNodeIfNeeded, MakeStrong(this), address)
            .Via(metaStateFacade->GetEpochInvoker())
            .Run();
    }

    void CreateNodeIfNeeded(const Stroka& address)
    {
        auto proxy = GetProxy();

        if (proxy->FindChild(address))
            return;

        // TODO(babenko): make a single transaction
        // TODO(babenko): check for errors and retry

        {
            auto req = TCypressYPathProxy::Create("/" + ToYPathLiteral(address));
            req->set_type(EObjectType::Node);
            ExecuteVerb(proxy, req);
        }

        {
            auto req = TCypressYPathProxy::Create("/" + ToYPathLiteral(address) + "/orchid");
            req->set_type(EObjectType::Orchid);

            auto attributes = CreateEphemeralAttributes();
            attributes->Set("remote_address", address);
            ToProto(req->mutable_node_attributes(), *attributes);

            ExecuteVerb(proxy, req);
        }
    }

};

class TDataNodeMapProxy
    : public TMapNodeProxy
{
public:
    TDataNodeMapProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TMapNode* trunkNode)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

private:
    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const override
    {
        attributes->push_back("offline");
        attributes->push_back("banned");
        attributes->push_back("registered");
        attributes->push_back("online");
        attributes->push_back("unconfirmed");
        attributes->push_back("confirmed");
        attributes->push_back("available_space");
        attributes->push_back("used_space");
        attributes->push_back("chunk_count");
        attributes->push_back("session_count");
        attributes->push_back("online_node_count");
        attributes->push_back("chunk_replicator_enabled");
        TMapNodeProxy::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer) const override
    {
        auto chunkManager = Bootstrap->GetChunkManager();

        if (key == "offline") {
            BuildYsonFluently(consumer)
                .DoListFor(GetKeys(), [=] (TFluentList fluent, Stroka address) {
                    if (!chunkManager->FindNodeByAddress(address) &&
                        !this->GetChild(address)->Attributes().Get<bool>("banned", false))
                    {
                        fluent.Item().Value(address);
                    }
            });
            return true;
        }

        if (key == "banned") {
            BuildYsonFluently(consumer)
                .DoListFor(GetKeys(), [=] (TFluentList fluent, Stroka address) {
                    if (this->GetChild(address)->Attributes().Get<bool>("banned", false)) {
                        fluent.Item().Value(address);
                    }
            });
            return true;
        }

        if (key == "registered" || key == "online") {
            auto expectedState = key == "registered" ? ENodeState::Registered : ENodeState::Online;
            BuildYsonFluently(consumer)
                .DoListFor(chunkManager->GetNodes(), [=] (TFluentList fluent, TDataNode* node) {
                    if (node->GetState() == expectedState) {
                        fluent.Item().Value(node->GetAddress());
                    }
                });
            return true;
        }

        if (key == "unconfirmed" || key == "confirmed") {
            ValidateActiveLeader();
            bool state = key == "confirmed";
            BuildYsonFluently(consumer)
                .DoListFor(chunkManager->GetNodes(), [=] (TFluentList fluent, TDataNode* node) {
                    if (chunkManager->IsNodeConfirmed(node) == state) {
                        fluent.Item().Value(node->GetAddress());
                    }
                });
            return true;
        }

        auto statistics = chunkManager->GetTotalNodeStatistics();
        if (key == "available_space") {
            BuildYsonFluently(consumer)
                .Value(statistics.AvailbaleSpace);
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

        if (key == "session_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.SessionCount);
            return true;
        }

        // COMPAT(babenko): notify Roman
        if (key == "online_node_count" || key == "online_holder_count") {
            BuildYsonFluently(consumer)
                .Value(statistics.OnlineNodeCount);
            return true;
        }

        if (key == "chunk_replicator_enabled") {
            ValidateActiveLeader();
            BuildYsonFluently(consumer)
                .Value(chunkManager->IsReplicatorEnabled());
            return true;
        }

        return TMapNodeProxy::GetSystemAttribute(key, consumer);
    }
};

class TNodeMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit TNodeMapTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType() override
    {
        return EObjectType::NodeMap;
    }
    
private:
    virtual ICypressNodeProxyPtr DoGetProxy(
        TMapNode* trunkNode,
        TTransaction* transaction) override
    {
        return New<TDataNodeMapProxy>(
            this,
            Bootstrap,
            transaction,
            trunkNode);
    }

    virtual INodeBehaviorPtr DoCreateBehavior(TMapNode* trunkNode) override
    {
        return New<TNodeMapBehavior>(Bootstrap, trunkNode);
    }

};

INodeTypeHandlerPtr CreateNodeMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return New<TNodeMapTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
