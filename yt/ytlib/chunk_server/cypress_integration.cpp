#include "stdafx.h"
#include "cypress_integration.h"

#include "../misc/string.h"
#include "../ytree/virtual.h"
#include "../ytree/fluent.h"
#include "../cypress/virtual.h"
#include "../cypress/node_proxy_detail.h"
#include "../cypress/cypress_ypath_rpc.h"

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NCypress;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkMap(TChunkManager* chunkManager)
        : ChunkManager(chunkManager)
    { }

private:
    TChunkManager::TPtr ChunkManager;

    virtual yvector<Stroka> GetKeys()
    {
        return ConvertToStrings(ChunkManager->GetChunkIds());
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto id = TChunkId::FromString(key);
        auto* chunk = ChunkManager->FindChunk(id);
        if (chunk == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                // TODO: locations
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("chunk_size").Scalar(chunk->GetSize())
                        .Item("meta_size").Scalar(
                            chunk->GetMasterMeta() == TSharedRef()
                            ? -1
                            : static_cast<i64>(chunk->GetMasterMeta().Size()))
                        .Item("chunk_list_id").Scalar(chunk->GetChunkListId().ToString())
                    .EndMap();
            }));
    }
};

INodeTypeHandler::TPtr CreateChunkMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::ChunkMap,
        // TODO: extract type name
        "chunk_map",
        ~New<TVirtualChunkMap>(chunkManager));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkListMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkListMap(TChunkManager* chunkManager)
        : ChunkManager(chunkManager)
    { }

private:
    TChunkManager::TPtr ChunkManager;

    virtual yvector<Stroka> GetKeys()
    {
        return ConvertToStrings(ChunkManager->GetChunkListIds());
    }

    virtual IYPathService::TPtr GetItemService(const Stroka& key)
    {
        auto id = TChunkListId::FromString(key);
        auto* chunkList = ChunkManager->FindChunkList(id);
        if (chunkList == NULL) {
            return NULL;
        }

        return IYPathService::FromProducer(~FromFunctor([=] (IYsonConsumer* consumer)
            {
                // TODO: ChunkIds
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Item("replica_count").Scalar(chunkList->GetReplicaCount())
                    .EndMap();
            }));
    }
};

INodeTypeHandler::TPtr CreateChunkListMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);

    return CreateVirtualTypeHandler(
        cypressManager,
        ERuntimeNodeType::ChunkListMap,
        // TODO: extract type name
        "chunk_list_map",
        ~New<TVirtualChunkListMap>(chunkManager));
}

////////////////////////////////////////////////////////////////////////////////

class THolderRegistry
    : public IHolderRegistry
{
public:
    typedef TIntrusivePtr<THolderRegistry> TPtr;

    THolderRegistry(TCypressManager* cypressManager)
        : CypressManager(cypressManager)
    { }

    virtual bool IsHolderAuthorized(const Stroka& address)
    {
        UNUSED(address);
        return true;
    }
    
private:
    TCypressManager::TPtr CypressManager;

};

IHolderRegistry::TPtr CreateHolderRegistry(
    TCypressManager* cypressManager)
{
    return New<THolderRegistry>(cypressManager);
}

////////////////////////////////////////////////////////////////////////////////

class THolderTypeHandler
    : public TMapNodeTypeHandler
{
public:
    typedef THolderTypeHandler TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    THolderTypeHandler(
        TCypressManager* cypressManager,
        TChunkManager* chunkManager)
        : TMapNodeTypeHandler(cypressManager)
        , CypressManager(cypressManager)
        , ChunkManager(chunkManager)
    {
        // NB: No smartpointer for this here.
        RegisterGetter("alive", FromMethod(&TThis::GetAlive, this));
    }

    virtual ERuntimeNodeType GetRuntimeType()
    {
        return ERuntimeNodeType::Holder;
    }

    virtual Stroka GetTypeName()
    {
        // TODO: extract type name
        return "holder";
    }

    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::INode* manifest)
    {
        UNUSED(transactionId);
        UNUSED(manifest);
        return Create(TBranchedNodeId(nodeId, NullTransactionId));
    }

private:
    TCypressManager::TPtr CypressManager;
    TChunkManager::TPtr ChunkManager;

    Stroka GetAddress(const ICypressNode& node)
    {
        auto proxy = CypressManager->GetNodeProxy(node.GetId().NodeId, NullTransactionId);
        return proxy->GetParent()->AsMap()->GetChildKey(~proxy);
    }

    void GetAlive(const TGetAttributeParam& param)
    {
        Stroka address = GetAddress(*param.Node);
        bool alive = ChunkManager->FindHolder(address) != NULL;
        BuildYsonFluently(param.Consumer)
            .Scalar(alive);
    }
};

INodeTypeHandler::TPtr CreateHolderTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);

    return New<THolderTypeHandler>(cypressManager, chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

class THolderMapBehavior
    : public TNodeBehaviorBase<TMapNode, TMapNodeProxy>
{
public:
    typedef TNodeBehaviorBase<TMapNode, TMapNodeProxy> TBase;
    typedef THolderMapBehavior TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    THolderMapBehavior(
        const ICypressNode& node,
        TCypressManager* cypressManager,
        TChunkManager* chunkManager)
        : TBase(node, cypressManager)
        , ChunkManager(chunkManager)
    {
        OnRegistered_ = FromMethod(&TThis::OnRegistered, TPtr(this));
        ChunkManager->HolderRegistered().Subscribe(OnRegistered_);
    }

    virtual void Destroy()
    {
        ChunkManager->HolderRegistered().Unsubscribe(OnRegistered_);
    }

private:
    TChunkManager::TPtr ChunkManager;

    IParamAction<const THolder&>::TPtr OnRegistered_;
    
    void OnRegistered(const THolder& holder)
    {
        Stroka address = holder.GetAddress();

        auto node = GetProxy();
        if (~node->FindChild(address) != NULL)
            return;

        // TODO: use fluent
        // TODO: make a single transaction
        // TODO: extract literals

        {
            auto request = TCypressYPathProxy::Create();
            request->SetPath(Sprintf("/%s", ~address));
            request->SetType("holder");     
            request->SetManifest("{}");     
            ExecuteVerb(
                ~IYPathService::FromNode(~node),
                ~request,
                ~CypressManager);
        }

        {
            auto request = TCypressYPathProxy::Create();
            request->SetPath(Sprintf("/%s/orchid", ~address));
            request->SetType("orchid");     
            request->SetManifest(Sprintf("{remote_address=\"%s\"}", ~address));     
            ExecuteVerb(
                ~IYPathService::FromNode(~node),
                ~request,
                ~CypressManager);
        }
    }

};

class THolderMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    typedef THolderMapTypeHandler TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    THolderMapTypeHandler(
        TCypressManager* cypressManager,
        TChunkManager* chunkManager)
        : TMapNodeTypeHandler(cypressManager)
        , CypressManager(cypressManager)
        , ChunkManager(chunkManager)
    {
        // NB: No smartpointer for this here.
        RegisterGetter("alive_holders", FromMethod(&TThis::GetAliveHolders, this));
        RegisterGetter("dead_holders", FromMethod(&TThis::GetDeadHolders, this));
    }

    virtual ERuntimeNodeType GetRuntimeType()
    {
        return ERuntimeNodeType::HolderMap;
    }

    virtual Stroka GetTypeName()
    {
        // TODO: extract type name
        return "holder_map";
    }

    virtual TAutoPtr<ICypressNode> CreateFromManifest(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        NYTree::INode* manifest)
    {
        UNUSED(transactionId);
        UNUSED(manifest);
        return Create(TBranchedNodeId(nodeId, NullTransactionId));
    }

    virtual INodeBehavior::TPtr CreateBehavior(const ICypressNode& node)
    {
        return New<THolderMapBehavior>(node, ~CypressManager, ~ChunkManager);
    }

private:
    TCypressManager::TPtr CypressManager;
    TChunkManager::TPtr ChunkManager;

    void GetAliveHolders(const TGetAttributeParam& param)
    {
        // TODO: use new fluent API
        param.Consumer->OnBeginList();
        FOREACH (auto holderId, ChunkManager->GetHolderIds()) {
            const auto& holder = ChunkManager->GetHolder(holderId);
            param.Consumer->OnListItem();
            param.Consumer->OnStringScalar(holder.GetAddress());
        }
        param.Consumer->OnEndList();
    }

    void GetDeadHolders(const TGetAttributeParam& param)
    {
        // TODO: use new fluent API
        param.Consumer->OnBeginList();
        FOREACH (const auto& pair, param.Node->NameToChild()) {
            Stroka address = pair.First();
            if (ChunkManager->FindHolder(address) == NULL) {
                param.Consumer->OnListItem();
                param.Consumer->OnStringScalar(address);
            }
        }
        param.Consumer->OnEndList();
    }
};

INodeTypeHandler::TPtr CreateHolderMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);

    return New<THolderMapTypeHandler>(cypressManager, chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
