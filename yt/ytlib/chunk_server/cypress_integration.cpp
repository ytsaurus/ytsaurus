#include "stdafx.h"
#include "cypress_integration.h"

#include "../cypress/virtual.h"
#include "../cypress/node_proxy_detail.h"
#include "../ytree/virtual.h"
#include "../ytree/fluent.h"
#include "../ytree/ypath_rpc.h"
#include "../misc/string.h"

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
                        .Item("size").Scalar(chunk->GetSize())
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

class THolderMapNode
    : public TMapNode
{
public:
    explicit THolderMapNode(const TBranchedNodeId& id)
        : TMapNode(id)
    { }

    THolderMapNode(const TBranchedNodeId& id, const TMapNode& other)
        : TMapNode(id, other)
    { }

    virtual TAutoPtr<ICypressNode> Clone() const
    {
        return new THolderMapNode(Id, *this);
    }

    ERuntimeNodeType GetRuntimeType() const
    {
        return ERuntimeNodeType::HolderMap;
    }
};

class THolderMapBehavior
    : public TNodeBehaviorBase<THolderMapNode, TMapNodeProxy>
{
public:
    typedef TNodeBehaviorBase<THolderMapNode, TMapNodeProxy> TBase;
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
        auto request = TYPathProxy::Set();
        request->SetValue("{}");
        
        TYPath path = "/" + holder.GetAddress();

        ExecuteVerb(
            ~IYPathService::FromNode(~GetProxy()),
            path,
            ~request,
            ~CypressManager);
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

        return new THolderMapNode(TBranchedNodeId(nodeId, NullTransactionId));
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
            param.Consumer->OnStringScalar(holder.GetAddress(), false);
        }
        param.Consumer->OnEndList(false);
    }

};

INodeTypeHandler::TPtr CreateHolderMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager != NULL);
    YASSERT(chunkManager != NULL);

    return New<THolderMapTypeHandler>(
        cypressManager,
        chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
