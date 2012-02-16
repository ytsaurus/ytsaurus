#include "stdafx.h"
#include "cypress_integration.h"

#include <ytlib/misc/string.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/cypress/virtual.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/orchid/cypress_integration.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NCypress;
using namespace NMetaState;
using namespace NOrchid;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkMap
    : public TVirtualMapBase
{
public:
    DECLARE_ENUM(EChunkFilter,
        (All)
        (Lost)
        (Overreplicated)
        (Underreplicated)
    );

    TVirtualChunkMap(TChunkManager* chunkManager, EChunkFilter filter)
        : ChunkManager(chunkManager)
        , Filter(filter)
    { }

private:
    TChunkManager::TPtr ChunkManager;
    EChunkFilter Filter;

    const yhash_set<TChunkId>& GetFilteredChunkIds() const
    {
        switch (Filter) {
            case EChunkFilter::Lost:
                return ChunkManager->LostChunkIds();
            case EChunkFilter::Overreplicated:
                return ChunkManager->OverreplicatedChunkIds();
            case EChunkFilter::Underreplicated:
                return ChunkManager->UnderreplicatedChunkIds();
            default:
                YUNREACHABLE();
        }
    }

    bool CheckFilter(const TChunkId& chunkId) const
    {
        if (Filter == EChunkFilter::All)
            return true;

        const auto& chunkIds = GetFilteredChunkIds();
        return chunkIds.find(chunkId) != chunkIds.end();
    }

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        if (Filter == EChunkFilter::All) {
            const auto& chunkIds = ChunkManager->GetChunkIds(sizeLimit);
            return ConvertToStrings(chunkIds.begin(), chunkIds.end(), sizeLimit);
        } else {
            const auto& chunkIds = GetFilteredChunkIds();
            return ConvertToStrings(chunkIds.begin(), chunkIds.end(), sizeLimit);
        }
    }

    virtual size_t GetSize() const
    {
        if (Filter == EChunkFilter::All) {
            return ChunkManager->GetChunkCount();
        } else {
            return GetFilteredChunkIds().size();
        }
    }

    virtual TYPathServicePtr GetItemService(const Stroka& key) const
    {
        auto id = TChunkId::FromString(key);

        if (!CheckFilter(id)) {
            return NULL;
        }

        return ChunkManager->GetObjectManager()->FindProxy(id);
    }
};

INodeTypeHandler::TPtr CreateChunkMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager);
    YASSERT(chunkManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::ChunkMap,
        ~New<TVirtualChunkMap>(chunkManager, TVirtualChunkMap::EChunkFilter::All));
}

INodeTypeHandler::TPtr CreateLostChunkMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager);
    YASSERT(chunkManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::LostChunkMap,
        ~New<TVirtualChunkMap>(chunkManager, TVirtualChunkMap::EChunkFilter::Lost));
}

INodeTypeHandler::TPtr CreateOverreplicatedChunkMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager);
    YASSERT(chunkManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::OverreplicatedChunkMap,
        ~New<TVirtualChunkMap>(chunkManager, TVirtualChunkMap::EChunkFilter::Overreplicated));
}

INodeTypeHandler::TPtr CreateUnderreplicatedChunkMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager);
    YASSERT(chunkManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::UnderreplicatedChunkMap,
        ~New<TVirtualChunkMap>(chunkManager, TVirtualChunkMap::EChunkFilter::Underreplicated));
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

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& chunkListIds = ChunkManager->GetChunkListIds(sizeLimit);
        return ConvertToStrings(chunkListIds.begin(), chunkListIds.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return ChunkManager->GetChunkListCount();
    }

    virtual TYPathServicePtr GetItemService(const Stroka& key) const
    {
        auto id = TChunkListId::FromString(key);
        return ChunkManager->GetObjectManager()->GetProxy(id);
    }
};

INodeTypeHandler::TPtr CreateChunkListMapTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager);
    YASSERT(chunkManager);

    return CreateVirtualTypeHandler(
        cypressManager,
        EObjectType::ChunkListMap,
        ~New<TVirtualChunkListMap>(chunkManager));
}

////////////////////////////////////////////////////////////////////////////////

class THolderAuthority
    : public IHolderAuthority
{
public:
    typedef TIntrusivePtr<THolderAuthority> TPtr;

    THolderAuthority(TCypressManager* cypressManager)
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

IHolderAuthority::TPtr CreateHolderAuthority(
    TCypressManager* cypressManager)
{
    return New<THolderAuthority>(cypressManager);
}

////////////////////////////////////////////////////////////////////////////////

class THolderProxy
    : public TMapNodeProxy
{
public:
    THolderProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        TChunkManager* chunkManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TMapNodeProxy(
            typeHandler,
            cypressManager,
            transactionId,
            nodeId)
        , ChunkManager(chunkManager)
    { }

private:
    TChunkManager::TPtr ChunkManager;

    const THolder* GetHolder() const
    {
        auto address = GetParent()->AsMap()->GetChildKey(this);
        return ChunkManager->FindHolder(address);
    }

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        const auto* holder = GetHolder();
        attributes->push_back("alive");
        attributes->push_back(TAttributeInfo("incarnation_id", holder));
        attributes->push_back(TAttributeInfo("available_space", holder));
        attributes->push_back(TAttributeInfo("used_space", holder));
        attributes->push_back(TAttributeInfo("chunk_count", holder));
        attributes->push_back(TAttributeInfo("session_count", holder));
        attributes->push_back(TAttributeInfo("full", holder));
        TMapNodeProxy::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto* holder = GetHolder();

        if (name == "alive") {
            BuildYsonFluently(consumer)
                .Scalar(holder != NULL);
            return true;
        }

        if (holder) {
            if (name == "incarnation_id") {
                BuildYsonFluently(consumer)
                    .Scalar(holder->GetIncarnationId().ToString());
                return true;
            }

            const auto& statistics = holder->Statistics();
            if (name == "available_space") {
                BuildYsonFluently(consumer)
                    .Scalar(statistics.available_space());
                return true;
            }
            if (name == "used_space") {
                BuildYsonFluently(consumer)
                    .Scalar(statistics.used_space());
                return true;
            }
            if (name == "chunk_count") {
                BuildYsonFluently(consumer)
                    .Scalar(statistics.chunk_count());
                return true;
            }
            if (name == "session_count") {
                BuildYsonFluently(consumer)
                    .Scalar(statistics.session_count());
                return true;
            }
            if (name == "full") {
                BuildYsonFluently(consumer)
                    .Scalar(statistics.full());
                return true;
            }
        }

        return TMapNodeProxy::GetSystemAttribute(name, consumer);
    }
};

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
    { }

    virtual EObjectType GetObjectType()
    {
        return EObjectType::Holder;
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(const TVersionedNodeId& id)
    {
        return New<THolderProxy>(
            this,
            ~CypressManager,
            ~ChunkManager,
            id.TransactionId,
            id.ObjectId);
    }

private:
    TCypressManager::TPtr CypressManager;
    TChunkManager::TPtr ChunkManager;

};

INodeTypeHandler::TPtr CreateHolderTypeHandler(
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager);
    YASSERT(chunkManager);

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
        const TNodeId& nodeId,
        IMetaStateManager* metaStateManager,
        TCypressManager* cypressManager,
        TChunkManager* chunkManager)
        : TBase(nodeId, cypressManager)
        , ChunkManager(chunkManager)
    {
        ChunkManager->HolderRegistered().Subscribe(
            // TODO(babenko): use AsWeak
            FromMethod(&TThis::OnRegistered, TWeakPtr<THolderMapBehavior>(this))
            ->Via(metaStateManager->GetEpochStateInvoker()));
    }

private:
    TChunkManager::TPtr ChunkManager;
    
    void OnRegistered(const THolder& holder)
    {
        Stroka address = holder.GetAddress();

        auto node = GetProxy();
        if (node->FindChild(address))
            return;

        auto service = CypressManager->GetVersionedNodeProxy(NodeId, NullTransactionId);

        // TODO(babenko): make a single transaction

        {
            auto req = TCypressYPathProxy::Create(address);
            req->set_type(EObjectType::Holder);
            ExecuteVerb(~service, ~req);
        }

        {
            auto req = TCypressYPathProxy::Create(CombineYPaths(address, "orchid"));
            req->set_type(EObjectType::Orchid);     
            auto manifest = New<TOrchidManifest>();
            manifest->RemoteAddress = address;
            req->set_manifest(SerializeToYson(~manifest));
            ExecuteVerb(~service, ~req);
        }
    }

};

class THolderMapProxy
    : public TMapNodeProxy
{
public:
    THolderMapProxy(
        INodeTypeHandler* typeHandler,
        TCypressManager* cypressManager,
        TChunkManager* chunkManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TMapNodeProxy(
        typeHandler,
        cypressManager,
        transactionId,
        nodeId)
        , ChunkManager(chunkManager)
    { }

private:
    TChunkManager::TPtr ChunkManager;

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        attributes->push_back("alive");
        attributes->push_back("dead");
        attributes->push_back("available_space");
        attributes->push_back("used_space");
        attributes->push_back("chunk_count");
        attributes->push_back("session_count");
        attributes->push_back("alive_holder_count");
        TMapNodeProxy::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        if (name == "alive") {
            BuildYsonFluently(consumer)
                .DoListFor(ChunkManager->GetHolderIds(), [=] (TFluentList fluent, THolderId id)
                    {
                        const auto& holder = ChunkManager->GetHolder(id);
                        fluent.Item().Scalar(holder.GetAddress());
                    });
            return true;
        }

        if (name == "dead") {
            BuildYsonFluently(consumer)
                .DoListFor(GetKeys(), [=] (TFluentList fluent, Stroka address)
                    {
                        if (!ChunkManager->FindHolder(address)) {
                            fluent.Item().Scalar(address);
                        }
                    });
            return true;
        }

        auto statistics = ChunkManager->GetTotalHolderStatistics();
        if (name == "available_space") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.AvailbaleSpace);
            return true;
        }

        if (name == "used_space") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.UsedSpace);
            return true;
        }

        if (name == "chunk_count") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.ChunkCount);
            return true;
        }

        if (name == "session_count") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.SessionCount);
            return true;
        }

        if (name == "alive_holder_count") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.AliveHolderCount);
            return true;
        }

        return TMapNodeProxy::GetSystemAttribute(name, consumer);
    }
};

class THolderMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    typedef THolderMapTypeHandler TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    THolderMapTypeHandler(
        IMetaStateManager* metaStateManager,
        TCypressManager* cypressManager,
        TChunkManager* chunkManager)
        : TMapNodeTypeHandler(cypressManager)
        , MetaStateManager(metaStateManager)
        , CypressManager(cypressManager)
        , ChunkManager(chunkManager)
    { }

    virtual EObjectType GetObjectType()
    {
        return EObjectType::HolderMap;
    }
    
    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(const TVersionedNodeId& id)
    {
        return New<THolderMapProxy>(
            this,
            ~CypressManager,
            ~ChunkManager,
            id.TransactionId,
            id.ObjectId);
    }

    virtual INodeBehavior::TPtr CreateBehavior(const TNodeId& nodeId)
    {
        return New<THolderMapBehavior>(
            nodeId,
            ~MetaStateManager,
            ~CypressManager,
            ~ChunkManager);
    }

private:
    IMetaStateManager::TPtr MetaStateManager;
    TCypressManager::TPtr CypressManager;
    TChunkManager::TPtr ChunkManager;

};

INodeTypeHandler::TPtr CreateHolderMapTypeHandler(
    IMetaStateManager* metaStateManager,
    TCypressManager* cypressManager,
    TChunkManager* chunkManager)
{
    YASSERT(cypressManager);
    YASSERT(chunkManager);

    return New<THolderMapTypeHandler>(
        metaStateManager,
        cypressManager,
        chunkManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
