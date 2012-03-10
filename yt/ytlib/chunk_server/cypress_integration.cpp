#include "stdafx.h"
#include "cypress_integration.h"

#include <ytlib/actions/bind.h>
#include <ytlib/misc/string.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/cypress/virtual.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/orchid/cypress_integration.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NCypress;
using namespace NMetaState;
using namespace NOrchid;
using namespace NObjectServer;
using namespace NCellMaster;

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

    TVirtualChunkMap(TBootstrap* bootstrap, EChunkFilter filter)
        : Bootstrap(bootstrap)
        , Filter(filter)
    { }

private:
    TBootstrap* Bootstrap;
    EChunkFilter Filter;

    const yhash_set<TChunkId>& GetFilteredChunkIds() const
    {
        auto chunkManager = Bootstrap->GetChunkManager();
        switch (Filter) {
            case EChunkFilter::Lost:
                return chunkManager->LostChunkIds();
            case EChunkFilter::Overreplicated:
                return chunkManager->OverreplicatedChunkIds();
            case EChunkFilter::Underreplicated:
                return chunkManager->UnderreplicatedChunkIds();
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
            const auto& chunkIds = Bootstrap->GetChunkManager()->GetChunkIds(sizeLimit);
            return ConvertToStrings(chunkIds.begin(), chunkIds.end(), sizeLimit);
        } else {
            const auto& chunkIds = GetFilteredChunkIds();
            return ConvertToStrings(chunkIds.begin(), chunkIds.end(), sizeLimit);
        }
    }

    virtual size_t GetSize() const
    {
        if (Filter == EChunkFilter::All) {
            return Bootstrap->GetChunkManager()->GetChunkCount();
        } else {
            return GetFilteredChunkIds().size();
        }
    }

    virtual IYPathServicePtr GetItemService(const Stroka& key) const
    {
        auto id = TChunkId::FromString(key);

        if (!CheckFilter(id)) {
            return NULL;
        }

        return Bootstrap->GetObjectManager()->FindProxy(id);
    }
};

INodeTypeHandler::TPtr CreateChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkMap,
        ~New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::All));
}

INodeTypeHandler::TPtr CreateLostChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LostChunkMap,
        ~New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::Lost));
}

INodeTypeHandler::TPtr CreateOverreplicatedChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::OverreplicatedChunkMap,
        ~New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::Overreplicated));
}

INodeTypeHandler::TPtr CreateUnderreplicatedChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::UnderreplicatedChunkMap,
        ~New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::Underreplicated));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualChunkListMap
    : public TVirtualMapBase
{
public:
    TVirtualChunkListMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& chunkListIds = Bootstrap->GetChunkManager()->GetChunkListIds(sizeLimit);
        return ConvertToStrings(chunkListIds.begin(), chunkListIds.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return Bootstrap->GetChunkManager()->GetChunkListCount();
    }

    virtual IYPathServicePtr GetItemService(const Stroka& key) const
    {
        auto id = TChunkListId::FromString(key);
        return Bootstrap->GetObjectManager()->GetProxy(id);
    }
};

INodeTypeHandler::TPtr CreateChunkListMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkListMap,
        ~New<TVirtualChunkListMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

class THolderAuthority
    : public IHolderAuthority
{
public:
    typedef TIntrusivePtr<THolderAuthority> TPtr;

    THolderAuthority(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

    virtual bool IsHolderAuthorized(const Stroka& address)
    {
        UNUSED(address);
        return true;
    }
    
private:
    TBootstrap* Bootstrap;

};

IHolderAuthority::TPtr CreateHolderAuthority(TBootstrap* bootstrap)
{
    return New<THolderAuthority>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class THolderProxy
    : public TMapNodeProxy
{
public:
    THolderProxy(
        INodeTypeHandler* typeHandler,
        TBootstrap* bootstrap,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transactionId,
            nodeId)
    { }

private:
    const THolder* GetHolder() const
    {
        auto address = GetParent()->AsMap()->GetChildKey(this);
        return Bootstrap->GetChunkManager()->FindHolder(address);
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

    THolderTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType()
    {
        return EObjectType::Holder;
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(const TVersionedNodeId& id)
    {
        return New<THolderProxy>(
            this,
            Bootstrap,
            id.TransactionId,
            id.ObjectId);
    }
};

INodeTypeHandler::TPtr CreateHolderTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return New<THolderTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class THolderMapBehavior
    : public TNodeBehaviorBase<TMapNode, TMapNodeProxy>
{
public:
    typedef TNodeBehaviorBase<TMapNode, TMapNodeProxy> TBase;
    typedef THolderMapBehavior TThis;
    typedef TIntrusivePtr<TThis> TPtr;

    THolderMapBehavior(TBootstrap* bootstrap, const TNodeId& nodeId)
        : TBase(bootstrap, nodeId)
    {
        // TODO(babenko): use AsWeak
        bootstrap->GetChunkManager()->SubscribeHolderRegistered(Bind(
            &TThis::OnRegistered,
            TWeakPtr<THolderMapBehavior>(this)));
    }

private:
    void OnRegistered(const THolder& holder)
    {
        Stroka address = holder.GetAddress();
        auto node = GetProxy();

        auto cypressManager = Bootstrap->GetCypressManager();

        // We're already in the state thread but need to postpone the planned changes and enqueue a callback.
        // Doing otherwise will turn holder registration and Cypress update into a single
        // logged change, which is undesirable.
        Bootstrap
            ->GetMetaStateManager()
            ->GetEpochContext()
            ->CreateInvoker(~Bootstrap->GetStateInvoker())
            ->Invoke(FromFunctor([=] ()
                {
                    if (node->FindChild(address))
                        return;

                    auto service = cypressManager->GetVersionedNodeProxy(NodeId, NullTransactionId);

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
                }));
    }

};

class THolderMapProxy
    : public TMapNodeProxy
{
public:
    THolderMapProxy(
        INodeTypeHandler* typeHandler,
        TBootstrap* bootstrap,
        const TTransactionId& transactionId,
        const TNodeId& nodeId)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transactionId,
            nodeId)
    { }

private:
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
        auto chunkManager = Bootstrap->GetChunkManager();

        if (name == "alive") {
            BuildYsonFluently(consumer)
                .DoListFor(chunkManager->GetHolderIds(), [=] (TFluentList fluent, THolderId id)
                    {
                        const auto& holder = chunkManager->GetHolder(id);
                        fluent.Item().Scalar(holder.GetAddress());
                    });
            return true;
        }

        if (name == "dead") {
            BuildYsonFluently(consumer)
                .DoListFor(GetKeys(), [=] (TFluentList fluent, Stroka address)
                    {
                        if (!chunkManager->FindHolder(address)) {
                            fluent.Item().Scalar(address);
                        }
                    });
            return true;
        }

        auto statistics = chunkManager->GetTotalHolderStatistics();
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

    THolderMapTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType()
    {
        return EObjectType::HolderMap;
    }
    
    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(const TVersionedNodeId& id)
    {
        return New<THolderMapProxy>(
            this,
            Bootstrap,
            id.TransactionId,
            id.ObjectId);
    }

    virtual INodeBehavior::TPtr CreateBehavior(const TNodeId& nodeId)
    {
        return New<THolderMapBehavior>(Bootstrap, nodeId);
    }
};

INodeTypeHandler::TPtr CreateHolderMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return New<THolderMapTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
