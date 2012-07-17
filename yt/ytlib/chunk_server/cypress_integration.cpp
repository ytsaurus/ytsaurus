#include "stdafx.h"
#include "cypress_integration.h"
#include "holder.h"
#include "holder_statistics.h"

#include <ytlib/actions/bind.h>
#include <ytlib/misc/string.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/cypress/virtual.h>
#include <ytlib/cypress/node_proxy_detail.h>
#include <ytlib/cypress_client/cypress_ypath_proxy.h>
#include <ytlib/chunk_server/chunk_manager.h>
#include <ytlib/chunk_server/holder_authority.h>
#include <ytlib/orchid/cypress_integration.h>
#include <ytlib/cell_master/bootstrap.h>

namespace NYT {
namespace NChunkServer {

using namespace NYTree;
using namespace NCypress;
using namespace NCypressClient;
using namespace NMetaState;
using namespace NOrchid;
using namespace NObjectServer;
using namespace NTransactionServer;
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
        if (Filter == EChunkFilter::All) {
            return true;
        }

        const auto& chunkIds = GetFilteredChunkIds();
        return chunkIds.find(chunkId) != chunkIds.end();
    }

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const
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

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const
    {
        auto id = TChunkId::FromString(key);

        if (TypeFromId(id) != EObjectType::Chunk) {
            return NULL;
        }

        if (!CheckFilter(id)) {
            return NULL;
        }

        return Bootstrap->GetObjectManager()->FindProxy(id);
    }
};

INodeTypeHandlerPtr CreateChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkMap,
        New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::All));
}

INodeTypeHandlerPtr CreateLostChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LostChunkMap,
        New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::Lost));
}

INodeTypeHandlerPtr CreateOverreplicatedChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::OverreplicatedChunkMap,
        New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::Overreplicated));
}

INodeTypeHandlerPtr CreateUnderreplicatedChunkMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::UnderreplicatedChunkMap,
        New<TVirtualChunkMap>(bootstrap, TVirtualChunkMap::EChunkFilter::Underreplicated));
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

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const
    {
        const auto& chunkListIds = Bootstrap->GetChunkManager()->GetChunkListIds(sizeLimit);
        return ConvertToStrings(chunkListIds.begin(), chunkListIds.end(), sizeLimit);
    }

    virtual size_t GetSize() const
    {
        return Bootstrap->GetChunkManager()->GetChunkListCount();
    }

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const
    {
        auto id = TChunkListId::FromString(key);
        if (TypeFromId(id) != EObjectType::ChunkList) {
            return NULL;
        }
        return Bootstrap->GetObjectManager()->FindProxy(id);
    }
};

INodeTypeHandlerPtr CreateChunkListMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ChunkListMap,
        New<TVirtualChunkListMap>(bootstrap));
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
        auto cypressManager = Bootstrap->GetCypressManager();
        auto rootNodeProxy =
            cypressManager->GetVersionedNodeProxy(cypressManager->GetRootNodeId());
        auto holderMap = GetNodeByYPath(rootNodeProxy, "/sys/holders")->AsMap();
        auto holderNode = holderMap->FindChild(address);

        if (!holderNode) {
            // New holder.
            return true;
        }

        bool banned = holderNode->Attributes().Get<bool>("banned", false);
        return !banned;
    }
    
private:
    TBootstrap* Bootstrap;

};

IHolderAuthorityPtr CreateHolderAuthority(TBootstrap* bootstrap)
{
    return New<THolderAuthority>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class THolderProxy
    : public TMapNodeProxy
{
public:
    THolderProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
    { }

private:
    THolder* GetHolder() const
    {
        auto address = GetParent()->AsMap()->GetChildKey(this);
        return Bootstrap->GetChunkManager()->FindHolderByAddress(address);
    }

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        const auto* holder = GetHolder();
        attributes->push_back(TAttributeInfo("state"));
        attributes->push_back(TAttributeInfo("confirmed", holder));
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

        if (name == "state") {
            auto state = holder ? holder->GetState() : EHolderState(EHolderState::Offline);
            BuildYsonFluently(consumer)
                .Scalar(FormatEnum(state));
            return true;
        }

        if (holder) {
            if (name == "confirmed") {
                BuildYsonFluently(consumer)
                    .Scalar(FormatBool(Bootstrap->GetChunkManager()->IsHolderConfirmed(holder)));
                return true;
            }

            if (name == "incarnation_id") {
                BuildYsonFluently(consumer)
                    .Scalar(holder->GetIncarnationId());
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

    virtual void OnUpdateAttribute(
        const Stroka& key,
        const TNullable<NYTree::TYsonString>& oldValue,
        const TNullable<NYTree::TYsonString>& newValue)
    {
        UNUSED(oldValue);
        if (key == "banned") {
            if (newValue) {
                ConvertTo<bool>(*newValue);
            }
        }
    }
};

class THolderTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit THolderTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType()
    {
        return EObjectType::Holder;
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const TNodeId& nodeId,
        TTransaction* transaction)
    {
        return New<THolderProxy>(
            this,
            Bootstrap,
            transaction,
            nodeId);
    }
};

INodeTypeHandlerPtr CreateHolderTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return New<THolderTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

class THolderMapBehavior
    : public TNodeBehaviorBase<TMapNode, TMapNodeProxy>
{
public:
    THolderMapBehavior(TBootstrap* bootstrap, const TNodeId& nodeId)
        : TNodeBehaviorBase<TMapNode, TMapNodeProxy>(bootstrap, nodeId)
    {
        bootstrap->GetChunkManager()->SubscribeHolderRegistered(BIND(
            &THolderMapBehavior::OnRegistered,
            MakeWeak(this)));
    }

private:
    void OnRegistered(const THolder* holder)
    {
        Stroka address = holder->GetAddress();
        auto node = GetProxy();

        auto cypressManager = Bootstrap->GetCypressManager();

        // We're already in the state thread but need to postpone the planned changes and enqueue a callback.
        // Doing otherwise will turn holder registration and Cypress update into a single
        // logged change, which is undesirable.
        BIND([=] () {
            if (node->FindChild(address))
                return;

            auto service = cypressManager->GetVersionedNodeProxy(NodeId);

            // TODO(babenko): make a single transaction
            // TODO(babenko): check for errors and retry

            {
                auto req = TCypressYPathProxy::Create("/" + EscapeYPathToken(address));
                req->set_type(EObjectType::Holder);
                ExecuteVerb(service, req);
            }

            {
                auto req = TCypressYPathProxy::Create("/" + EscapeYPathToken(address) + "/orchid");
                req->set_type(EObjectType::Orchid);
                req->Attributes().Set<Stroka>("remote_address", address);
                ExecuteVerb(service, req);
            }
        })
        .Via(
            Bootstrap->GetStateInvoker(),
            Bootstrap->GetMetaStateManager()->GetEpochContext())
        .Run();
    }

};

class THolderMapProxy
    : public TMapNodeProxy
{
public:
    THolderMapProxy(
        INodeTypeHandlerPtr typeHandler,
        TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId)
        : TMapNodeProxy(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
    { }

private:
    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        attributes->push_back("offline");
        attributes->push_back("registered");
        attributes->push_back("online");
        attributes->push_back("unconfirmed");
        attributes->push_back("confirmed");
        attributes->push_back("available_space");
        attributes->push_back("used_space");
        attributes->push_back("chunk_count");
        attributes->push_back("session_count");
        attributes->push_back("online_holder_count");
        attributes->push_back("chunk_replicator_enabled");
        TMapNodeProxy::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        auto chunkManager = Bootstrap->GetChunkManager();

        if (name == "offline") {
            BuildYsonFluently(consumer)
                .DoListFor(GetKeys(), [=] (TFluentList fluent, Stroka address) {
                    if (!chunkManager->FindHolderByAddress(address)) {
                        fluent.Item().Scalar(address);
                    }
            });
            return true;
        }

        if (name == "registered" || name == "online") {
            auto state = name == "registered" ? EHolderState::Registered : EHolderState::Online;
            BuildYsonFluently(consumer)
                .DoListFor(chunkManager->GetHolders(), [=] (TFluentList fluent, THolder* holder) {
                    if (holder->GetState() == state) {
                        fluent.Item().Scalar(holder->GetAddress());
                    }
                });
            return true;
        }

        if (name == "unconfirmed" || name == "confirmed") {
            bool state = name == "confirmed";
            BuildYsonFluently(consumer)
                .DoListFor(chunkManager->GetHolders(), [=] (TFluentList fluent, THolder* holder) {
                    if (chunkManager->IsHolderConfirmed(holder) == state) {
                        fluent.Item().Scalar(holder->GetAddress());
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

        if (name == "online_holder_count") {
            BuildYsonFluently(consumer)
                .Scalar(statistics.OnlineHolderCount);
            return true;
        }

        if (name == "chunk_replicator_enabled") {
            BuildYsonFluently(consumer)
                .Scalar(chunkManager->IsReplicatorEnabled());
            return true;
        }

        return TMapNodeProxy::GetSystemAttribute(name, consumer);
    }
};

class THolderMapTypeHandler
    : public TMapNodeTypeHandler
{
public:
    explicit THolderMapTypeHandler(TBootstrap* bootstrap)
        : TMapNodeTypeHandler(bootstrap)
    { }

    virtual EObjectType GetObjectType()
    {
        return EObjectType::HolderMap;
    }
    
    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        const TNodeId& nodeId,
        TTransaction* transaction)
    {
        return New<THolderMapProxy>(
            this,
            Bootstrap,
            transaction,
            nodeId);
    }

    virtual INodeBehaviorPtr CreateBehavior(const TNodeId& nodeId)
    {
        return New<THolderMapBehavior>(Bootstrap, nodeId);
    }
};

INodeTypeHandlerPtr CreateHolderMapTypeHandler(TBootstrap* bootstrap)
{
    YASSERT(bootstrap);

    return New<THolderMapTypeHandler>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
