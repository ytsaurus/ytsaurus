#pragma once

#include "node_proxy.h"
#include "node_detail.h"

#include <ytlib/cypress/cypress_ypath.pb.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ypath_format.h>
#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/node_detail.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/object_server/object_detail.h>
#include <ytlib/object_server/id.h>
#include <ytlib/cell_master/public.h>
#include <ytlib/transaction_server/transaction.h>

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public NYTree::INodeFactory
{
public:
    TNodeFactory(
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction);
    ~TNodeFactory();

    virtual NYTree::IStringNodePtr CreateString();
    virtual NYTree::IIntegerNodePtr CreateInteger();
    virtual NYTree::IDoubleNodePtr CreateDouble();
    virtual NYTree::IMapNodePtr CreateMap();
    virtual NYTree::IListNodePtr CreateList();
    virtual NYTree::IEntityNodePtr CreateEntity();

private:
    NCellMaster::TBootstrap* Bootstrap;
    NTransactionServer::TTransaction* Transaction;
    yvector<TNodeId> CreatedNodeIds;

    ICypressNodeProxy::TPtr DoCreate(EObjectType type);

};

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCypressNodeProxyBase
    : public NYTree::TNodeBase
    , public NObjectServer::TObjectProxyBase
    , public ICypressNodeProxy
    , public virtual IBase
{
public:
    typedef TIntrusivePtr<TCypressNodeProxyBase> TPtr;

    TCypressNodeProxyBase(
        INodeTypeHandler* typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId)
        : NObjectServer::TObjectProxyBase(bootstrap, nodeId)
        , TypeHandler(typeHandler)
        , Bootstrap(bootstrap)
        , Transaction(transaction)
        , NodeId(nodeId)
    {
        YASSERT(typeHandler);
        YASSERT(bootstrap);

        Logger = NLog::TLogger("Cypress");
    }

    NYTree::INodeFactoryPtr CreateFactory() const
    {
        return New<TNodeFactory>(Bootstrap, Transaction);
    }

    virtual TTransactionId GetTransactionId() const
    {
        return NObjectServer::GetObjectId(Transaction);
    }

    virtual TNodeId GetId() const
    {
        return NodeId;
    }


    virtual NYTree::ENodeType GetType() const
    {
        return TypeHandler->GetNodeType();
    }


    virtual const ICypressNode& GetImpl() const
    {
        return this->GetTypedImpl();
    }

    virtual ICypressNode& GetImplForUpdate()
    {
        return this->GetTypedImplForUpdate();
    }


    virtual NYTree::ICompositeNodePtr GetParent() const
    {
        auto nodeId = GetImpl().GetParentId();
        return nodeId == NullObjectId ? NULL : GetProxy(nodeId)->AsComposite();
    }

    virtual void SetParent(NYTree::ICompositeNode* parent)
    {
        GetImplForUpdate().SetParentId(
            parent
            ? ToProxy(parent)->GetId()
            : NullObjectId);
    }


    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Lock);
        // NB: Create is not considered a write verb since it always fails here.
        return NYTree::TNodeBase::IsWriteRequest(context);
    }

    virtual NYTree::IAttributeDictionary& Attributes()
    {
        return NObjectServer::TObjectProxyBase::Attributes();
    }

protected:
    INodeTypeHandler::TPtr TypeHandler;
    NCellMaster::TBootstrap* Bootstrap;
    NTransactionServer::TTransaction* Transaction;
    TNodeId NodeId;


    virtual NObjectServer::TVersionedObjectId GetVersionedId() const
    {
        return TVersionedObjectId(NodeId, NObjectServer::GetObjectId(Transaction));
    }


    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes)
    {
        attributes->push_back("parent_id");
        attributes->push_back("lock_ids");
        attributes->push_back(TAttributeInfo("lock_mode", Transaction));
        NObjectServer::TObjectProxyBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        const auto& node = GetImpl();
        // NB: Locks are only valid for originating nodes.
        const auto& origniatingNode = Bootstrap->GetCypressManager()->GetNode(Id);

        if (name == "parent_id") {
            BuildYsonFluently(consumer)
                .Scalar(node.GetParentId().ToString());
            return true;
        }

        if (name == "lock_ids") {
            BuildYsonFluently(consumer)
                .DoListFor(origniatingNode.Locks(), [=] (NYTree::TFluentList fluent, TLock* lock) {
                    fluent.Item().Scalar(lock->GetId().ToString());
                });
            return true;
        }

        if (Transaction) {
            if (name == "lock_mode") {
                BuildYsonFluently(consumer)
                    .Scalar(FormatEnum(node.GetLockMode()));
                return true;
            }
        }

        return NObjectServer::TObjectProxyBase::GetSystemAttribute(name, consumer);
    }


    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(GetId);
        DISPATCH_YPATH_SERVICE_METHOD(Lock);
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        TNodeBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Lock)
    {
        auto mode = ELockMode(request->mode());

        context->SetRequestInfo("Mode: %s", ~mode.ToString());
        if (mode != ELockMode::Snapshot &&
            mode != ELockMode::Shared &&
            mode != ELockMode::Exclusive)
        {
            ythrow yexception() << Sprintf("Invalid lock mode %s",
                ~CamelCaseToUnderscoreCase(mode.ToString()).Quote());
        }

        auto lockId = Bootstrap->GetCypressManager()->LockVersionedNode(NodeId, Transaction, mode);

        *response->mutable_lock_id() = lockId.ToProto();

        context->SetResponseInfo("LockId: %s", ~lockId.ToString());

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Create)
    {
        UNUSED(request);
        UNUSED(response);

        NYTree::TTokenizer tokenizer(context->GetPath());
        if (!tokenizer.ParseNext()) {
            ythrow yexception() << "Node already exists";
        }

        context->Reply(NRpc::EErrorCode::NoSuchVerb, "Verb is not supported");
    }


    const ICypressNode& GetImpl(const TNodeId& nodeId) const
    {
        return Bootstrap->GetCypressManager()->GetVersionedNode(nodeId, Transaction);
    }

    ICypressNode& GetImplForUpdate(const TNodeId& nodeId, ELockMode requestedMode = ELockMode::Exclusive)
    {
        return Bootstrap->GetCypressManager()->GetVersionedNodeForUpdate(nodeId, Transaction, requestedMode);
    }


    const TImpl& GetTypedImpl() const
    {
        return static_cast<const TImpl&>(GetImpl(NodeId));
    }

    TImpl& GetTypedImplForUpdate(ELockMode requestedMode = ELockMode::Exclusive)
    {
        return static_cast<TImpl&>(GetImplForUpdate(NodeId, requestedMode));
    }


    ICypressNodeProxy::TPtr GetProxy(const TNodeId& nodeId) const
    {
        YASSERT(nodeId != NullObjectId);
        return Bootstrap->GetCypressManager()->GetVersionedNodeProxy(nodeId, Transaction);
    }

    static ICypressNodeProxy* ToProxy(INode* node)
    {
        YASSERT(node);
        return &dynamic_cast<ICypressNodeProxy&>(*node);
    }

    static const ICypressNodeProxy* ToProxy(const INode* node)
    {
        YASSERT(node);
        return &dynamic_cast<const ICypressNodeProxy&>(*node);
    }


    void AttachChild(ICypressNode& child)
    {
        child.SetParentId(NodeId);
        Bootstrap->GetObjectManager()->RefObject(child.GetId().ObjectId);
    }

    void DetachChild(ICypressNode& child)
    {
        child.SetParentId(NullObjectId);
        Bootstrap->GetObjectManager()->UnrefObject(child.GetId().ObjectId);
    }

    virtual TAutoPtr<NYTree::IAttributeDictionary> DoCreateUserAttributes()
    {
        return new TVersionedUserAttributeDictionary(
            NodeId,
            Transaction,
            Bootstrap);
    }

    class TVersionedUserAttributeDictionary
        : public NObjectServer::TObjectProxyBase::TUserAttributeDictionary
    {
    public:
        TVersionedUserAttributeDictionary(
            TObjectId objectId,
            NTransactionServer::TTransaction* transaction,
            NCellMaster::TBootstrap* bootstrap)
            : TUserAttributeDictionary(
                ~bootstrap->GetObjectManager(),
                objectId)
            , Transaction(transaction)
            , Bootstrap(bootstrap)
        { }
           
        
        virtual yhash_set<Stroka> List() const
        {
            if (Transaction == NULL) {
                return TUserAttributeDictionary::List();
            }

            yhash_set<Stroka> attributes;
            auto transactions = Bootstrap->GetTransactionManager()->GetTransactionPath(Transaction);
            auto objectManager = Bootstrap->GetObjectManager();
            for (auto it = transactions.rbegin(); it != transactions.rend(); ++it) {
                TVersionedObjectId parentId(ObjectId, NObjectServer::GetObjectId(Transaction));
                const auto* userAttributes = objectManager->FindAttributes(parentId);
                if (userAttributes) {
                    FOREACH (const auto& pair, userAttributes->Attributes()) {
                        if (pair.second.empty()) {
                            attributes.erase(pair.first);
                        } else {
                            attributes.insert(pair.first);
                        }
                    }
                }
            }
            return attributes;
        }

        virtual TNullable<NYTree::TYson> FindYson(const Stroka& name) const
        {
            if (Transaction == NULL) {
                return TUserAttributeDictionary::FindYson(name);
            }

            auto transactions = Bootstrap->GetTransactionManager()->GetTransactionPath(Transaction);
            auto objectManager = Bootstrap->GetObjectManager();
            FOREACH (const auto& transaction, transactions) {
                TVersionedObjectId parentId(ObjectId, NObjectServer::GetObjectId(transaction));
                const auto* userAttributes = objectManager->FindAttributes(parentId);
                if (userAttributes) {
                    auto it = userAttributes->Attributes().find(name);
                    if (it != userAttributes->Attributes().end()) {
                        if (it->second.empty()) {
                            break;
                        } else {
                            return it->second;
                        }
                    }
                }
            }
            return Null;
        }

        virtual void SetYson(const Stroka& name, const NYTree::TYson& value)
        {
            // This takes the lock.
            Bootstrap
                ->GetCypressManager()
                ->GetVersionedNodeForUpdate(ObjectId, Transaction);

            TUserAttributeDictionary::SetYson(name, value);
        }

        virtual bool Remove(const Stroka& name)
        {
            // This takes the lock.
            auto id = Bootstrap
                ->GetCypressManager()
                ->GetVersionedNodeForUpdate(ObjectId, Transaction)
                .GetId();

            if (Transaction == NULL) {
                return TUserAttributeDictionary::Remove(name);
            }

            bool contains = false;
            auto transactions = Bootstrap->GetTransactionManager()->GetTransactionPath(Transaction);
            auto objectManager = Bootstrap->GetObjectManager();
            for (auto it = transactions.rbegin() + 1; it != transactions.rend(); ++it) {
                TVersionedObjectId parentId(ObjectId, NObjectServer::GetObjectId(*it));
                const auto* userAttributes = objectManager->FindAttributes(parentId);
                if (userAttributes) {
                    auto it = userAttributes->Attributes().find(name);
                    if (it != userAttributes->Attributes().end()) {
                        if (!it->second.empty()) {
                            contains = true;
                        }
                        break;
                    }
                }
            }

            auto* userAttributes = objectManager->FindAttributes(id);
            if (contains) {
                if (!userAttributes) {
                    userAttributes = objectManager->CreateAttributes(id);
                }
                userAttributes->Attributes()[name] = "";
                return true;
            } else {
                if (!userAttributes) {
                    return false;
                }
                return userAttributes->Attributes().erase(name) > 0;
            }
        }
    protected:
        NTransactionServer::TTransaction* Transaction;
        NCellMaster::TBootstrap* Bootstrap;
    };

};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<IBase, TImpl>
{
public:
    TScalarNodeProxy(
        INodeTypeHandler* typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId)
        : TCypressNodeProxyBase<IBase, TImpl>(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
    { }

    virtual TValue GetValue() const
    {
        return this->GetTypedImpl().Value();
    }

    virtual void SetValue(const TValue& value)
    {
        this->GetTypedImplForUpdate(ELockMode::Exclusive).Value() = value;
    }
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_SCALAR_TYPE(key, type) \
    class T##key##NodeProxy \
        : public TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node> \
    { \
        YTREE_NODE_TYPE_OVERRIDES(key) \
    \
    public: \
        T##key##NodeProxy( \
            INodeTypeHandler* typeHandler, \
            NCellMaster::TBootstrap* bootstrap, \
            NTransactionServer::TTransaction* transaction, \
            const TNodeId& id) \
            : TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node>( \
                typeHandler, \
                bootstrap, \
                transaction, \
                id) \
        { } \
    }; \
    \
    template <> \
    inline ICypressNodeProxy::TPtr TScalarNodeTypeHandler<type>::GetProxy( \
        const TNodeId& nodeId, \
        NTransactionServer::TTransaction* transaction) \
    { \
        return New<T##key##NodeProxy>( \
            this, \
            Bootstrap, \
            transaction, \
            nodeId); \
    }

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Integer, i64)
DECLARE_SCALAR_TYPE(Double, double)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCompositeNodeProxyBase
    : public TCypressNodeProxyBase<IBase, TImpl>
{
public:
    virtual TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const
    {
        return this;
    }

    virtual TIntrusivePtr<NYTree::ICompositeNode> AsComposite()
    {
        return this;
    }

protected:
    typedef TCypressNodeProxyBase<IBase, TImpl> TBase;

    TCompositeNodeProxyBase(
        INodeTypeHandler* typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
    { }

    virtual void CreateRecursive(
        const NYTree::TYPath& path,
        NYTree::INode* value) = 0;

    virtual void DoInvoke(NRpc::IServiceContext* context)
    {
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        TBase::DoInvoke(context);
    }

    virtual bool IsWriteRequest(NRpc::IServiceContext* context) const
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Create);
        return TBase::IsWriteRequest(context);
    }

protected:
    virtual void GetSystemAttributes(std::vector<typename TBase::TAttributeInfo>* attributes)
    {
        attributes->push_back("count");
        TBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer)
    {
        if (name == "count") {
            BuildYsonFluently(consumer)
                .Scalar(this->GetChildCount());
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }

    DECLARE_RPC_SERVICE_METHOD(NProto, Create)
    {
        auto type = EObjectType(request->type());

        context->SetRequestInfo("Type: %s", ~type.ToString());

        NYTree::TTokenizer tokenizer(context->GetPath());
        if (!tokenizer.ParseNext()) {
            ythrow yexception() << "Node already exists";
        }
        tokenizer.CurrentToken().CheckType(NYTree::PathSeparatorToken);

        auto cypressManager = this->Bootstrap->GetCypressManager();

        auto handler = cypressManager->FindHandler(type);
        if (!handler) {
            ythrow yexception() << "Unknown object type";
        }

        auto nodeId = handler->CreateDynamic(
            this->Transaction,
            request,
            response);

        auto proxy = cypressManager->GetVersionedNodeProxy(
            nodeId,
            this->Transaction);

        proxy->Attributes().MergeFrom(request->Attributes());

        CreateRecursive(NYTree::TYPath(tokenizer.GetCurrentSuffix()), ~proxy);

        *response->mutable_object_id() = nodeId.ToProto();

        context->Reply();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeProxy
    : public TCompositeNodeProxyBase<NYTree::IMapNode, TMapNode>
    , public NYTree::TMapNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(Map)

public:
    TMapNodeProxy(
        INodeTypeHandler* typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector< TPair<Stroka, NYTree::INodePtr> > GetChildren() const;
    virtual yvector<Stroka> GetKeys() const;
    virtual NYTree::INodePtr FindChild(const TStringBuf& key) const;
    virtual bool AddChild(NYTree::INode* child, const TStringBuf& key);
    virtual bool RemoveChild(const TStringBuf& key);
    virtual void ReplaceChild(NYTree::INode* oldChild, NYTree::INode* newChild);
    virtual void RemoveChild(NYTree::INode* child);
    virtual Stroka GetChildKey(const INode* child);

protected:
    typedef TCompositeNodeProxyBase<NYTree::IMapNode, TMapNode> TBase;

    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual void CreateRecursive(const NYTree::TYPath& path, INode* value);
    virtual IYPathService::TResolveResult ResolveRecursive(const NYTree::TYPath& path, const Stroka& verb);
    virtual void SetRecursive(const NYTree::TYPath& path, TReqSet* request, TRspSet* response, TCtxSet* context);
    virtual void SetNodeRecursive(const NYTree::TYPath& path, TReqSetNode* request, TRspSetNode* response, TCtxSetNode* context);

    yhash_map<Stroka, NYTree::INodePtr> DoGetChildren() const;
    NYTree::INodePtr DoFindChild(const TStringBuf& key, bool skipCurrentTransaction) const;
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCompositeNodeProxyBase<NYTree::IListNode, TListNode>
    , public NYTree::TListNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    TListNodeProxy(
        INodeTypeHandler* typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual yvector<NYTree::INodePtr> GetChildren() const;
    virtual NYTree::INodePtr FindChild(int index) const;
    virtual void AddChild(NYTree::INode* child, int beforeIndex = -1);
    virtual bool RemoveChild(int index);
    virtual void ReplaceChild(NYTree::INode* oldChild, NYTree::INode* newChild);
    virtual void RemoveChild(NYTree::INode* child);
    virtual int GetChildIndex(const NYTree::INode* child);

protected:
    typedef TCompositeNodeProxyBase<NYTree::IListNode, TListNode> TBase;

    virtual void CreateRecursive(
        const NYTree::TYPath& path,
        INode* value);
    virtual IYPathService::TResolveResult ResolveRecursive(
        const NYTree::TYPath& path,
        const Stroka& verb);
    virtual void SetRecursive(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSet* context);
    virtual void SetNodeRecursive(
        const NYTree::TYPath& path,
        TReqSetNode* request,
        TRspSetNode* response,
        TCtxSetNode* context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
