#pragma once

#include "node_proxy.h"
#include "node_detail.h"

#include <ytlib/cypress_client/cypress_ypath.pb.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/ypath_format.h>
#include <ytlib/ytree/tokenizer.h>
#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/node_detail.h>
#include <ytlib/ytree/convert.h>
#include <ytlib/ytree/ephemeral.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/object_server/object_detail.h>
#include <ytlib/object_server/id.h>
#include <ytlib/cell_master/public.h>
#include <ytlib/transaction_server/transaction.h>

namespace NYT {
namespace NCypressServer {

using NObjectServer::TObjectId;
using NObjectServer::NullObjectId;

////////////////////////////////////////////////////////////////////////////////

class TNodeFactory
    : public NYTree::INodeFactory
{
public:
    TNodeFactory(
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction);
    ~TNodeFactory();

    virtual NYTree::IStringNodePtr CreateString() override;
    virtual NYTree::IIntegerNodePtr CreateInteger() override;
    virtual NYTree::IDoubleNodePtr CreateDouble() override;
    virtual NYTree::IMapNodePtr CreateMap() override;
    virtual NYTree::IListNodePtr CreateList() override;
    virtual NYTree::IEntityNodePtr CreateEntity() override;

private:
    NCellMaster::TBootstrap* Bootstrap;
    NTransactionServer::TTransaction* Transaction;
    std::vector<TNodeId> CreatedNodeIds;

    ICypressNodeProxyPtr DoCreate(NObjectServer::EObjectType type);

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
    TCypressNodeProxyBase(
        INodeTypeHandlerPtr typeHandler,
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

    NYTree::IYPathResolverPtr GetResolver() const
    {
        if (!Resolver) {
            auto cypressManager = Bootstrap->GetCypressManager();
            Resolver = cypressManager->CreateResolver(Transaction);
        }
        return Resolver;
    }


    virtual TTransactionId GetTransactionId() const override
    {
        return NObjectServer::GetObjectId(Transaction);
    }

    virtual const TNodeId& GetId() const override
    {
        return NodeId;
    }


    virtual NYTree::ENodeType GetType() const override
    {
        return TypeHandler->GetNodeType();
    }


    virtual NYTree::ICompositeNodePtr GetParent() const override
    {
        auto nodeId = GetThisImpl()->GetParentId();
        return nodeId == NullObjectId ? NULL : GetProxy(nodeId)->AsComposite();
    }

    virtual void SetParent(NYTree::ICompositeNodePtr parent) override
    {
        LockThisImpl()->SetParentId(parent ? GetNodeId(NYTree::INodePtr(parent)) : NullObjectId);
    }


    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Lock);
        // NB: Create is not considered a write verb since it always fails here.
        return NYTree::TNodeBase::IsWriteRequest(context);
    }

    virtual NYTree::IAttributeDictionary& Attributes() override
    {
        return NObjectServer::TObjectProxyBase::Attributes();
    }

    virtual const NYTree::IAttributeDictionary& Attributes() const override
    {
        return NObjectServer::TObjectProxyBase::Attributes();
    }


    virtual ICypressNodeProxyPtr Clone() override
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        auto objectManager = Bootstrap->GetObjectManager();

        auto type = NObjectServer::TypeFromId(NodeId);
        auto clonedNodeId = Bootstrap->GetObjectManager()->GenerateId(type);

        auto clonedNode = TypeHandler->Instantiate(clonedNodeId);
        auto clonedNode_ = clonedNode.Get();

        clonedNode_->SetTrunkNode(clonedNode_);

        cypressManager->RegisterNode(Transaction, clonedNode);

        DoCloneTo(dynamic_cast<TImpl*>(clonedNode_));

        return TypeHandler->GetProxy(clonedNodeId, Transaction);;
    }


protected:
    INodeTypeHandlerPtr TypeHandler;
    NCellMaster::TBootstrap* Bootstrap;
    NTransactionServer::TTransaction* Transaction;
    TNodeId NodeId;

    mutable NYTree::IYPathResolverPtr Resolver;


    virtual NObjectServer::TVersionedObjectId GetVersionedId() const override
    {
        return NObjectServer::TVersionedObjectId(NodeId, NObjectServer::GetObjectId(Transaction));
    }


    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("parent_id");
        attributes->push_back("locks");
        attributes->push_back("lock_mode");
        attributes->push_back(TAttributeInfo("path", true, true));
        attributes->push_back("creation_time");
        NObjectServer::TObjectProxyBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer) override
    {
        const auto* node = GetThisImpl();

        // NB: Locks are stored in trunk nodes (TransactionId == Null).
        const auto* trunkNode = Bootstrap->GetCypressManager()->GetNode(Id);

        if (name == "parent_id") {
            BuildYsonFluently(consumer)
                .Scalar(node->GetParentId().ToString());
            return true;
        }

        if (name == "locks") {
            BuildYsonFluently(consumer)
                .DoListFor(trunkNode->Locks(), [=] (NYTree::TFluentList fluent, const ICypressNode::TLockMap::value_type& pair) {
                    fluent.Item()
                        .BeginMap()
                            .Item("mode").Scalar(pair.second.Mode)
                            .Item("transaction_id").Scalar(pair.first->GetId())
                            .DoIf(!pair.second.ChildKeys.empty(), [=] (NYTree::TFluentMap fluent) {
                                fluent
                                    .Item("child_keys").List(pair.second.ChildKeys);
                            })
                            .DoIf(!pair.second.AttributeKeys.empty(), [=] (NYTree::TFluentMap fluent) {
                                fluent
                                    .Item("attribute_keys").List(pair.second.AttributeKeys);
                            })
                        .EndMap();
                 });
            return true;
        }

        if (name == "lock_mode") {
            BuildYsonFluently(consumer)
                .Scalar(FormatEnum(node->GetLockMode()));
            return true;
        }

        if (name == "path") {
            BuildYsonFluently(consumer)
                .Scalar(GetPath());
            return true;
        }

        if (name == "creation_time") {
            BuildYsonFluently(consumer)
                .Scalar(node->GetCreationTime().ToString());
            return true;
        }

        return NObjectServer::TObjectProxyBase::GetSystemAttribute(name, consumer);
    }


    virtual void DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(GetId);
        DISPATCH_YPATH_SERVICE_METHOD(Lock);
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        TNodeBase::DoInvoke(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, Lock)
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

        if (!Transaction) {
            ythrow yexception() << "Cannot take a lock outside of a transaction";
        }

        auto cypressManager = Bootstrap->GetCypressManager();
        cypressManager->LockVersionedNode(NodeId, Transaction, mode);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, Create)
    {
        UNUSED(request);
        UNUSED(response);

        NYTree::TTokenizer tokenizer(context->GetPath());
        if (!tokenizer.ParseNext()) {
            ythrow yexception() << Sprintf("Node %s already exists",
                ~this->GetPath());
        }

        ThrowVerbNotSuppored(this, context->GetVerb());
    }


    const ICypressNode* GetImpl(const TNodeId& nodeId) const
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        return cypressManager->GetVersionedNode(nodeId, Transaction);
    }

    ICypressNode* LockImpl(
        const TNodeId& nodeId,
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        auto cypressManager = Bootstrap->GetCypressManager();
        return cypressManager->LockVersionedNode(nodeId, Transaction, request, recursive);
    }


    const ICypressNode* GetThisImpl() const
    {
        return GetImpl(NodeId);
    }

    ICypressNode* LockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return LockImpl(NodeId, request, recursive);
    }


    const TImpl* GetThisTypedImpl() const
    {
        return static_cast<const TImpl*>(GetThisImpl());
    }

    TImpl* LockThisTypedImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return static_cast<TImpl*>(LockThisImpl(request, recursive));
    }


    ICypressNodeProxyPtr GetProxy(const TNodeId& nodeId) const
    {
        YASSERT(nodeId != NullObjectId);
        return Bootstrap->GetCypressManager()->GetVersionedNodeProxy(nodeId, Transaction);
    }


    TNodeId GetNodeId(NYTree::INodePtr node)
    {
        return dynamic_cast<ICypressNodeProxy&>(*node).GetId();
    }

    TNodeId GetNodeId(NYTree::IConstNodePtr node)
    {
        return dynamic_cast<const ICypressNodeProxy&>(*node).GetId();
    }


    void AttachChild(ICypressNode* child)
    {
        child->SetParentId(NodeId);
        Bootstrap->GetObjectManager()->RefObject(child);
    }

    void DetachChild(ICypressNode* child, bool unref)
    {
        child->SetParentId(NullObjectId);
        if (unref) {
            Bootstrap->GetObjectManager()->UnrefObject(child);
        }
    }


    virtual void DoCloneTo(TImpl* clonedNode)
    {
        // Copy attributes directly to suppress validation.
        auto objectManager = Bootstrap->GetObjectManager();
        auto* attributes = GetUserAttributes();
        NObjectServer::TAttributeSet* clonedAttributes = NULL;
        FOREACH (const auto& key, attributes->List()) {
            auto value = attributes->GetYson(key);
            if (!clonedAttributes) {
                clonedAttributes = objectManager->CreateAttributes(clonedNode->GetId());
            }
            YCHECK(clonedAttributes->Attributes().insert(std::make_pair(key, value)).second);
        }
    }


    virtual TAutoPtr<NYTree::IAttributeDictionary> DoCreateUserAttributes() override
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
            const TObjectId& objectId,
            NTransactionServer::TTransaction* transaction,
            NCellMaster::TBootstrap* bootstrap)
            : TUserAttributeDictionary(
                bootstrap->GetObjectManager(),
                objectId)
            , Transaction(transaction)
            , Bootstrap(bootstrap)
        { }
           
        
        virtual yhash_set<Stroka> List() const
        {
            auto objectManager = Bootstrap->GetObjectManager();
            auto transactionManager = Bootstrap->GetTransactionManager();

            auto transactions = transactionManager->GetTransactionPath(Transaction);
            std::reverse(transactions.begin(), transactions.end());

            yhash_set<Stroka> attributes;
            FOREACH (const auto* transaction, transactions) {
                NObjectServer::TVersionedObjectId versionedId(ObjectId, NObjectServer::GetObjectId(transaction));
                const auto* userAttributes = objectManager->FindAttributes(versionedId);
                if (userAttributes) {
                    FOREACH (const auto& pair, userAttributes->Attributes()) {
                        if (pair.second) {
                            attributes.insert(pair.first);
                        } else {
                            attributes.erase(pair.first);
                        }
                    }
                }
            }
            return attributes;
        }

        virtual TNullable<NYTree::TYsonString> FindYson(const Stroka& name) const override
        {
            auto objectManager = Bootstrap->GetObjectManager();
            auto transactionManager = Bootstrap->GetTransactionManager();

            auto transactions = transactionManager->GetTransactionPath(Transaction);

            FOREACH (const auto* transaction, transactions) {
                NObjectServer::TVersionedObjectId versionedId(ObjectId, NObjectServer::GetObjectId(transaction));
                const auto* userAttributes = objectManager->FindAttributes(versionedId);
                if (userAttributes) {
                    auto it = userAttributes->Attributes().find(name);
                    if (it != userAttributes->Attributes().end()) {
                        return it->second;
                    }
                }
            }

            return Null;
        }

        virtual void SetYson(const Stroka& key, const NYTree::TYsonString& value) override
        {
            auto objectManager = Bootstrap->GetObjectManager();
            auto cypressManager = Bootstrap->GetCypressManager();

            auto* node = cypressManager->LockVersionedNode(
                ObjectId,
                Transaction,
                TLockRequest::SharedAttribute(key));
            auto versionedId = node->GetId();

            auto* userAttributes = objectManager->FindAttributes(versionedId);
            if (!userAttributes) {
                userAttributes = objectManager->CreateAttributes(versionedId);
            }

            userAttributes->Attributes()[key] = value;
        }

        virtual bool Remove(const Stroka& key) override
        {
            auto cypressManager = Bootstrap->GetCypressManager();
            auto objectManager = Bootstrap->GetObjectManager();
            auto transactionManager = Bootstrap->GetTransactionManager();

            auto transactions = transactionManager->GetTransactionPath(Transaction);
            std::reverse(transactions.begin(), transactions.end());

            const NTransactionServer::TTransaction* containingTransaction = NULL;
            bool contains = false;
            FOREACH (const auto* transaction, transactions) {
                NObjectServer::TVersionedObjectId versionedId(ObjectId, NObjectServer::GetObjectId(transaction));
                const auto* userAttributes = objectManager->FindAttributes(versionedId);
                if (userAttributes) {
                    auto it = userAttributes->Attributes().find(key);
                    if (it != userAttributes->Attributes().end()) {
                        contains = it->second;
                        if (contains) {
                            containingTransaction = transaction;
                        }
                        break;
                    }
                }
            }

            if (!contains) {
                return false;
            }

            auto* node = cypressManager->LockVersionedNode(
                ObjectId,
                Transaction,
                TLockRequest::SharedAttribute(key));
            auto versionedId = node->GetId();

            if (containingTransaction == Transaction) {
                auto* userAttributes = objectManager->GetAttributes(versionedId);
                YCHECK(userAttributes->Attributes().erase(key) == 1);
            } else {
                YCHECK(!containingTransaction);
                auto* userAttributes = objectManager->FindAttributes(versionedId);
                if (!userAttributes) {
                    userAttributes = objectManager->CreateAttributes(versionedId);
                }
                userAttributes->Attributes()[key] = Null;           
            }

            return true;
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
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
    { }

    virtual TValue GetValue() const override
    {
        return this->GetThisTypedImpl()->Value();
    }

    virtual void SetValue(const TValue& value) override
    {
        this->LockThisTypedImpl(ELockMode::Exclusive)->Value() = value;
    }

private:
    typedef TCypressNodeProxyBase<IBase, TImpl> TBase;

    virtual void DoCloneTo(TImpl* clonedNode) override
    {
        TBase::DoCloneTo(clonedNode);
        clonedNode->Value() = this->GetThisTypedImpl()->Value();
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
            INodeTypeHandlerPtr typeHandler, \
            NCellMaster::TBootstrap* bootstrap, \
            NTransactionServer::TTransaction* transaction, \
            const TNodeId& versionedChildId) \
            : TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node>( \
                typeHandler, \
                bootstrap, \
                transaction, \
                versionedChildId) \
        { } \
    }; \
    \
    template <> \
    inline ICypressNodeProxyPtr TScalarNodeTypeHandler<type>::GetProxy( \
        const TNodeId& NodeId, \
        NTransactionServer::TTransaction* transaction) \
    { \
        return New<T##key##NodeProxy>( \
            this, \
            Bootstrap, \
            transaction, \
            NodeId); \
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
    virtual TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const override
    {
        return this;
    }

    virtual TIntrusivePtr<NYTree::ICompositeNode> AsComposite() override
    {
        return this;
    }

protected:
    typedef TCypressNodeProxyBase<IBase, TImpl> TBase;

    TCompositeNodeProxyBase(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            nodeId)
    { }

    virtual void SetRecursive(
        const NYTree::TYPath& path,
        NYTree::INodePtr value) = 0;

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Create);
        DISPATCH_YPATH_SERVICE_METHOD(Copy);
        TBase::DoInvoke(context);
    }

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override
    {
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Create);
        DECLARE_YPATH_SERVICE_WRITE_METHOD(Copy);
        return TBase::IsWriteRequest(context);
    }

    virtual void GetSystemAttributes(std::vector<typename TBase::TAttributeInfo>* attributes) override
    {
        attributes->push_back("count");
        TBase::GetSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& name, NYTree::IYsonConsumer* consumer) override
    {
        if (name == "count") {
            BuildYsonFluently(consumer)
                .Scalar(this->GetChildCount());
            return true;
        }

        return TBase::GetSystemAttribute(name, consumer);
    }


    Stroka GetCreativePath(const NYTree::TYPath& path)
    {
        NYTree::TTokenizer tokenizer(path);
        if (!tokenizer.ParseNext()) {
            auto cypressManager = this->Bootstrap->GetCypressManager();
            ythrow yexception() << Sprintf("Node %s already exists",
                ~this->GetPath());
        }
        tokenizer.CurrentToken().CheckType(NYTree::PathSeparatorToken);
        return NYTree::TYPath(tokenizer.GetCurrentSuffix());
    }

    ICypressNodeProxyPtr ResolveSourcePath(const NYTree::TYPath& path)
    {
        auto sourceNode = this->GetResolver()->ResolvePath(path);
        return dynamic_cast<ICypressNodeProxy*>(~sourceNode);
    }


    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, Create)
    {
        auto type = NObjectServer::EObjectType(request->type());
        context->SetRequestInfo("Type: %s", ~type.ToString());

        auto cypressManager = this->Bootstrap->GetCypressManager();
        auto creativePath = this->GetCreativePath(context->GetPath());

        auto handler = cypressManager->FindHandler(type);
        if (!handler) {
            ythrow yexception() << "Unknown object type";
        }

        auto* newNode = cypressManager->CreateNode(
            handler,
            this->Transaction,
            request,
            response,
            &request->Attributes());
        auto newProxy = cypressManager->GetVersionedNodeProxy(
            newNode->GetId().ObjectId,
            this->Transaction);
        
        this->SetRecursive(creativePath, newProxy);

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, Copy)
    {
        auto sourcePath = request->source_path();
        context->SetRequestInfo("SourcePath: %s", ~sourcePath);

        auto creativePath = this->GetCreativePath(context->GetPath());

        auto sourceProxy = this->ResolveSourcePath(sourcePath);
        if (sourceProxy->GetId() == this->GetId()) {
            ythrow yexception() << "Cannot copy a node to its child";
        }

        auto clonedProxy = sourceProxy->Clone();

        this->SetRecursive(creativePath, clonedProxy);

        *response->mutable_object_id() = clonedProxy->GetId().ToProto();

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
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId);

    virtual void Clear() override;
    virtual int GetChildCount() const override;
    virtual std::vector< TPair<Stroka, NYTree::INodePtr> > GetChildren() const override;
    virtual std::vector<Stroka> GetKeys() const override;
    virtual NYTree::INodePtr FindChild(const TStringBuf& key) const override;
    virtual bool AddChild(NYTree::INodePtr child, const TStringBuf& key) override;
    virtual bool RemoveChild(const TStringBuf& key) override;
    virtual void ReplaceChild(NYTree::INodePtr oldChild, NYTree::INodePtr newChild) override;
    virtual void RemoveChild(NYTree::INodePtr child) override;
    virtual Stroka GetChildKey(NYTree::IConstNodePtr child) override;

private:
    typedef TCompositeNodeProxyBase<NYTree::IMapNode, TMapNode> TBase;

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override;
    virtual void SetRecursive(const NYTree::TYPath& path, NYTree::INodePtr value) override;
    virtual IYPathService::TResolveResult ResolveRecursive(const NYTree::TYPath& path, const Stroka& verb) override;

    void DoListChildren(yhash_map<Stroka, TNodeId>* keyToChild) const;
    TVersionedNodeId DoFindChild(const Stroka& key) const;

    virtual void DoCloneTo(TMapNode* clonedNode) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCompositeNodeProxyBase<NYTree::IListNode, TListNode>
    , public NYTree::TListNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    TListNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        const TNodeId& nodeId);

    virtual void Clear();
    virtual int GetChildCount() const;
    virtual std::vector<NYTree::INodePtr> GetChildren() const;
    virtual NYTree::INodePtr FindChild(int index) const;
    virtual void AddChild(NYTree::INodePtr child, int beforeIndex = -1);
    virtual bool RemoveChild(int index);
    virtual void ReplaceChild(NYTree::INodePtr oldChild, NYTree::INodePtr newChild);
    virtual void RemoveChild(NYTree::INodePtr child);
    virtual int GetChildIndex(NYTree::IConstNodePtr child);

private:
    typedef TCompositeNodeProxyBase<NYTree::IListNode, TListNode> TBase;

    virtual void SetRecursive(
        const NYTree::TYPath& path,
        NYTree::INodePtr value);
    virtual IYPathService::TResolveResult ResolveRecursive(
        const NYTree::TYPath& path,
        const Stroka& verb);

    virtual void DoCloneTo(TListNode* clonedNode);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
