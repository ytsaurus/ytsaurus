#pragma once

#include "node_proxy.h"
#include "node_detail.h"

#include <ytlib/ytree/node.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/ypath_detail.h>
#include <ytlib/ytree/node_detail.h>
#include <ytlib/ytree/convert.h>
#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/fluent.h>

#include <ytlib/ypath/tokenizer.h>

#include <ytlib/cypress_client/cypress_ypath.pb.h>

#include <server/object_server/public.h>
#include <server/object_server/object_detail.h>

#include <server/cell_master/public.h>

#include <server/transaction_server/transaction.h>

namespace NYT {
namespace NCypressServer {

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

    ICypressNodeProxyPtr DoCreate(NObjectClient::EObjectType type);

};

////////////////////////////////////////////////////////////////////////////////

class TVersionedUserAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    TVersionedUserAttributeDictionary(
        const NObjectClient::TObjectId& id,
        NTransactionServer::TTransaction* transaction,
        NCellMaster::TBootstrap* bootstrap);

    virtual std::vector<Stroka> List() const override;
    virtual TNullable<NYTree::TYsonString> FindYson(const Stroka& name) const override;
    virtual void SetYson(const Stroka& key, const NYTree::TYsonString& value) override;
    virtual bool Remove(const Stroka& key) override;

protected:
    TNodeId Id;
    NTransactionServer::TTransaction* Transaction;
    NCellMaster::TBootstrap* Bootstrap;

};

////////////////////////////////////////////////////////////////////////////////

class TCypressNodeProxyNontemplateBase
    : public NYTree::TNodeBase
    , public NObjectServer::TObjectProxyBase
    , public ICypressNodeProxy
{
public:
    TCypressNodeProxyNontemplateBase(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        ICypressNode* trunkNode);

    NYTree::INodeFactoryPtr CreateFactory() const;
    NYTree::IYPathResolverPtr GetResolver() const;

    virtual NTransactionServer::TTransaction* GetTransaction() const override;

    virtual ICypressNode* GetTrunkNode() const override;

    virtual NYTree::ENodeType GetType() const override;

    virtual NYTree::ICompositeNodePtr GetParent() const override;
    virtual void SetParent(NYTree::ICompositeNodePtr parent) override;

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

    virtual NYTree::IAttributeDictionary& Attributes() override;
    virtual const NYTree::IAttributeDictionary& Attributes() const override;

    virtual void GetAttributes(
        NYTree::IYsonConsumer* consumer,
        const NYTree::TAttributeFilter& filter) const override;

    virtual TClusterResources GetResourceUsage() const override;

protected:
    INodeTypeHandlerPtr TypeHandler;
    NCellMaster::TBootstrap* Bootstrap;
    NTransactionServer::TTransaction* Transaction;
    ICypressNode* TrunkNode;

    mutable NYTree::IYPathResolverPtr Resolver;

    virtual NObjectServer::TVersionedObjectId GetVersionedId() const override;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const override;
    virtual bool GetSystemAttribute(const Stroka& key, NYTree::IYsonConsumer* consumer) const override;
    virtual TAsyncError GetSystemAttributeAsync(const Stroka& key, NYTree::IYsonConsumer* consumer) const override;

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override;

    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, Lock);
    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, Create);

    const ICypressNode* GetImpl(const TNodeId& nodeId) const;
    ICypressNode* GetMutableImpl(const TNodeId& nodeId);

    ICypressNode* LockImpl(
        const TNodeId& nodeId,
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false);

    const ICypressNode* GetThisImpl() const;
    ICypressNode* GetThisMutableImpl();

    ICypressNode* LockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false);

    ICypressNodeProxyPtr GetProxy(const TNodeId& nodeId) const;
    static ICypressNodeProxyPtr ToProxy(NYTree::INodePtr node);

    static TNodeId GetNodeId(NYTree::INodePtr node);
    static TNodeId GetNodeId(NYTree::IConstNodePtr node);

    void AttachChild(ICypressNode* child);
    void DetachChild(ICypressNode* child, bool unref);

    virtual TAutoPtr<NYTree::IAttributeDictionary> DoCreateUserAttributes() override;
    
    void SetModified();
};

////////////////////////////////////////////////////////////////////////////////

template <class IBase, class TImpl>
class TCypressNodeProxyBase
    : public TCypressNodeProxyNontemplateBase
    , public virtual IBase
{
public:
    TCypressNodeProxyBase(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        ICypressNode* trunkNode)
        : TCypressNodeProxyNontemplateBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

protected:
    const TImpl* GetThisTypedImpl() const
    {
        return dynamic_cast<const TImpl*>(GetThisImpl());
    }

    TImpl* GetThisTypedMutableImpl()
    {
        return dynamic_cast<TImpl*>(GetThisMutableImpl());
    }

    TImpl* LockThisTypedImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return dynamic_cast<TImpl*>(LockThisImpl(request, recursive));
    }
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
        ICypressNode* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

    virtual typename NMpl::TCallTraits<TValue>::TType GetValue() const override
    {
        return this->GetThisTypedImpl()->Value();
    }

    virtual void SetValue(typename NMpl::TCallTraits<TValue>::TType value) override
    {
        this->LockThisTypedImpl(ELockMode::Exclusive)->Value() = value;
        this->SetModified();
    }

private:
    typedef TCypressNodeProxyBase<IBase, TImpl> TBase;

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
            ICypressNode* trunkNode) \
            : TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node>( \
                typeHandler, \
                bootstrap, \
                transaction, \
                trunkNode) \
        { } \
    }; \
    \
    template <> \
    inline ICypressNodeProxyPtr TScalarNodeTypeHandler<type>::GetProxy( \
        ICypressNode* trunkNode, \
        NTransactionServer::TTransaction* transaction) \
    { \
        return New<T##key##NodeProxy>( \
            this, \
            Bootstrap, \
            transaction, \
            trunkNode); \
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
        ICypressNode* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

    virtual void SetRecursive(
        const NYPath::TYPath& path,
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

    virtual void ListSystemAttributes(std::vector<typename TBase::TAttributeInfo>* attributes) const override
    {
        attributes->push_back("count");
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, NYTree::IYsonConsumer* consumer) const override
    {
        if (key == "count") {
            BuildYsonFluently(consumer)
                .Scalar(this->GetChildCount());
            return true;
        }

        return TBase::GetSystemAttribute(key, consumer);
    }

    NYPath::TYPath GetCreativePath(const NYPath::TYPath& path) const
    {
        NYPath::TTokenizer tokenizer(path);
        if (tokenizer.Advance() == NYPath::ETokenType::EndOfStream) {
            THROW_ERROR_EXCEPTION("Node already exists: %s",
                ~this->GetPath());
        }

        tokenizer.Expect(NYPath::ETokenType::Slash);
        return tokenizer.GetSuffix();
    }

    ICypressNodeProxyPtr ResolveSourcePath(const NYPath::TYPath& path)
    {
        auto sourceNode = this->GetResolver()->ResolvePath(path);
        return dynamic_cast<ICypressNodeProxy*>(~sourceNode);
    }

    DECLARE_RPC_SERVICE_METHOD(NCypressClient::NProto, Create)
    {
        auto type = NObjectClient::EObjectType(request->type());
        context->SetRequestInfo("Type: %s", ~type.ToString());

        auto cypressManager = this->Bootstrap->GetCypressManager();
        auto creativePath = this->GetCreativePath(context->GetPath());

        auto handler = cypressManager->FindHandler(type);
        if (!handler) {
            THROW_ERROR_EXCEPTION("Unknown object type: %s",
                ~type.ToString());
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
            THROW_ERROR_EXCEPTION("Cannot copy a node to its child");
        }

        auto cypressManager = this->Bootstrap->GetCypressManager();
        auto sourceId = this->GetNodeId(NYTree::INodePtr(sourceProxy));
        auto* sourceImpl = const_cast<ICypressNode*>(this->GetImpl(sourceId));
        auto* clonedImpl = cypressManager->CloneNode(
            sourceImpl,
            this->Transaction);
        const auto& clonedId = clonedImpl->GetId().ObjectId;
        auto clonedProxy = this->GetProxy(clonedId);

        this->SetRecursive(creativePath, clonedProxy);

        *response->mutable_object_id() = clonedId.ToProto();

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
        ICypressNode* trunkNode);

    virtual void Clear() override;
    virtual int GetChildCount() const override;
    virtual std::vector< TPair<Stroka, NYTree::INodePtr> > GetChildren() const override;
    virtual std::vector<Stroka> GetKeys() const override;
    virtual NYTree::INodePtr FindChild(const Stroka& key) const override;
    virtual bool AddChild(NYTree::INodePtr child, const Stroka& key) override;
    virtual bool RemoveChild(const Stroka& key) override;
    virtual void ReplaceChild(NYTree::INodePtr oldChild, NYTree::INodePtr newChild) override;
    virtual void RemoveChild(NYTree::INodePtr child) override;
    virtual Stroka GetChildKey(NYTree::IConstNodePtr child) override;

private:
    typedef TCompositeNodeProxyBase<NYTree::IMapNode, TMapNode> TBase;

    virtual void DoInvoke(NRpc::IServiceContextPtr context) override;
    virtual void SetRecursive(const NYPath::TYPath& path, NYTree::INodePtr value) override;
    virtual IYPathService::TResolveResult ResolveRecursive(const NYPath::TYPath& path, NRpc::IServiceContextPtr context) override;

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
        ICypressNode* trunkNode);

    virtual void Clear() override;
    virtual int GetChildCount() const override;
    virtual std::vector<NYTree::INodePtr> GetChildren() const override;
    virtual NYTree::INodePtr FindChild(int index) const override;
    virtual void AddChild(NYTree::INodePtr child, int beforeIndex = -1) override;
    virtual bool RemoveChild(int index) override;
    virtual void ReplaceChild(NYTree::INodePtr oldChild, NYTree::INodePtr newChild) override;
    virtual void RemoveChild(NYTree::INodePtr child) override;
    virtual int GetChildIndex(NYTree::IConstNodePtr child) override;

private:
    typedef TCompositeNodeProxyBase<NYTree::IListNode, TListNode> TBase;

    virtual void SetRecursive(
        const NYPath::TYPath& path,
        NYTree::INodePtr value);
    virtual IYPathService::TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        NRpc::IServiceContextPtr context) override;
        
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
