#pragma once

#include "node_proxy.h"
#include "node_detail.h"

#include <core/ytree/node.h>

#include <ytlib/cypress_client/cypress_ypath.pb.h>

#include <server/object_server/public.h>

#include <server/cell_master/public.h>

#include <server/transaction_server/public.h>

#include <server/security_server/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeProxyBase
    : public NYTree::TNodeBase
    , public NObjectServer::TObjectProxyBase
    , public ICypressNodeProxy
{
public:
    TNontemplateCypressNodeProxyBase(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TCypressNodeBase* trunkNode);

    virtual NYTree::INodeFactoryPtr CreateFactory() const override;
    virtual ICypressNodeFactoryPtr CreateCypressFactory(
        bool preserveAccount) const override;

    virtual NYTree::INodeResolverPtr GetResolver() const override;

    virtual NTransactionServer::TTransaction* GetTransaction() const override;

    virtual TCypressNodeBase* GetTrunkNode() const override;

    virtual NYTree::ENodeType GetType() const override;

    virtual NYTree::ICompositeNodePtr GetParent() const override;
    virtual void SetParent(NYTree::ICompositeNodePtr parent) override;

    virtual const NYTree::IAttributeDictionary& Attributes() const override;
    virtual NYTree::IAttributeDictionary* MutableAttributes() override;

    virtual NSecurityServer::TClusterResources GetResourceUsage() const override;

protected:
    class TCustomAttributeDictionary;
    class TResourceUsageVisitor;

    INodeTypeHandlerPtr TypeHandler;
    NCellMaster::TBootstrap* Bootstrap;
    NTransactionServer::TTransaction* Transaction;
    TCypressNodeBase* TrunkNode;

    mutable TCypressNodeBase* CachedNode;
    mutable NYTree::INodeResolverPtr CachedResolver;

    bool AccessTrackingSuppressed;

    virtual NLog::TLogger CreateLogger() const override;

    virtual NObjectServer::TVersionedObjectId GetVersionedId() const override;
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() override;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual TAsyncError GetBuiltinAttributeAsync(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(const Stroka& key, const NYTree::TYsonString& value) override;

    virtual void BeforeInvoke(NRpc::IServiceContextPtr context) override;
    virtual void AfterInvoke(NRpc::IServiceContextPtr context) override;
    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    // Suppress access handling in the cases below.
    virtual void GetAttribute(
        const NYTree::TYPath& path,
        TReqGet* request,
        TRspGet* response,
        TCtxGetPtr context) override;
    virtual void ListAttribute(
        const NYTree::TYPath& path,
        TReqList* request,
        TRspList* response,
        TCtxListPtr context) override;
    virtual void ExistsSelf(
        TReqExists* request,
        TRspExists* response,
        TCtxExistsPtr context) override;
    virtual void ExistsRecursive(
        const NYTree::TYPath& path,
        TReqExists* request,
        TRspExists* response,
        TCtxExistsPtr context) override;
    virtual void ExistsAttribute(
        const NYTree::TYPath& path,
        TReqExists* request,
        TRspExists* response,
        TCtxExistsPtr context) override;

    TCypressNodeBase* GetImpl(TCypressNodeBase* trunkNode) const;

    TCypressNodeBase* LockImpl(
        TCypressNodeBase* trunkNode,
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false) const;

    TCypressNodeBase* GetThisImpl();
    const TCypressNodeBase* GetThisImpl() const;

    TCypressNodeBase* LockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false);


    template <class TImpl>
    TImpl* GetThisTypedImpl()
    {
        return dynamic_cast<TImpl*>(GetThisImpl());
    }

    template <class TImpl>
    const TImpl* GetThisTypedImpl() const
    {
        return dynamic_cast<const TImpl*>(GetThisImpl());
    }

    template <class TImpl>
    TImpl* LockThisTypedImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return dynamic_cast<TImpl*>(LockThisImpl(request, recursive));
    }


    ICypressNodeProxyPtr GetProxy(TCypressNodeBase* trunkNode) const;
    static ICypressNodeProxy* ToProxy(NYTree::INodePtr node);
    static const ICypressNodeProxy* ToProxy(NYTree::IConstNodePtr node);

    virtual std::unique_ptr<NYTree::IAttributeDictionary> DoCreateCustomAttributes() override;
    
    // TSupportsPermissions members
    virtual void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission) override;

    // Cypress-specific overload.
    void ValidatePermission(
        TCypressNodeBase* node,
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission);

    // Inject other overloads into the scope.
    using TObjectProxyBase::ValidatePermission;

    void SetModified();

    void SetAccessed();
    void SuppressAccessTracking();

    ICypressNodeProxyPtr ResolveSourcePath(const NYPath::TYPath& path);

    virtual bool CanHaveChildren() const;
    
    virtual void SetChild(
        NYTree::INodeFactoryPtr factory,
        const NYPath::TYPath& path,
        NYTree::INodePtr value,
        bool recursive);

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Lock);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);

};

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCompositeCypressNodeProxyBase
    : public TNontemplateCypressNodeProxyBase
    , public virtual NYTree::ICompositeNode
{
public:
    virtual TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const override;
    virtual TIntrusivePtr<NYTree::ICompositeNode> AsComposite() override;

protected:
    TNontemplateCompositeCypressNodeProxyBase(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TCypressNodeBase* trunkNode);

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;

    virtual bool CanHaveChildren() const override;

};

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class IBase, class TImpl>
class TCypressNodeProxyBase
    : public TBase
    , public virtual IBase
{
public:
    TCypressNodeProxyBase(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TImpl* trunkNode)
        : TBase(
            typeHandler,
            bootstrap,
            transaction,
            trunkNode)
    { }

protected:
    TImpl* GetThisTypedImpl()
    {
        return TNontemplateCypressNodeProxyBase::GetThisTypedImpl<TImpl>();
    }

    const TImpl* GetThisTypedImpl() const
    {
        return TNontemplateCypressNodeProxyBase::GetThisTypedImpl<TImpl>();
    }

    TImpl* LockThisTypedImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return TNontemplateCypressNodeProxyBase::LockThisTypedImpl<TImpl>(request, recursive);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl>
{
public:
    TScalarNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TScalarNode<TValue>* trunkNode)
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
        this->LockThisTypedImpl()->Value() = value;
        this->SetModified();
    }

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl> TBase;

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
            TScalarNode<type>* node) \
            : TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node>( \
                typeHandler, \
                bootstrap, \
                transaction, \
                node) \
        { } \
    }; \
    \
    template <> \
    inline ICypressNodeProxyPtr TScalarNodeTypeHandler<type>::DoGetProxy( \
        TScalarNode<type>* node, \
        NTransactionServer::TTransaction* transaction) \
    { \
        return New<T##key##NodeProxy>( \
            this, \
            Bootstrap, \
            transaction, \
            node); \
    }

DECLARE_SCALAR_TYPE(String, Stroka)
DECLARE_SCALAR_TYPE(Int64, i64)
DECLARE_SCALAR_TYPE(Double, double)
DECLARE_SCALAR_TYPE(Boolean, bool)

#undef DECLARE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

class TMapNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IMapNode, TMapNode>
    , public NYTree::TMapNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(Map)

public:
    TMapNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TMapNode* trunkNode);

    virtual void Clear() override;
    virtual int GetChildCount() const override;
    virtual std::vector< std::pair<Stroka, NYTree::INodePtr> > GetChildren() const override;
    virtual std::vector<Stroka> GetKeys() const override;
    virtual NYTree::INodePtr FindChild(const Stroka& key) const override;
    virtual bool AddChild(NYTree::INodePtr child, const Stroka& key) override;
    virtual bool RemoveChild(const Stroka& key) override;
    virtual void ReplaceChild(NYTree::INodePtr oldChild, NYTree::INodePtr newChild) override;
    virtual void RemoveChild(NYTree::INodePtr child) override;
    virtual Stroka GetChildKey(NYTree::IConstNodePtr child) override;

private:
    typedef TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IMapNode, TMapNode> TBase;

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;
    
    virtual void SetChild(
        NYTree::INodeFactoryPtr factory,
        const NYPath::TYPath& path,
        NYTree::INodePtr value,
        bool recursive) override;
    
    virtual IYPathService::TResolveResult ResolveRecursive(const NYPath::TYPath& path, NRpc::IServiceContextPtr context) override;

    void DoRemoveChild(TMapNode* impl, const Stroka& key, TCypressNodeBase* childImpl);

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode>
    , public NYTree::TListNodeMixin
{
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    TListNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TListNode* trunkNode);

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
    typedef TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode> TBase;

    virtual void SetChild(
        NYTree::INodeFactoryPtr factory,
        const NYPath::TYPath& path,
        NYTree::INodePtr value,
        bool recursive);

    virtual IYPathService::TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        NRpc::IServiceContextPtr context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TLinkNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TLinkNode>
{
    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    TLinkNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TLinkNode* trunkNode);

    virtual IYPathService::TResolveResult Resolve(
        const NYPath::TYPath& path,
        NRpc::IServiceContextPtr context) override;

private:
    class TDoesNotExistService;

    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TLinkNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(const Stroka& key, const NYTree::TYsonString& value) override;

    NObjectServer::IObjectProxyPtr FindTargetProxy() const;
    NObjectServer::IObjectProxyPtr GetTargetProxy() const;

    bool IsBroken(const NObjectServer::TObjectId& id) const;

};

////////////////////////////////////////////////////////////////////////////////

class TDocumentNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TDocumentNode>
{
public:
    TDocumentNodeProxy(
        INodeTypeHandlerPtr typeHandler,
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransaction* transaction,
        TDocumentNode* trunkNode);

    virtual NYTree::ENodeType GetType() const override;

    virtual TIntrusivePtr<const NYTree::IEntityNode> AsEntity() const override;
    virtual TIntrusivePtr<NYTree::IEntityNode> AsEntity() override;

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TDocumentNode> TBase;

    IYPathService::TResolveResult ResolveRecursive(const NYPath::TYPath& path, NRpc::IServiceContextPtr context);

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override;
    virtual void GetRecursive(const NYPath::TYPath& path, TReqGet* request, TRspGet* response, TCtxGetPtr context) override;

    virtual void SetSelf(TReqSet* request, TRspSet* response, TCtxSetPtr context) override;
    virtual void SetRecursive(const NYPath::TYPath& path, TReqSet* request, TRspSet* response, TCtxSetPtr context) override;

    virtual void ListSelf(TReqList* request, TRspList* response, TCtxListPtr context) override;
    virtual void ListRecursive(const NYPath::TYPath& path, TReqList* request, TRspList* response, TCtxListPtr context) override;

    virtual void RemoveRecursive(const NYPath::TYPath& path, TReqRemove* request, TRspRemove* response, TCtxRemovePtr context) override;

    virtual void ExistsRecursive(const NYPath::TYPath& path, TReqExists* request, TRspExists* response, TCtxExistsPtr context) override;

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(const Stroka& key, const NYTree::TYsonString& value) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
