#pragma once

#include "node_detail.h"
#include "node_proxy.h"

#include <yt/server/cell_master/config.h>

#include <yt/server/object_server/public.h>

#include <yt/server/security_server/public.h>

#include <yt/server/transaction_server/public.h>

#include <yt/ytlib/cypress_client/cypress_ypath.pb.h>

#include <yt/core/ytree/node.h>

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
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TCypressNodeBase* trunkNode);

    virtual std::unique_ptr<NYTree::ITransactionalNodeFactory> CreateFactory() const override;
    virtual std::unique_ptr<ICypressNodeFactory> CreateCypressFactory(
        NSecurityServer::TAccount* account,
        const TNodeFactoryOptions& options) const override;

    virtual NYPath::TYPath GetPath() const override;

    virtual NTransactionServer::TTransaction* GetTransaction() const override;

    virtual TCypressNodeBase* GetTrunkNode() const override;

    virtual NYTree::ICompositeNodePtr GetParent() const override;
    virtual void SetParent(const NYTree::ICompositeNodePtr& parent) override;

    virtual const NYTree::IAttributeDictionary& Attributes() const override;
    virtual NYTree::IAttributeDictionary* MutableAttributes() override;

    virtual void ValidateStorageParametersUpdate();

protected:
    class TCustomAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        explicit TCustomAttributeDictionary(TNontemplateCypressNodeProxyBase* proxy);

        // IAttributeDictionary members
        virtual std::vector<TString> List() const override;
        virtual NYson::TYsonString FindYson(const TString& key) const override;
        virtual void SetYson(const TString& key, const NYson::TYsonString& value) override;
        virtual bool Remove(const TString& key) override;

    private:
        TNontemplateCypressNodeProxyBase* const Proxy_;

    } CustomAttributesImpl_;

    class TResourceUsageVisitor;

    NTransactionServer::TTransaction* const Transaction;
    TCypressNodeBase* const TrunkNode;

    mutable TCypressNodeBase* CachedNode = nullptr;

    bool AccessTrackingSuppressed = false;
    bool ModificationTrackingSuppressed = false;


    virtual NObjectServer::TVersionedObjectId GetVersionedId() const override;
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() override;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    TFuture<NYson::TYsonString> GetExternalBuiltinAttributeAsync(NYTree::TInternedAttributeKey key);
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual void BeforeInvoke(const NRpc::IServiceContextPtr& context) override;
    virtual void AfterInvoke(const NRpc::IServiceContextPtr& context) override;
    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual void GetSelf(
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;

    virtual void RemoveSelf(
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    // Suppress access handling in the cases below.
    virtual void GetAttribute(
        const NYTree::TYPath& path,
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;
    virtual void ListAttribute(
        const NYTree::TYPath& path,
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;
    virtual void ExistsSelf(
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;
    virtual void ExistsRecursive(
        const NYTree::TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;
    virtual void ExistsAttribute(
        const NYTree::TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;

    TCypressNodeBase* GetImpl(TCypressNodeBase* trunkNode) const;

    TCypressNodeBase* LockImpl(
        TCypressNodeBase* trunkNode,
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false) const;

    template <class TImpl = TCypressNodeBase>
    TImpl* GetThisImpl()
    {
        return DoGetThisImpl()->As<TImpl>();
    }

    template <class TImpl = TCypressNodeBase>
    const TImpl* GetThisImpl() const
    {
        return const_cast<TNontemplateCypressNodeProxyBase*>(this)->GetThisImpl<TImpl>();
    }

    template <class TImpl = TCypressNodeBase>
    TImpl* LockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return DoLockThisImpl(request, recursive)->As<TImpl>();
    }


    ICypressNodeProxyPtr GetProxy(TCypressNodeBase* trunkNode) const;

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

    void ValidateNotExternal();

    // If some argument is null, eschews corresponding parts of validation.
    void ValidateMediaChange(
        const TNullable<NChunkServer::TChunkReplication>& oldReplication,
        TNullable<int> primaryMediumIndex,
        const NChunkServer::TChunkReplication& newReplication);

    //! Validates an attempt to set #newPrimaryMedium as a primary medium.
    /*!
     * On failure, throws.
     * If there's nothing to be done, return false.
     * On success, returns true and modifies *newReplication accordingly.
     */
    bool ValidatePrimaryMediumChange(
        NChunkServer::TMedium* newPrimaryMedium,
        const NChunkServer::TChunkReplication& oldReplication,
        TNullable<int> oldPrimaryMediumIndex,
        NChunkServer::TChunkReplication* newReplication);

    void SetModified();
    void SuppressModificationTracking();

    void SetAccessed();
    void SuppressAccessTracking();

    virtual bool CanHaveChildren() const;

    virtual void SetChildNode(
        NYTree::INodeFactory* factory,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& child,
        bool recursive);

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Lock);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);

private:
    TCypressNodeBase* DoGetThisImpl();
    TCypressNodeBase* DoLockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false);

    void GatherInheritableAttributes(TCypressNodeBase* parent, TCompositeNodeBase::TAttributes* attributes);
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
    using TNontemplateCypressNodeProxyBase::TNontemplateCypressNodeProxyBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual bool CanHaveChildren() const override;
};

////////////////////////////////////////////////////////////////////////////////

//! A set of inheritable attributes represented as an attribute dictionary.
//! If a setter for a non-inheritable attribute is called, falls back to an ephemeral dictionary.
class TInheritedAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    TInheritedAttributeDictionary(NCellMaster::TBootstrap* bootstrap);

    virtual std::vector<TString> List() const override;
    virtual NYson::TYsonString FindYson(const TString& key) const override;
    virtual void SetYson(const TString& key, const NYson::TYsonString& value) override;
    virtual bool Remove(const TString& key) override;

    TCompositeNodeBase::TAttributes& Attributes();

private:
    const NCellMaster::TBootstrap* Bootstrap_;
    TCompositeNodeBase::TAttributes InheritedAttributes_;
    std::unique_ptr<IAttributeDictionary> Fallback_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class IBase, class TImpl>
class TCypressNodeProxyBase
    : public TBase
    , public virtual IBase
{
public:
    TCypressNodeProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TImpl* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

protected:
    template <class TActualImpl = TImpl>
    TActualImpl* GetThisImpl()
    {
        return TNontemplateCypressNodeProxyBase::GetThisImpl<TActualImpl>();
    }

    template <class TActualImpl = TImpl>
    const TActualImpl* GetThisImpl() const
    {
        return TNontemplateCypressNodeProxyBase::GetThisImpl<TActualImpl>();
    }

    template <class TActualImpl = TImpl>
    TActualImpl* LockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return TNontemplateCypressNodeProxyBase::LockThisImpl<TActualImpl>(request, recursive);
    }

    void ValidateSetCommand() const
    {
        if (TBase::Bootstrap_->GetConfig()->CypressManager->ForbidSetCommand) {
            THROW_ERROR_EXCEPTION("Command \"set\" is forbidden in Cypress, use \"create\" instead");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl>
{
public:
    TScalarNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TScalarNode<TValue>* trunkNode)
        : TBase(
            bootstrap,
            metadata,
            transaction,
            trunkNode)
    { }

    virtual NYTree::ENodeType GetType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

    virtual typename NMpl::TCallTraits<TValue>::TType GetValue() const override
    {
        return this->GetThisImpl()->Value();
    }

    virtual void SetValue(typename NMpl::TCallTraits<TValue>::TType value) override
    {
        this->ValidateValue(value);
        this->LockThisImpl()->Value() = value;
        this->SetModified();
    }

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl> TBase;

    virtual void ValidateValue(typename NMpl::TCallTraits<TValue>::TType /*value*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

#define YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(key) \
    YTREE_NODE_TYPE_OVERRIDES_BASE(key) \
    virtual void SetSelf(TReqSet* request, TRspSet* response, const TCtxSetPtr& context) override \
    { \
        Y_UNUSED(response); \
        context->SetRequestInfo(); \
        ValidateSetCommand(); \
        DoSetSelf<::NYT::NYTree::I##key##Node>(this, NYson::TYsonString(request->value())); \
        context->Reply(); \
    }

#define BEGIN_DEFINE_SCALAR_TYPE(key, type) \
    class T##key##NodeProxy \
        : public TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node> \
    { \
        YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(key) \
    \
    public: \
        T##key##NodeProxy( \
            NCellMaster::TBootstrap* bootstrap, \
            NObjectServer::TObjectTypeMetadata* metadata, \
            NTransactionServer::TTransaction* transaction, \
            TScalarNode<type>* node) \
            : TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node>( \
                bootstrap, \
                metadata, \
                transaction, \
                node) \
        { }

#define END_DEFINE_SCALAR_TYPE(key, type) \
    }; \
    \
    template <> \
    inline ICypressNodeProxyPtr TScalarNodeTypeHandler<type>::DoGetProxy( \
        TScalarNode<type>* node, \
        NTransactionServer::TTransaction* transaction) \
    { \
        return New<T##key##NodeProxy>( \
            Bootstrap_, \
            &Metadata_, \
            transaction, \
            node); \
    }

BEGIN_DEFINE_SCALAR_TYPE(String, TString)
    protected:
        virtual void ValidateValue(const TString& value) override
        {
            auto length = value.length();
            auto limit = Bootstrap_->GetConfig()->CypressManager->MaxStringNodeLength;
            if (length > limit) {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::MaxStringLengthViolation,
                    "String node length limit exceeded: %v > %v",
                    length,
                    limit);
            }
        }
END_DEFINE_SCALAR_TYPE(String, TString)

BEGIN_DEFINE_SCALAR_TYPE(Int64, i64)
END_DEFINE_SCALAR_TYPE(Int64, i64)

BEGIN_DEFINE_SCALAR_TYPE(Uint64, ui64)
END_DEFINE_SCALAR_TYPE(Uint64, ui64)

BEGIN_DEFINE_SCALAR_TYPE(Double, double)
END_DEFINE_SCALAR_TYPE(Double, double)

BEGIN_DEFINE_SCALAR_TYPE(Boolean, bool)
END_DEFINE_SCALAR_TYPE(Boolean, bool)

#undef BEGIN_DEFINE_SCALAR_TYPE
#undef END_DEFINE_SCALAR_TYPE

////////////////////////////////////////////////////////////////////////////////

class TMapNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IMapNode, TMapNode>
    , public NYTree::TMapNodeMixin
{
    using TBase = TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IMapNode, TMapNode>;

    YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(Map)

public:
    using TBase::TBase;

    virtual void Clear() override;
    virtual int GetChildCount() const override;
    virtual std::vector< std::pair<TString, NYTree::INodePtr> > GetChildren() const override;
    virtual std::vector<TString> GetKeys() const override;
    virtual NYTree::INodePtr FindChild(const TString& key) const override;
    virtual bool AddChild(const NYTree::INodePtr& child, const TString& key) override;
    virtual bool RemoveChild(const TString& key) override;
    virtual void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    virtual void RemoveChild(const NYTree::INodePtr& child) override;
    virtual TNullable<TString> FindChildKey(const NYTree::IConstNodePtr& child) override;

private:
    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual void SetChildNode(
        NYTree::INodeFactory* factory,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& child,
        bool recursive) override;

    virtual int GetMaxChildCount() const override;
    virtual int GetMaxKeyLength() const override;

    virtual TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        const NRpc::IServiceContextPtr& context) override;

    virtual void ListSelf(
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;

    virtual void SetRecursive(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    void DoRemoveChild(
        TMapNode* impl,
        const TString& key,
        TCypressNodeBase* childImpl);

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode>
    , public NYTree::TListNodeMixin
{
    using TBase = TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode>;

    YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(List)

public:
    using TBase::TBase;

    virtual void Clear() override;
    virtual int GetChildCount() const override;
    virtual std::vector<NYTree::INodePtr> GetChildren() const override;
    virtual NYTree::INodePtr FindChild(int index) const override;
    virtual void AddChild(const NYTree::INodePtr& child, int beforeIndex = -1) override;
    virtual bool RemoveChild(int index) override;
    virtual void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    virtual void RemoveChild(const NYTree::INodePtr& child) override;
    virtual TNullable<int> FindChildIndex(const NYTree::IConstNodePtr& child) override;

private:
    virtual void SetChildNode(
        NYTree::INodeFactory* factory,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& child,
        bool recursive) override;

    virtual int GetMaxChildCount() const override;

    virtual TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        const NRpc::IServiceContextPtr& context) override;

    virtual void SetRecursive(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TLinkNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TLinkNode>
{
    YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(Entity)

public:
    TLinkNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TLinkNode* trunkNode);

    virtual TResolveResult Resolve(
        const NYPath::TYPath& path,
        const NRpc::IServiceContextPtr& context) override;

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TLinkNode> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;

    bool IsBroken() const;

};

////////////////////////////////////////////////////////////////////////////////

class TDocumentNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TDocumentNode>
{
public:
    TDocumentNodeProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TDocumentNode* trunkNode);

    virtual NYTree::ENodeType GetType() const override;

    virtual TIntrusivePtr<const NYTree::IEntityNode> AsEntity() const override;
    virtual TIntrusivePtr<NYTree::IEntityNode> AsEntity() override;

private:
    typedef TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, NYTree::IEntityNode, TDocumentNode> TBase;

    virtual TResolveResult ResolveRecursive(const NYPath::TYPath& path, const NRpc::IServiceContextPtr& context) override;

    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    virtual void GetRecursive(const NYPath::TYPath& path, TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;

    virtual void SetSelf(TReqSet* request, TRspSet* response, const TCtxSetPtr& context) override;
    virtual void SetRecursive(const NYPath::TYPath& path, TReqSet* request, TRspSet* response, const TCtxSetPtr& context) override;

    virtual void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override;
    virtual void ListRecursive(const NYPath::TYPath& path, TReqList* request, TRspList* response, const TCtxListPtr& context) override;

    virtual void RemoveRecursive(const NYPath::TYPath& path, TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override;

    virtual void ExistsRecursive(const NYPath::TYPath& path, TReqExists* request, TRspExists* response, const TCtxExistsPtr& context) override;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;

    void SetImplValue(const NYson::TYsonString& value);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
