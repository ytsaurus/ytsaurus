#pragma once

#include "node_detail.h"
#include "node_proxy.h"

#include <yt/server/master/cell_master/config.h>

#include <yt/server/master/object_server/public.h>
#include <yt/server/master/object_server/permission_validator.h>

#include <yt/server/master/security_server/public.h>

#include <yt/server/master/transaction_server/public.h>

#include <yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/core/ytree/node.h>
#include <yt/core/ytree/ypath_client.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeProxyBase
    : public NYTree::TNodeBase
    , public NObjectServer::TObjectProxyBase
    , public NObjectServer::THierarchicPermissionValidator<TCypressNode>
    , public ICypressNodeProxy
{
public:
    TNontemplateCypressNodeProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        NTransactionServer::TTransaction* transaction,
        TCypressNode* trunkNode);

    virtual std::unique_ptr<NYTree::ITransactionalNodeFactory> CreateFactory() const override;
    virtual std::unique_ptr<ICypressNodeFactory> CreateCypressFactory(
        NSecurityServer::TAccount* account,
        const TNodeFactoryOptions& options) const override;

    virtual NYPath::TYPath GetPath() const override;

    virtual NTransactionServer::TTransaction* GetTransaction() const override;

    virtual TCypressNode* GetTrunkNode() const override;

    virtual NYTree::ICompositeNodePtr GetParent() const override;
    virtual void SetParent(const NYTree::ICompositeNodePtr& parent) override;

    virtual const NYTree::IAttributeDictionary& Attributes() const override;
    virtual NYTree::IAttributeDictionary* MutableAttributes() override;

    virtual void ValidateStorageParametersUpdate();
    virtual void ValidateLockPossible();

protected:
    class TCustomAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        explicit TCustomAttributeDictionary(TNontemplateCypressNodeProxyBase* proxy);

        // IAttributeDictionary members
        virtual std::vector<TString> ListKeys() const override;
        virtual std::vector<TKeyValuePair> ListPairs() const override;
        virtual NYson::TYsonString FindYson(TStringBuf key) const override;
        virtual void SetYson(const TString& key, const NYson::TYsonString& value) override;
        virtual bool Remove(const TString& key) override;

    private:
        TNontemplateCypressNodeProxyBase* const Proxy_;

    } CustomAttributesImpl_;

    class TResourceUsageVisitor;

    NTransactionServer::TTransaction* const Transaction_;
    TCypressNode* const TrunkNode_;

    mutable TCypressNode* CachedNode_ = nullptr;

    bool AccessTrackingSuppressed_ = false;
    bool ModificationTrackingSuppressed_ = false;


    virtual NObjectServer::TVersionedObjectId GetVersionedId() const override;
    virtual NSecurityServer::TAccessControlDescriptor* FindThisAcd() override;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    TFuture<NYson::TYsonString> GetExternalBuiltinAttributeAsync(NYTree::TInternedAttributeKey key);
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual TCypressNode* FindClosestAncestorWithAnnotation(TCypressNode* node);

    virtual void BeforeInvoke(const NRpc::IServiceContextPtr& context) override;
    virtual void AfterInvoke(const NRpc::IServiceContextPtr& context) override;
    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual void GetSelf(
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;

    virtual void DoRemoveSelf() override;

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

    TCypressNode* GetImpl(TCypressNode* trunkNode) const;

    TCypressNode* LockImpl(
        TCypressNode* trunkNode,
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false) const;

    template <class TImpl = TCypressNode>
    TImpl* GetThisImpl()
    {
        return DoGetThisImpl()->As<TImpl>();
    }

    template <class TImpl = TCypressNode>
    const TImpl* GetThisImpl() const
    {
        return const_cast<TNontemplateCypressNodeProxyBase*>(this)->GetThisImpl<TImpl>();
    }

    template <class TImpl = TCypressNode>
    TImpl* LockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false)
    {
        return DoLockThisImpl(request, recursive)->As<TImpl>();
    }


    ICypressNodeProxyPtr GetProxy(TCypressNode* trunkNode) const;

    virtual SmallVector<TCypressNode*, 1> ListDescendants(TCypressNode* node) override;

    // TSupportsPermissions members
    virtual void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission,
        const TString& /* user */ = "") override;

    // Inject other overloads into the scope.
    using THierarchicPermissionValidator<TCypressNode>::ValidatePermission;
    using TObjectProxyBase::ValidatePermission;

    void ValidateNotExternal();

    // If some argument is null, eschews corresponding parts of validation.
    void ValidateMediaChange(
        const std::optional<NChunkServer::TChunkReplication>& oldReplication,
        std::optional<int> primaryMediumIndex,
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
        std::optional<int> oldPrimaryMediumIndex,
        NChunkServer::TChunkReplication* newReplication);

    void SetModified(EModificationType modificationType = EModificationType::Content);
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
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Unlock);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, BeginCopy);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, EndCopy);

private:
    TCypressNode* DoGetThisImpl();
    TCypressNode* DoLockThisImpl(
        const TLockRequest& request = ELockMode::Exclusive,
        bool recursive = false);

    void GatherInheritableAttributes(
        TCypressNode* parent,
        TCompositeNodeBase::TAttributes* attributes);

    template <class TContextPtr, class TClonedTreeBuilder>
    void CopyCore(
        const TContextPtr& context,
        bool inplace,
        const TClonedTreeBuilder& clonedTreeBuilder);

    void ValidateAccessTransaction();
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
    explicit TInheritedAttributeDictionary(NCellMaster::TBootstrap* bootstrap);

    virtual std::vector<TString> ListKeys() const override;
    virtual std::vector<TKeyValuePair> ListPairs() const override;
    virtual NYson::TYsonString FindYson(TStringBuf key) const override;
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

    void ValidateSetCommand(const NYTree::TYPath& path, const TString& user, bool force) const
    {
        const auto& Logger = CypressServerLogger;
        bool forbidden = TBase::GetDynamicCypressManagerConfig()->ForbidSetCommand && !force;
        if (path && !force) {
            YT_LOG_DEBUG("Validating possibly malicious \"set\" in Cypress (Path: %v, User: %v, Forbidden: %v)",
                path,
                user,
                forbidden);
        }
        if (forbidden) {
            THROW_ERROR_EXCEPTION("Command \"set\" is forbidden in Cypress, use \"create\" instead");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl>
{
private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl>;

public:
    using TBase::TBase;

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
        ValidateSetCommand(GetPath(), context->GetUser(), request->force()); \
        DoSetSelf<::NYT::NYTree::I##key##Node>(this, NYson::TYsonString(request->value())); \
        context->Reply(); \
    }

#define BEGIN_DEFINE_SCALAR_TYPE(key, type) \
    class T##key##NodeProxy \
        : public TScalarNodeProxy<type, NYTree::I##key##Node, T##key##Node> \
    { \
        YTREE_NODE_TYPE_OVERRIDES(key) \
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
            auto limit = GetDynamicCypressManagerConfig()->MaxStringNodeLength;
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
private:
    using TBase = TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IMapNode, TMapNode>;

public:
    YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(Map)

public:
    using TBase::TBase;

    virtual void Clear() override;
    virtual int GetChildCount() const override;
    virtual std::vector< std::pair<TString, NYTree::INodePtr> > GetChildren() const override;
    virtual std::vector<TString> GetKeys() const override;
    virtual NYTree::INodePtr FindChild(const TString& key) const override;
    virtual bool AddChild(const TString& key, const NYTree::INodePtr& child) override;
    virtual bool RemoveChild(const TString& key) override;
    virtual void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    virtual void RemoveChild(const NYTree::INodePtr& child) override;
    virtual std::optional<TString> FindChildKey(const NYTree::IConstNodePtr& child) override;

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
        TCypressNode* childImpl);

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode>
    , public NYTree::TListNodeMixin
{
private:
    using TBase = TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode>;

public:
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
    virtual std::optional<int> FindChildIndex(const NYTree::IConstNodePtr& child) override;

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

} // namespace NYT::NCypressServer
