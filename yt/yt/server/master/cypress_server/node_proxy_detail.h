#pragma once

#include "node_detail.h"
#include "node_proxy.h"
#include "config.h"

#include <yt/yt/server/master/cell_master/config.h>

#include <yt/yt/server/master/object_server/public.h>
#include <yt/yt/server/master/object_server/permission_validator.h>

#include <yt/yt/server/master/security_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TNontemplateCypressNodeProxyBase
    : public virtual NYTree::TNodeBase
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

    std::unique_ptr<NYTree::ITransactionalNodeFactory> CreateFactory() const override;
    std::unique_ptr<ICypressNodeFactory> CreateCypressFactory(
        NSecurityServer::TAccount* account,
        const TNodeFactoryOptions& options,
        NYPath::TYPath unresolvedPathSuffix) const override;

    NYPath::TYPath GetPath() const override;

    NTransactionServer::TTransaction* GetTransaction() const override;

    TCypressNode* GetTrunkNode() const override;

    NYTree::ICompositeNodePtr GetParent() const override;
    void SetParent(const NYTree::ICompositeNodePtr& parent) override;

    const NYTree::IAttributeDictionary& Attributes() const override;
    NYTree::IAttributeDictionary* MutableAttributes() override;

    virtual void ValidateStorageParametersUpdate();
    virtual void ValidateLockPossible();

    void GetBasicAttributes(TGetBasicAttributesContext* context) override;

protected:
    class TCustomAttributeDictionary
        : public NYTree::IAttributeDictionary
    {
    public:
        explicit TCustomAttributeDictionary(TNontemplateCypressNodeProxyBase* proxy);

        // IAttributeDictionary members
        std::vector<TString> ListKeys() const override;
        std::vector<TKeyValuePair> ListPairs() const override;
        NYson::TYsonString FindYson(TStringBuf key) const override;
        void SetYson(const TString& key, const NYson::TYsonString& value) override;
        bool Remove(const TString& key) override;

    private:
        TNontemplateCypressNodeProxyBase* const Proxy_;

    };

    using TCustomAttributeDictionaryPtr = TIntrusivePtr<TCustomAttributeDictionary>;

    TCustomAttributeDictionaryPtr CustomAttributesImpl_;

    NTransactionServer::TTransaction* const Transaction_;
    TCypressNode* const TrunkNode_;
    const TVersionedNodeId VersionedId_;

    mutable TCypressNode* CachedNode_ = nullptr;

    bool AccessTrackingSuppressed_ = false;
    bool ExpirationTimeoutRenewalSuppressed_ = false;


    NObjectServer::TVersionedObjectId GetVersionedId() const override;
    NSecurityServer::TAccessControlDescriptor* FindThisAcd() override;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    TFuture<NYson::TYsonString> GetExternalBuiltinAttributeAsync(NYTree::TInternedAttributeKey key);
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    void LogAcdUpdate(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;

    void BeforeInvoke(const NYTree::IYPathServiceContextPtr& context) override;
    void AfterInvoke(const NYTree::IYPathServiceContextPtr& context) override;
    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    void GetSelf(
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;

    void DoRemoveSelf(bool recursive, bool force) override;

    // Suppress access handling in the cases below.
    void GetAttribute(
        const NYTree::TYPath& path,
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;
    void ListAttribute(
        const NYTree::TYPath& path,
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;
    void ExistsSelf(
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;
    void ExistsRecursive(
        const NYTree::TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context) override;
    void ExistsAttribute(
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

    TCompactVector<TCypressNode*, 1> ListDescendantsForPermissionValidation(TCypressNode* node) override;
    TCypressNode* GetParentForPermissionValidation(TCypressNode* node) override;

    // TSupportsPermissions members
    void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission,
        const TString& /*user*/ = "") override;

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

    void SetModified(NObjectServer::EModificationType modificationType) override;

    void SetAccessed() override;
    void SuppressAccessTracking();

    void ValidateMethodWhitelistedForTransaction(const TString& method) const;

    void SetTouched() override;
    void SuppressExpirationTimeoutRenewal();

    virtual bool CanHaveChildren() const;

    void SetChildNode(
        NYTree::INodeFactory* factory,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& child,
        bool recursive) override;

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
    TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const override;
    TIntrusivePtr<NYTree::ICompositeNode> AsComposite() override;

protected:
    using TNontemplateCypressNodeProxyBase::TNontemplateCypressNodeProxyBase;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    bool CanHaveChildren() const override;

private:
    void SetReplicationFactor(int replicationFactor);
    void SetPrimaryMedium(const TString& primaryMediumName);
    void SetMedia(NChunkServer::TSerializableChunkReplication serializableReplication);
    void ThrowReplicationFactorMismatch(int mediumIndex) const;
};

////////////////////////////////////////////////////////////////////////////////

//! A set of inheritable attributes represented as an attribute dictionary.
//! If a setter for a non-inheritable attribute is called, falls back to an ephemeral dictionary.
class TInheritedAttributeDictionary
    : public NYTree::IAttributeDictionary
{
public:
    explicit TInheritedAttributeDictionary(NCellMaster::TBootstrap* bootstrap);

    std::vector<TString> ListKeys() const override;
    std::vector<TKeyValuePair> ListPairs() const override;
    NYson::TYsonString FindYson(TStringBuf key) const override;
    void SetYson(const TString& key, const NYson::TYsonString& value) override;
    bool Remove(const TString& key) override;

    TCompositeNodeBase::TTransientAttributes& Attributes();

private:
    const NCellMaster::TBootstrap* Bootstrap_;
    TCompositeNodeBase::TTransientAttributes InheritedAttributes_;
    NYTree::IAttributeDictionaryPtr Fallback_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TBase, class IBase, class TImpl>
class TCypressNodeProxyBase
    : public TBase
    , public virtual IBase
{
public:
    using TBase::TBase;

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
};

////////////////////////////////////////////////////////////////////////////////

class TSupportsForcefulSetSelfMixin
    : public NYTree::TSupportsSetSelfMixin
{
protected:
    void ValidateSetSelf(bool force) const override;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class IBase, class TImpl>
class TScalarNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl>
    , public NYTree::TSupportsSetSelfMixin
{
private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IBase, TImpl>;

public:
    using TBase::TBase;

    NYTree::ENodeType GetType() const override
    {
        return NDetail::TCypressScalarTypeTraits<TValue>::NodeType;
    }

    typename NMpl::TCallTraits<TValue>::TType GetValue() const override
    {
        return this->GetThisImpl()->Value();
    }

    void SetValue(typename NMpl::TCallTraits<TValue>::TType value) override
    {
        this->ValidateValue(value);
        this->LockThisImpl()->Value() = value;
        this->SetModified(NObjectServer::EModificationType::Content);
    }

private:
    virtual void ValidateValue(typename NMpl::TCallTraits<TValue>::TType /*value*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

#define BEGIN_DEFINE_SCALAR_TYPE(type, cppType) \
    class T##type##NodeProxy \
        : public TScalarNodeProxy<cppType, NYTree::I##type##Node, T##type##Node> \
    { \
    public: \
        YTREE_NODE_TYPE_OVERRIDES(type) \
    \
    public: \
        using TScalarNodeProxy::TScalarNodeProxy;

#define END_DEFINE_SCALAR_TYPE(type, cppType) \
    }; \
    \
    template <> \
    inline ICypressNodeProxyPtr TScalarNodeTypeHandler<cppType>::DoGetProxy( \
        TScalarNode<cppType>* node, \
        NTransactionServer::TTransaction* transaction) \
    { \
        return New<T##type##NodeProxy>( \
            GetBootstrap(), \
            &Metadata_, \
            transaction, \
            node); \
    }

BEGIN_DEFINE_SCALAR_TYPE(String, TString)
    protected:
        void ValidateValue(const TString& value) override
        {
            auto length = std::ssize(value);
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

class TCypressMapNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IMapNode, TCypressMapNode>
    , public NYTree::TMapNodeMixin
    , public TSupportsForcefulSetSelfMixin
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Map)

public:
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

    void Clear() override;
    int GetChildCount() const override;
    std::vector< std::pair<TString, NYTree::INodePtr> > GetChildren() const override;
    std::vector<TString> GetKeys() const override;
    NYTree::INodePtr FindChild(const TString& key) const override;
    bool AddChild(const TString& key, const NYTree::INodePtr& child) override;
    bool RemoveChild(const TString& key) override;
    void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    void RemoveChild(const NYTree::INodePtr& child) override;
    std::optional<TString> FindChildKey(const NYTree::IConstNodePtr& child) override;

protected:
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IMapNode, TCypressMapNode>;

    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    void SetChildNode(
        NYTree::INodeFactory* factory,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& child,
        bool recursive) override;

    int GetMaxChildCount() const override;
    int GetMaxKeyLength() const override;

    TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        const NYTree::IYPathServiceContextPtr& context) override;

    void ListSelf(
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;

    void SetRecursive(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    void DoRemoveChild(
        TCypressMapNode* impl,
        const TString& key,
        TCypressNode* childImpl);
};

////////////////////////////////////////////////////////////////////////////////

class TSequoiaMapNodeProxy
    : public TCypressNodeProxyBase<
        TNontemplateCompositeCypressNodeProxyBase,
        NYTree::IEntityNode,
        TSequoiaMapNode>
    , public virtual NYTree::TSupportsList
{
public:
    using TBase = TCypressNodeProxyBase<
        TNontemplateCompositeCypressNodeProxyBase,
        NYTree::IEntityNode,
        TSequoiaMapNode>;

    YTREE_NODE_TYPE_OVERRIDES(Entity)

public:
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;

    //! Returns the number of child nodes.
    int GetChildCount() const override;

    // Methods below are unimplemented.
    void Clear() override;
    void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    void RemoveChild(const NYTree::INodePtr& child) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode>
    , public NYTree::TListNodeMixin
    , public TSupportsForcefulSetSelfMixin
{
private:
    using TBase = TCypressNodeProxyBase<TNontemplateCompositeCypressNodeProxyBase, NYTree::IListNode, TListNode>;

public:
    YTREE_NODE_TYPE_OVERRIDES(List)

public:
    using TBase::TBase;

    void Clear() override;
    int GetChildCount() const override;
    std::vector<NYTree::INodePtr> GetChildren() const override;
    NYTree::INodePtr FindChild(int index) const override;
    void AddChild(const NYTree::INodePtr& child, int beforeIndex = -1) override;
    bool RemoveChild(int index) override;
    void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    void RemoveChild(const NYTree::INodePtr& child) override;
    std::optional<int> FindChildIndex(const NYTree::IConstNodePtr& child) override;

private:
    void SetChildNode(
        NYTree::INodeFactory* factory,
        const NYPath::TYPath& path,
        const NYTree::INodePtr& child,
        bool recursive) override;

    int GetMaxChildCount() const override;

    TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        const NYTree::IYPathServiceContextPtr& context) override;

    void SetRecursive(
        const NYTree::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
