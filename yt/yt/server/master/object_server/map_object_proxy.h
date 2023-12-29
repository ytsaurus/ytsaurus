#pragma once

#include "public.h"
#include "map_object.h"
#include "object_detail.h"

#include <yt/yt/core/ytree/node_detail.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>

#include <yt/yt/server/master/cypress_server/public.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TNonversionedMapObjectFactoryBase;

template <class TObject>
class TNonversionedMapObjectTypeHandlerBase;

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
class TNonversionedMapObjectProxyBase
    : public TNonversionedObjectProxyBase<TObject>
    , public THierarchicPermissionValidator<TObject>
    , public virtual NYTree::TMapNodeMixin
    , public virtual NYTree::TNodeBase
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Map)

private:
    friend class TNonversionedMapObjectFactoryBase<TObject>;

    using TBase = TNonversionedObjectProxyBase<TObject>;
    using TSelf = TNonversionedMapObjectProxyBase<TObject>;
    using TSelfPtr = TIntrusivePtr<TSelf>;
    using TTypeHandler = TNonversionedMapObjectTypeHandlerBase<TObject>;
    using TTypeHandlerPtr = TIntrusivePtr<TTypeHandler>;

public:
    TNonversionedMapObjectProxyBase(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TObject* object);

    TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const override;
    TIntrusivePtr<NYTree::ICompositeNode> AsComposite() override;

    NYTree::TYPath GetPath() const override;

    NYTree::ICompositeNodePtr GetParent() const override;
    void SetParent(const NYTree::ICompositeNodePtr& parent) override;

    int GetChildCount() const override;
    std::vector<std::pair<TString, NYTree::INodePtr>> GetChildren() const override;
    std::vector<TString> GetKeys() const override;
    NYTree::INodePtr FindChild(const TString& key) const override;
    std::optional<TString> FindChildKey(const NYTree::IConstNodePtr& child) override;

    bool AddChild(const TString& key, const NYTree::INodePtr& child) override;
    void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    void RemoveChild(const NYTree::INodePtr& child) override;
    bool RemoveChild(const TString& key) override;

    std::unique_ptr<NYTree::ITransactionalNodeFactory> CreateFactory() const override;

    void Clear() override;

    TSelfPtr Create(
        EObjectType type,
        const TString& path,
        NYTree::IAttributeDictionary* attributes);
    TSelfPtr Copy(
        const TString& sourcePath,
        const TString& targetPath,
        NCypressClient::ENodeCloneMode mode,
        bool ignoreExisting);

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);

protected:
    static TIntrusivePtr<const TSelf> FromNode(const NYTree::IConstNodePtr& node);
    static TSelfPtr FromNode(const NYTree::INodePtr& node);

    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    NYTree::IYPathService::TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        const NYTree::IYPathServiceContextPtr& context) override;

    void GetSelf(
        TReqGet* request,
        TRspGet* response,
        const TCtxGetPtr& context) override;
    void RemoveSelf(
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;
    void SetSelf(
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;
    void SetRecursive(
        const NYPath::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    TTypeHandlerPtr GetTypeHandler() const;

    TSelfPtr GetProxy(TObject* object) const;
    NYPath::TYPath GetShortPath() const;
    NObjectServer::TObject* ResolvePathToNonversionedObject(const NYPath::TYPath& path) const;

    virtual TSelfPtr ResolveNameOrThrow(const TString& name) = 0;

    TCompactVector<TObject*, 1> ListDescendantsForPermissionValidation(TObject* object) override;
    TObject* GetParentForPermissionValidation(TObject* object) override;

    void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission,
        const TString& user = {}) override;

    using THierarchicPermissionValidator<TObject>::ValidatePermission;

    virtual void ValidateBeforeAttachChild(
        const TString& key,
        const TSelfPtr& childProxy);
    virtual void ValidateAfterAttachChild(
        const TString& key,
        const TSelfPtr& childProxy);
    virtual void ValidateAttachChildDepth(const TSelfPtr& child);
    virtual void ValidateAttachChildSubtreeSize(const TSelfPtr& child);
    void ValidateRemoval() override;

    // XXX(kiselyovp) These methods have total complexity of O(depth_limit + subtree_size) and get called
    // on each call of Create and Move verbs. Those calls are not expected to be common.
    int GetDepth(const TObject* object) const;
    void ValidateHeightLimit(const TObject* root, int heightLimit) const;

    int GetTopmostAncestorSubtreeSize(const TObject* object) const;

    virtual std::unique_ptr<TNonversionedMapObjectFactoryBase<TObject>> CreateObjectFactory() const = 0;

    void SetImmediateChild(
        TNonversionedMapObjectFactoryBase<TObject>* factory,
        const NYPath::TYPath& path,
        const TSelfPtr& child);

    void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;

    virtual TSelfPtr DoGetParent() const;

    //! This method doesn't validate ref counts.
    virtual void DoRemoveChild(const TSelfPtr& childProxy);

    //! Attaches a child object without validations.
    void AttachChild(const TString& key, const TSelfPtr& childProxy) noexcept;
    //! Detaches a child object without removing it. It's implied that no validations are needed for this.
    void DetachChild(const TSelfPtr& childProxy) noexcept;

    void RemoveChildren();

    void ValidateChildName(const TString& childName);
    virtual void ValidateChildNameAvailability(const TString& childName);

    void RenameSelf(const TString& newName);
    virtual void DoRenameSelf(const TString& newName);
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMapObjectFactoryEventType,
    (RefObject)
    (AttachChild)
    (DetachChild)
);

//! Designed to work with TNonversionedMapObjectProxyBase.
//! Supports transactional semantics for its AttachChild and DetachChild methods.
template <class TObject>
class TNonversionedMapObjectFactoryBase
{
protected:
    using TProxy = TNonversionedMapObjectProxyBase<TObject>;
    using TProxyPtr = TIntrusivePtr<TProxy>;
    using EEventType = EMapObjectFactoryEventType;

public:
    explicit TNonversionedMapObjectFactoryBase(NCellMaster::TBootstrap* bootstrap);
    virtual ~TNonversionedMapObjectFactoryBase();

    virtual TObject* CreateObject(NYTree::IAttributeDictionary* attributes);
    virtual TObject* CloneObject(
        TObject* object,
        NCypressServer::ENodeCloneMode mode);

    virtual void Commit();
    virtual void Rollback();

    virtual void AttachChild(const TProxyPtr& parent, const TString& key, const TProxyPtr& child);
    virtual void DetachChild(const TProxyPtr& parent, const TProxyPtr& child);

protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    std::vector<TObject*> CreatedObjects_;

    struct TFactoryEvent
    {
        EEventType Type;
        TProxyPtr Parent;
        TString Key;
        TProxyPtr Child;
    };

    std::vector<TFactoryEvent> EventLog_;

    virtual void RemoveCreatedObjects();

    //! NB: in case of failure, the implementation must not leave any unattended objects.
    virtual TObject* DoCreateObject(NYTree::IAttributeDictionary* attributes) = 0;

    void LogEvent(const TFactoryEvent& event);
    virtual void CommitEvent(const TFactoryEvent& event);
    virtual void RollbackEvent(const TFactoryEvent& event);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
