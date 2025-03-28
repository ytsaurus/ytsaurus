#pragma once

#include "public.h"
#include "map_object.h"
#include "object_detail.h"

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/object_server/permission_validator.h>

#include <yt/yt/core/ytree/node_detail.h>
#include <yt/yt/core/ytree/ypath_detail.h>

#include <yt/yt/ytlib/cypress_client/proto/cypress_ypath.pb.h>


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
    , public THierarchicPermissionValidator<TObject*, NObjectServer::TObject*>
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
    std::vector<std::pair<std::string, NYTree::INodePtr>> GetChildren() const override;
    std::vector<std::string> GetKeys() const override;
    NYTree::INodePtr FindChild(const std::string& key) const override;
    std::optional<std::string> FindChildKey(const NYTree::IConstNodePtr& child) override;

    bool AddChild(const std::string& key, const NYTree::INodePtr& child) override;
    void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    void RemoveChild(const NYTree::INodePtr& child) override;
    bool RemoveChild(const std::string& key) override;

    std::unique_ptr<NYTree::ITransactionalNodeFactory> CreateFactory() const override;

    void Clear() override;

    TSelfPtr Create(
        EObjectType type,
        const NYPath::TYPath& path,
        NYTree::IAttributeDictionary* attributes);
    TSelfPtr Copy(
        const NYPath::TYPath& sourcePath,
        const NYPath::TYPath& targetPath,
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

    virtual TSelfPtr ResolveNameOrThrow(const std::string& name) = 0;

    TCompactVector<NObjectServer::TObject*, 1> ListDescendantsForPermissionValidation(TObject* object) override;
    NObjectServer::TObject* GetParentForPermissionValidation(TObject* object) override;

    void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission,
        const std::string& user = {}) override;

    using THierarchicPermissionValidator<TObject*, NObjectServer::TObject*>::ValidatePermission;

    virtual void ValidateBeforeAttachChild(
        const std::string& key,
        const TSelfPtr& childProxy);
    virtual void ValidateAfterAttachChild(
        const std::string& key,
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
    void AttachChild(const std::string& key, const TSelfPtr& childProxy) noexcept;
    //! Detaches a child object without removing it. It's implied that no validations are needed for this.
    void DetachChild(const TSelfPtr& childProxy) noexcept;

    void RemoveChildren();

    void ValidateChildName(const std::string& childName);
    virtual void ValidateChildNameAvailability(const std::string& childName);

    void RenameSelf(const std::string& newName);
    virtual void DoRenameSelf(const std::string& newName);
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

    virtual void AttachChild(const TProxyPtr& parent, const std::string& key, const TProxyPtr& child);
    virtual void DetachChild(const TProxyPtr& parent, const TProxyPtr& child);

protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    std::vector<TObject*> CreatedObjects_;

    struct TFactoryEvent
    {
        EEventType Type;
        TProxyPtr Parent;
        std::string Key;
        TProxyPtr Child;
    };

    std::vector<TFactoryEvent> EventLog_;

    virtual void RemoveCreatedObjects();

    //! NB: In case of failure, the implementation must not leave any unattended objects.
    virtual TObject* DoCreateObject(NYTree::IAttributeDictionary* attributes) = 0;

    void LogEvent(const TFactoryEvent& event);
    virtual void CommitEvent(const TFactoryEvent& event);
    virtual void RollbackEvent(const TFactoryEvent& event);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
