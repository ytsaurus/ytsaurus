#pragma once

#include "public.h"
#include "map_object.h"
#include "object_detail.h"

#include <yt/core/ytree/node_detail.h>
#include <yt/core/ytree/ypath_detail.h>

#include <yt/ytlib/cypress_client/cypress_ypath.pb.h>

#include <yt/server/master/cypress_server/public.h>

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
    , public virtual NYTree::TMapNodeMixin
    , public virtual NYTree::TNodeBase
{
    YTREE_NODE_TYPE_OVERRIDES_BASE(Map)

private:
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

    static TSelfPtr GetProxy(NCellMaster::TBootstrap* bootstrap, TObject* object);

    TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const;
    TIntrusivePtr<NYTree::ICompositeNode> AsComposite();

    virtual NYTree::TYPath GetPath() const override;

    virtual NYTree::ICompositeNodePtr GetParent() const override;
    virtual void SetParent(const NYTree::ICompositeNodePtr& parent);

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;
    virtual NYTree::IYPathService::TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        const NRpc::IServiceContextPtr& context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    virtual void RemoveSelf(
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    virtual int GetChildCount() const override;
    virtual std::vector<std::pair<TString, NYTree::INodePtr>> GetChildren() const override;
    virtual std::vector<TString> GetKeys() const override;
    virtual NYTree::INodePtr FindChild(const TString& key) const override;
    virtual std::optional<TString> FindChildKey(const NYTree::IConstNodePtr& child) override;

    virtual bool AddChild(const TString& key, const NYTree::INodePtr& child) override;
    virtual void ReplaceChild(const NYTree::INodePtr& oldChild, const NYTree::INodePtr& newChild) override;
    virtual void RemoveChild(const NYTree::INodePtr& child) override;
    virtual bool RemoveChild(const TString& key) override;

    virtual std::unique_ptr<NYTree::ITransactionalNodeFactory> CreateFactory() const override;

    virtual void Clear() override;

    virtual void SetSelf(TReqSet* request, TRspSet* response, const TCtxSetPtr& context) override;
    virtual void SetRecursive(
        const NYPath::TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    TSelfPtr Create(
        EObjectType type,
        const TString& path,
        NYTree::IAttributeDictionary* attributes);

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Create);
    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Copy);

protected:
    static TIntrusivePtr<const TSelf> FromNode(const NYTree::IConstNodePtr& node);
    static TSelfPtr FromNode(const NYTree::INodePtr& node);
    static TIntrusivePtr<const TSelf> FromNodeOrThrow(const NYTree::IConstNodePtr& node);
    static TSelfPtr FromNodeOrThrow(const NYTree::INodePtr& node);

    TTypeHandlerPtr GetTypeHandler() const;

    TSelfPtr GetProxy(TObject* object) const;
    NYPath::TYPath GetShortPath() const;
    NObjectServer::TObject* ResolvePathToNonversionedObject(const NYPath::TYPath& path) const;

    virtual TSelfPtr ResolveNameOrThrow(const TString& name) = 0;

    virtual void ValidatePermission(
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission,
        const TString& user = "") override;

    void ValidatePermission(
        TObject* object,
        NYTree::EPermissionCheckScope scope,
        NYTree::EPermission permission);

    virtual void ValidateBeforeAttachChild(
        const TString& key,
        const TSelfPtr& childProxy);
    virtual void ValidateAfterAttachChild(
        const TString& key,
        const TSelfPtr& childProxy);
    virtual void ValidateAttachChildDepth(const TSelfPtr& child);
    virtual void ValidateRemoval() override;

    virtual std::unique_ptr<TNonversionedMapObjectFactoryBase<TObject>> DoCreateFactory() const = 0;

    void SetImmediateChild(
        TNonversionedMapObjectFactoryBase<TObject>* factory,
        const NYPath::TYPath& path,
        const TSelfPtr& child);

    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override;

    virtual TSelfPtr DoGetParent() const;

    //! This method doesn't validate ref counts.
    virtual void DoRemoveChild(const TSelfPtr& childProxy);

    //! Attaches a child object without validations.
    void AttachChild(const TString& key, const TSelfPtr& childProxy) noexcept;
    //! Detaches a child object without removing it. It's implied that no validations are needed for this.
    void DetachChild(const TSelfPtr& childProxy) noexcept;

    void RemoveChildren();

    virtual void ValidateChildName(const TString& newChildName);

    void RenameSelf(const TString& newName);
    virtual void DoRenameSelf(const TString& newName);

    friend class TNonversionedMapObjectFactoryBase<TObject>;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFactoryEventType,
    (RefObject)
    (AttachChild)
    (DetachChild)
);

//! Is designed to work with TNonversionedMapObjectProxyBase.
//! Supports transactional semantics for its AttachChild and DetachChild methods.
template <class TObject>
class TNonversionedMapObjectFactoryBase
{
protected:
    using TProxy = TNonversionedMapObjectProxyBase<TObject>;
    using TProxyPtr = TIntrusivePtr<TProxy>;

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
    // YYY(kiselyovp) make sure that the whole transactional semantics actually works (e.g. all the logged events are "atomic")
    struct TFactoryEvent
    {
        EFactoryEventType Type;
        TProxyPtr Parent;
        TString Key;
        TProxyPtr Child;
    };

    NCellMaster::TBootstrap* Bootstrap_;
    std::vector<TObject*> CreatedObjects_;
    std::vector<TFactoryEvent> EventLog_;

    virtual void CleanupCreatedObjects(bool removeObjects);

    //! NB: in case of failure, the implementation must not leave any unattended objects
    virtual TObject* DoCreateObject(NYTree::IAttributeDictionary* attributes) = 0;

    void LogEvent(const TFactoryEvent& event);
    virtual void CommitEvent(const TFactoryEvent& event);
    virtual void RollbackEvent(const TFactoryEvent& event);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define MAP_OBJECT_PROXY_INL_H_
#include "map_object_proxy-inl.h"
#undef MAP_OBJECT_PROXY_INL_H_
