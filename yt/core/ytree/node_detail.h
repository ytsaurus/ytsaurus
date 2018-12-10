#pragma once

#include "convert.h"
#include "exception_helpers.h"
#include "node.h"
#include "permission.h"
#include "tree_builder.h"
#include "ypath_detail.h"
#include "ypath_service.h"

#include <yt/core/ytree/proto/ypath.pb.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public virtual TYPathServiceBase
    , public virtual TSupportsGetKey
    , public virtual TSupportsGet
    , public virtual TSupportsSet
    , public virtual TSupportsMultiset
    , public virtual TSupportsRemove
    , public virtual TSupportsList
    , public virtual TSupportsExists
    , public virtual TSupportsPermissions
    , public virtual INode
{
public:
#define IMPLEMENT_AS_METHODS(key) \
    virtual TIntrusivePtr<I##key##Node> As##key() override \
    { \
        ThrowInvalidNodeType(this, ENodeType::key, GetType()); \
        Y_UNREACHABLE(); \
    } \
    \
    virtual TIntrusivePtr<const I##key##Node> As##key() const override \
    { \
        ThrowInvalidNodeType(this, ENodeType::key, GetType()); \
        Y_UNREACHABLE(); \
    }

    IMPLEMENT_AS_METHODS(Entity)
    IMPLEMENT_AS_METHODS(Composite)
    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Int64)
    IMPLEMENT_AS_METHODS(Uint64)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(Boolean)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)
#undef IMPLEMENT_AS_METHODS

    virtual TResolveResult ResolveRecursive(const NYPath::TYPath& path, const NRpc::IServiceContextPtr& context) override;

    virtual TYPath GetPath() const override;

protected:
    template <class TNode>
    void DoSetSelf(TNode* node, const NYson::TYsonString& value)
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Write);
        ValidatePermission(EPermissionCheckScope::Descendants, EPermission::Remove);

        auto factory = CreateFactory();
        auto builder = CreateBuilderFromFactory(factory.get());
        SetNodeFromProducer(node, ConvertToProducer(value), builder.get());
        factory->Commit();
    }

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual void GetKeySelf(TReqGetKey* request, TRspGetKey* response, const TCtxGetKeyPtr& context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override;
};

////////////////////////////////////////////////////////////////////////////////

class TCompositeNodeMixin
    : public virtual TYPathServiceBase
    , public virtual TSupportsSet
    , public virtual TSupportsMultiset
    , public virtual TSupportsRemove
    , public virtual TSupportsPermissions
    , public virtual ICompositeNode
{
protected:
    virtual void RemoveRecursive(
        const TYPath &path,
        TReqRemove* request,
        TRspRemove* response,
        const TCtxRemovePtr& context) override;

    virtual void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        const TCtxSetPtr& context) override;

    virtual void SetChild(
        INodeFactory* factory,
        const TYPath& path,
        INodePtr child,
        bool recursive) = 0;

    virtual int GetMaxChildCount() const;

    void ValidateChildCount(const TYPath& path, int childCount) const;
};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual TSupportsList
    , public virtual IMapNode
{
protected:
    virtual IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const NRpc::IServiceContextPtr& context) override;

    virtual void ListSelf(
        TReqList* request,
        TRspList* response,
        const TCtxListPtr& context) override;

    virtual void SetChild(
        INodeFactory* factory,
        const TYPath& path,
        INodePtr child,
        bool recursive) override;

    virtual int GetMaxKeyLength() const;

    virtual void SetChildren(TReqMultiset* request, TRspMultiset* response) override;

private:
    void ThrowMaxKeyLengthViolated() const;

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual IListNode
{
protected:
    virtual IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const NRpc::IServiceContextPtr& context) override;

    virtual void SetChild(
        INodeFactory* factory,
        const TYPath& path,
        INodePtr child,
        bool recursive) override;

};

////////////////////////////////////////////////////////////////////////////////

class TNonexistingService
    : public TYPathServiceBase
    , public TSupportsExists
{
public:
    static IYPathServicePtr Get();

private:
    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context);

    virtual TResolveResult Resolve(
        const TYPath& path,
        const NRpc::IServiceContextPtr& context);

    virtual void ExistsSelf(
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context);

    virtual void ExistsRecursive(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context);

    virtual void ExistsAttribute(
        const TYPath& path,
        TReqExists* request,
        TRspExists* response,
        const TCtxExistsPtr& context);

    void ExistsAny(const TCtxExistsPtr& context);
};

////////////////////////////////////////////////////////////////////////////////

#define YTREE_NODE_TYPE_OVERRIDES_BASE(key) \
public: \
    virtual ::NYT::NYTree::ENodeType GetType() const override \
    { \
        return ::NYT::NYTree::ENodeType::key; \
    } \
    \
    virtual TIntrusivePtr<const ::NYT::NYTree::I##key##Node> As##key() const override \
    { \
        return this; \
    } \
    \
    virtual TIntrusivePtr< ::NYT::NYTree::I##key##Node > As##key() override \
    { \
        return this; \
    }

#define YTREE_NODE_TYPE_OVERRIDES(key) \
    YTREE_NODE_TYPE_OVERRIDES_BASE(key) \
protected: \
    virtual void SetSelf(TReqSet* request, TRspSet* response, const TCtxSetPtr& context) override \
    { \
        Y_UNUSED(response); \
        context->SetRequestInfo(); \
        DoSetSelf<::NYT::NYTree::I##key##Node>(this, NYson::TYsonString(request->value())); \
        context->Reply(); \
    }

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ENodeFactoryState,
    (Active)
    (Committed)
    (RolledBack)
);

class TTransactionalNodeFactoryBase
    : public virtual ITransactionalNodeFactory
{
public:
    virtual ~TTransactionalNodeFactoryBase();

    virtual void Commit() noexcept override;
    virtual void Rollback() noexcept override;

protected:
    void RollbackIfNeeded();

private:
    using EState = ENodeFactoryState;
    EState State_ = EState::Active;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

