#pragma once

#include "node.h"
#include "convert.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ypath_detail.h"

#include <ytlib/ytree/ypath.pb.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void ThrowInvalidNodeType(IConstNodePtr node, ENodeType expectedType, ENodeType actualType);
void ThrowNoSuchChildKey(IConstNodePtr node, const Stroka& key);
void ThrowNoSuchChildIndex(IConstNodePtr node, int index);
void ThrowVerbNotSuppored(const Stroka& verb);
void ThrowVerbNotSuppored(IConstNodePtr node, const Stroka& verb);
void ThrowCannotHaveChildren(IConstNodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public virtual TYPathServiceBase
    , public virtual TSupportsGet
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
    , public virtual TSupportsList
    , public virtual TSupportsExists
    , public virtual INode
{
public:
#define IMPLEMENT_AS_METHODS(key) \
    virtual TIntrusivePtr<I##key##Node> As##key() override \
    { \
        ThrowInvalidNodeType(this, ENodeType::key, GetType()); \
        YUNREACHABLE(); \
    } \
    \
    virtual TIntrusivePtr<const I##key##Node> As##key() const override \
    { \
        ThrowInvalidNodeType(this, ENodeType::key, GetType()); \
        YUNREACHABLE(); \
    }

    IMPLEMENT_AS_METHODS(Entity)
    IMPLEMENT_AS_METHODS(Composite)
    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Integer)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)
#undef IMPLEMENT_AS_METHODS

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const override;

protected:
    template <class TNode>
    void DoSetSelf(TNode* node, const TYsonString& value)
    {
        auto factory = CreateFactory();
        auto builder = CreateBuilderFromFactory(~factory);
        SetNodeFromProducer(node, ConvertToProducer(value), ~builder);
    }
    
    virtual void DoInvoke(NRpc::IServiceContextPtr context) override;

    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override;
    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemovePtr context) override;
    virtual void ExistsSelf(TReqExists* request, TRspExists* response, TCtxExistsPtr context) override;
    virtual void ExistsRecursive(const NYTree::TYPath& path, TReqExists* request, TRspExists* response, TCtxExistsPtr context) override;

    virtual TResolveResult ResolveRecursive(
        const NYPath::TYPath& path,
        NRpc::IServiceContextPtr context) override;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeNodeMixin
    : public virtual TYPathServiceBase
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
    , public virtual ICompositeNode
{
protected:
    virtual void RemoveRecursive(
        const TYPath &path,
        TReqRemove* request,
        TRspRemove* response,
        TCtxRemovePtr context) override;

    virtual void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSetPtr context) override;

    virtual void SetRecursive(
        const TYPath& path,
        INodePtr value) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual IMapNode
    , public virtual TSupportsList
{
protected:
    virtual IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        NRpc::IServiceContextPtr context) override;

    virtual void ListSelf(
        TReqList* request,
        TRspList* response,
        TCtxListPtr context) override;

    virtual void SetRecursive(
        const TYPath& path,
        INodePtr value) override;
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual IListNode
{
protected:
    virtual IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        NRpc::IServiceContextPtr context) override;

    void SetRecursive(
        const TYPath& path,
        INodePtr value);

};

////////////////////////////////////////////////////////////////////////////////

#define YTREE_NODE_TYPE_OVERRIDES(key) \
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
    } \
    \
    virtual void SetSelf(TReqSet* request, TRspSet* response, TCtxSetPtr context) override \
    { \
        UNUSED(response); \
        DoSetSelf< ::NYT::NYTree::I##key##Node >(this, NYTree::TYsonString(request->value())); \
        context->Reply(); \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

