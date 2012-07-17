#pragma once

#include "ytree.h"
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
void ThrowVerbNotSuppored(IConstNodePtr node, const Stroka& verb);
void ThrowCannotHaveChildren(IConstNodePtr node);

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public virtual TYPathServiceBase
    , public virtual TSupportsGet
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
    , public virtual TSupportsList
    , public virtual INode
{
public:
    typedef TIntrusivePtr<TNodeBase> TPtr;

#define IMPLEMENT_AS_METHODS(key) \
    virtual TIntrusivePtr<I##key##Node> As##key() OVERRIDE \
    { \
        ThrowInvalidNodeType(this, ENodeType::key, GetType()); \
        YUNREACHABLE(); \
    } \
    \
    virtual TIntrusivePtr<const I##key##Node> As##key() const OVERRIDE \
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

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const OVERRIDE;

protected:
    template <class TNode>
    void DoSetSelf(TNode* node, const TYsonString& value)
    {
        auto factory = CreateFactory();
        auto builder = CreateBuilderFromFactory(~factory);
        SetNodeFromProducer(node, ConvertToProducer(value), ~builder);
    }
    
    virtual void DoInvoke(NRpc::IServiceContextPtr context) OVERRIDE;
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context) OVERRIDE;
    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove* context) OVERRIDE;

    virtual TResolveResult ResolveRecursive(const NYTree::TYPath& path, const Stroka& verb) OVERRIDE;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeNodeMixin
    : public virtual ICompositeNode
    , public virtual TSupportsSet
    , public virtual TSupportsRemove
{
protected:
    virtual void RemoveRecursive(
        const TYPath &path,
        TReqRemove *request,
        TRspRemove *response,
        TCtxRemove *context) OVERRIDE;

    virtual void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSet* context) OVERRIDE;

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
    IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const Stroka& verb);

    void ListSelf(
        TReqList* request,
        TRspList* response,
        TCtxList* context);

    void SetRecursive(
        const TYPath& path,
        INodePtr value);
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual IListNode
{
protected:
    IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const Stroka& verb);

    void SetRecursive(
        const TYPath& path,
        INodePtr value);

};

////////////////////////////////////////////////////////////////////////////////

#define YTREE_NODE_TYPE_OVERRIDES(key) \
public: \
    virtual ::NYT::NYTree::ENodeType GetType() const \
    { \
        return ::NYT::NYTree::ENodeType::key; \
    } \
    \
    virtual TIntrusivePtr<const ::NYT::NYTree::I##key##Node> As##key() const \
    { \
        return this; \
    } \
    \
    virtual TIntrusivePtr< ::NYT::NYTree::I##key##Node > As##key() \
    { \
        return this; \
    } \
    \
    virtual void SetSelf(TReqSet* request, TRspSet* response, TCtxSet* context) \
    { \
        UNUSED(response); \
        DoSetSelf< ::NYT::NYTree::I##key##Node >(this, NYTree::TYsonString(request->value())); \
        context->Reply(); \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

