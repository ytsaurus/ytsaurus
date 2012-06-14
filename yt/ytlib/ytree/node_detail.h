#pragma once

#include "ytree.h"
#include "serialize.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "ypath_detail.h"

#include <ytlib/ytree/ypath.pb.h>

namespace NYT {
namespace NYTree {

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

#define IMPLEMENT_AS_METHODS(name) \
    virtual TIntrusivePtr<I##name##Node> As##name() \
    { \
        ythrow yexception() << Sprintf("Invalid node type: expected: %s, found %s", \
            #name, \
            ~GetType().ToString()); \
    } \
    \
    virtual TIntrusivePtr<const I##name##Node> As##name() const \
    { \
        ythrow yexception() << Sprintf("Invalid node type: expected %s, found %s", \
            #name, \
            ~GetType().ToString()); \
    }

    IMPLEMENT_AS_METHODS(Entity)
    IMPLEMENT_AS_METHODS(Composite)
    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Integer)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)
#undef IMPLEMENT_AS_METHODS

    virtual bool IsWriteRequest(NRpc::IServiceContextPtr context) const;

protected:
    template <class TNode>
    void DoSetSelf(TNode* node, const TYson& value)
    {
        auto factory = CreateFactory();
        auto builder = CreateBuilderFromFactory(~factory);
        TStringInput input(value);
        SetNodeFromProducer(node, ProducerFromYson(&input), ~builder);
    }
    
    virtual void DoInvoke(NRpc::IServiceContextPtr context);
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context);
    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove* context);
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
        TCtxRemove *context);

    virtual void SetRecursive(
        const TYPath& path,
        TReqSet* request,
        TRspSet* response,
        TCtxSet* context);

    virtual void SetRecursive(
        const TYPath& path,
        INode* value) = 0;
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
        const Stroka& verb);

    virtual void ListSelf(
        TReqList* request,
        TRspList* response,
        TCtxList* context);

    virtual void SetRecursive(
        const TYPath& path,
        INode* value);
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual TCompositeNodeMixin
    , public virtual IListNode
{
protected:
    virtual IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const Stroka& verb);

    virtual void SetRecursive(
        const TYPath& path,
        INode* value);

    i64 NormalizeAndCheckIndex(i64 index) const;
};

////////////////////////////////////////////////////////////////////////////////

#define YTREE_NODE_TYPE_OVERRIDES(name) \
public: \
    virtual ::NYT::NYTree::ENodeType GetType() const \
    { \
        return ::NYT::NYTree::ENodeType::name; \
    } \
    \
    virtual TIntrusivePtr<const ::NYT::NYTree::I##name##Node> As##name() const \
    { \
        return this; \
    } \
    \
    virtual TIntrusivePtr< ::NYT::NYTree::I##name##Node > As##name() \
    { \
        return this; \
    } \
    \
    virtual void SetSelf(TReqSet* request, TRspSet* response, TCtxSet* context) \
    { \
        UNUSED(response); \
        DoSetSelf< ::NYT::NYTree::I##name##Node >(this, request->value()); \
        context->Reply(); \
    }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

