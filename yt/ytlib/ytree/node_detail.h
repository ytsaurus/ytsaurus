#pragma once

#include "common.h"
#include "ytree.h"
#include "ypath.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public virtual IYPathService
    , public virtual INode
{
public:
    typedef TIntrusivePtr<TNodeBase> TPtr;

#define IMPLEMENT_AS_METHODS(name) \
    virtual TIntrusivePtr<I##name##Node> As##name() \
    { \
        YUNREACHABLE(); \
    } \
    \
    virtual TIntrusiveConstPtr<I##name##Node> As##name() const \
    { \
        YUNREACHABLE(); \
    }

    IMPLEMENT_AS_METHODS(Entity)
    IMPLEMENT_AS_METHODS(Composite)

    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Int64)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)

#undef IMPLEMENT_AS_METHODS

    virtual TNavigateResult Navigate(TYPath path);

    virtual TGetResult Get(TYPath path, IYsonConsumer* consumer);

    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer);

    virtual TRemoveResult Remove(TYPath path);

    virtual TLockResult Lock(TYPath path);

protected:
    virtual TNavigateResult NavigateRecursive(TYPath path);

    virtual TRemoveResult RemoveSelf();
    virtual TRemoveResult RemoveRecursive(TYPath path);

    virtual TGetResult GetSelf(IYsonConsumer* consumer);
    virtual TGetResult GetRecursive(TYPath path, IYsonConsumer* consumer);

    virtual TSetResult SetSelf(TYsonProducer::TPtr producer);
    virtual TSetResult SetRecursive(TYPath path, TYsonProducer::TPtr producer);

    virtual TLockResult LockSelf();
    virtual TLockResult LockRecursive(TYPath path);
    
    virtual yvector<Stroka> GetVirtualAttributeNames();
    virtual bool GetVirtualAttribute(const Stroka& name, IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeMixin
    : public virtual IMapNode
{
protected:
    IYPathService::TNavigateResult NavigateRecursive(TYPath path);

    IYPathService::TSetResult SetRecursive(
        TYPath path,
        TYsonProducer::TPtr producer,
        TYsonBuilder::TPtr builder);
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual IListNode
{
protected:
    IYPathService::TNavigateResult NavigateRecursive(TYPath path);

    IYPathService::TSetResult SetRecursive(
        TYPath path,
        TYsonProducer::TPtr producer,
        TYsonBuilder::TPtr builder);

    IYPathService::TNavigateResult GetYPathChild(
        int index,
        TYPath tailPath) const;

    IYPathService::TSetResult CreateYPathChild(
        int beforeIndex,
        TYPath tailPath,
        TYsonProducer::TPtr producer,
        TYsonBuilder::TPtr builder);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

