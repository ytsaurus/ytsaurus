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
    typedef TIntrusiveConstPtr<TNodeBase> TConstPtr;

    virtual INode::TPtr AsMutable() const
    {
        return dynamic_cast<INode*>(~AsMutableImpl());
    }

    virtual INode::TConstPtr AsImmutable() const
    {
        return dynamic_cast<INode*>(const_cast<TNodeBase*>(~AsImmutableImpl()));
    }

#define IMPLEMENT_AS_METHODS(name) \
    virtual TIntrusiveConstPtr<I ## name ## Node> As ## name() const \
    { \
        YASSERT(false); \
        return NULL; \
    } \
    \
    virtual TIntrusivePtr<I ## name ## Node> As ## name() \
    { \
        YASSERT(false); \
        return NULL; \
    }

    IMPLEMENT_AS_METHODS(Entity)
    IMPLEMENT_AS_METHODS(Composite)

    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Int64)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)

#undef IMPLEMENT_AS_METHODS

    virtual TNavigateResult Navigate(
        TYPath path);

    virtual TGetResult Get(
        TYPath path,
        IYsonConsumer* events);

    virtual TSetResult Set(
        TYPath path,
        TYsonProducer::TPtr producer);

    virtual TRemoveResult Remove(TYPath path);

protected:
    virtual TNodeBase::TPtr AsMutableImpl() const;
    virtual TNodeBase::TConstPtr AsImmutableImpl() const;

    virtual TRemoveResult RemoveSelf();
    virtual TSetResult SetSelf(TYsonProducer::TPtr producer);

private:

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

