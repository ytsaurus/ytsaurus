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
        return NULL; \
    } \
    \
    virtual TIntrusiveConstPtr<I##name##Node> As##name() const \
    { \
        YUNREACHABLE(); \
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

    virtual TLockResult Lock(TYPath path);

protected:
    TRemoveResult RemoveSelf();
    virtual void DoRemoveSelf();

    TSetResult SetSelf(TYsonProducer::TPtr producer);
    virtual void DoSetSelf(TYsonProducer::TPtr producer);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

