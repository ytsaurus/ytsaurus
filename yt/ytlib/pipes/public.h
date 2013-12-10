#pragma once

#include <yt/core/actions/future.h>

#include <contrib/libev/ev++.h>

namespace NYT {
namespace NPipes {

struct IFDWatcher
    : public virtual TRefCounted
{
    virtual void Start(ev::dynamic_loop& eventLoop) = 0;

    virtual ~IFDWatcher() {}
};

typedef TIntrusivePtr<IFDWatcher> IFDWatcherPtr;

}
}
