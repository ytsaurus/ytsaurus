#pragma once

#include <yt/core/actions/future.h>

namespace ev {
    class dynamic_loop;
}


namespace NYT {
namespace NPipes {

struct IFDWatcher
    : public virtual TRefCounted
{
    virtual void Start(ev::dynamic_loop& eventLoop) = 0;
};

typedef TIntrusivePtr<IFDWatcher> IFDWatcherPtr;

class TAsyncReader;
typedef TIntrusivePtr<TAsyncReader> TAsyncReaderPtr;

class TAsyncWriter;
typedef TIntrusivePtr<TAsyncWriter> TAsyncWriterPtr;

}
}
