#pragma once

#include "public.h"

#include <yt/core/misc/error.h>

#include <memory>

namespace NYT {
namespace NPipes {

class TIODispatcher
{
public:
    TIODispatcher();
    ~TIODispatcher();

    static TIODispatcher* Get();

    void Shutdown();
private:
    friend class TAsyncReader;
    friend class TAsyncWriter;

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
    TError Unregister(IFDWatcher& watcher);

    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

}
}
