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

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
    void Shutdown();
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

}
}
