#pragma once

#include "public.h"

#include <yt/core/misc/error.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
{
public:
    TIODispatcher();
    ~TIODispatcher();

    static TIODispatcher* Get();

    void Shutdown();

private:
    friend class TAsyncIOBase;

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
    TAsyncError AsyncUnregister(IFDWatcherPtr watcher);

    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
