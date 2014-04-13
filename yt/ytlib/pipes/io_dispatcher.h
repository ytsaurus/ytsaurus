#pragma once

#include "public.h"

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
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
