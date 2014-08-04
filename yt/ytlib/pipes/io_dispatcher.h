#pragma once

#include "public.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
{
public:
    static TIODispatcher* Get();

    DECLARE_SINGLETON_DEFAULT_MIXIN(TIODispatcher);

private:
    TIODispatcher();

    ~TIODispatcher();

    friend class TAsyncIOBase;

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
    TAsyncError AsyncUnregister(IFDWatcherPtr watcher);

    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
