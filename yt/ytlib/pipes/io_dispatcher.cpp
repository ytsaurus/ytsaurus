#include "stdafx.h"
#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"

#include <core/misc/singleton.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Impl_(New<TImpl>())
{
    Impl_->Start();
}

TIODispatcher::~TIODispatcher()
{
    Impl_->Shutdown();
}

TIODispatcher* TIODispatcher::Get()
{
    return TSingleton::Get();
}

TAsyncError TIODispatcher::AsyncRegister(IFDWatcherPtr watcher)
{
    return Impl_->AsyncRegister(watcher);
}

TAsyncError TIODispatcher::AsyncUnregister(IFDWatcherPtr watcher)
{
    return Impl_->AsyncUnregister(watcher);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
