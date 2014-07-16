#include "stdafx.h"
#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"

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
    Shutdown();
}

TIODispatcher* TIODispatcher::Get()
{
    return Singleton<TIODispatcher>();
}

TAsyncError TIODispatcher::AsyncRegister(IFDWatcherPtr watcher)
{
    return Impl_->AsyncRegister(watcher);
}

TAsyncError TIODispatcher::AsyncUnregister(IFDWatcherPtr watcher)
{
    return Impl_->AsyncUnregister(watcher);
}

void TIODispatcher::Shutdown()
{
    if (Impl_) {
        Impl_->Shutdown();
        Impl_.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
