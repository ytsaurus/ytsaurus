#include "stdafx.h"
#include "io_dispatcher_impl.h"
#include "async_io.h"

namespace NYT {
namespace NPipes {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TImpl::TImpl()
    : TEVSchedulerThread("Pipes", false)
{ }

TAsyncError TIODispatcher::TImpl::AsyncRegister(IFDWatcherPtr watcher)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND(&TImpl::DoRegister, MakeStrong(this), watcher)
        .Guarded()
        .AsyncVia(GetInvoker())
        .Run();
}

TAsyncError TIODispatcher::TImpl::AsyncUnregister(IFDWatcherPtr watcher)
{
    return BIND(&TImpl::DoUnregister, MakeStrong(this), watcher)
        .Guarded()
        .AsyncVia(GetInvoker())
        .Run();
}

void TIODispatcher::TImpl::DoRegister(IFDWatcherPtr watcher)
{
    watcher->Start(EventLoop);
}

void TIODispatcher::TImpl::DoUnregister(IFDWatcherPtr watcher)
{
    watcher->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
