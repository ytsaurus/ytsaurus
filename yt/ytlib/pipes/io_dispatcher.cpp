#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Impl(new TImpl())
{}

TIODispatcher::~TIODispatcher()
{}

TIODispatcher* TIODispatcher::Get()
{
    return Singleton<TIODispatcher>();
}

TAsyncError TIODispatcher::AsyncRegister(IFDWatcherPtr watcher)
{
    return Impl->AsyncRegister(watcher);
}

TAsyncError TIODispatcher::AsyncUnregister(IFDWatcherPtr watcher)
{
    return Impl->AsyncUnregister(watcher);
}

void TIODispatcher::Shutdown()
{
    return Impl->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
