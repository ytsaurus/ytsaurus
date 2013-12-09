#include "file_io_dispatcher.h"
#include "file_io_dispatcher_impl.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NFileIO {

TFileIODispatcher::TFileIODispatcher()
    : Impl(new TImpl())
{}

TFileIODispatcher* TFileIODispatcher::Get()
{
    return Singleton<TFileIODispatcher>();
}

TAsyncError TFileIODispatcher::AsyncRegister(IFDWatcherPtr watcher)
{
    return Impl->AsyncRegister(watcher);
}

void TFileIODispatcher::Shutdown()
{
    return Impl->Shutdown();
}

}
}
