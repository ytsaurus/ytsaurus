#pragma once

#include "io_dispatcher.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

#include <core/concurrency/action_queue_detail.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher::TImpl
    : public NConcurrency::TEVSchedulerThread
{
public:
    TImpl();

    TAsyncError AsyncRegister(IFDWatcherPtr watcher);
    TAsyncError AsyncUnregister(IFDWatcherPtr watcher);

private:
    void DoRegister(IFDWatcherPtr watcher);
    void DoUnregister(IFDWatcherPtr watcher);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
