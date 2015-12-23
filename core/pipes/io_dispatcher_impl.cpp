#include "io_dispatcher_impl.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NPipes {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TImpl::TImpl()
    : TEVSchedulerThread("Pipes", false)
{
    Start();
}

const ev::loop_ref& TIODispatcher::TImpl::GetEventLoop() const
{
    return EventLoop;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
