#include "stdafx.h"
#include "io_dispatcher_impl.h"

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
