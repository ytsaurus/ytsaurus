#include "io_dispatcher_impl.h"

namespace NYT {
namespace NPipes {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TImpl::TImpl()
    : TEVSchedulerThread("PipeDispatcher", false)
{ }

const ev::loop_ref& TIODispatcher::TImpl::GetEventLoop() const
{
    return EventLoop_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
