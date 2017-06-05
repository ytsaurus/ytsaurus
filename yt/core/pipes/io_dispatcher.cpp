#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Impl_(New<TIODispatcher::TImpl>())
{ }

TIODispatcher::~TIODispatcher()
{ }

TIODispatcher* TIODispatcher::Get()
{
    return Singleton<TIODispatcher>();
}

IInvokerPtr TIODispatcher::GetInvoker()
{
    if (Y_UNLIKELY(!Impl_->IsStarted())) {
        Impl_->Start();
    }
    return Impl_->GetInvoker();
}

const ev::loop_ref& TIODispatcher::GetEventLoop()
{
    if (Y_UNLIKELY(!Impl_->IsStarted())) {
        Impl_->Start();
    }
    return Impl_->GetEventLoop();
}

void TIODispatcher::StaticShutdown()
{
    Get()->Shutdown();
}

void TIODispatcher::Shutdown()
{
    return Impl_->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(6, TIODispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
