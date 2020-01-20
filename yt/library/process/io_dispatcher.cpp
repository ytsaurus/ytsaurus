#include "io_dispatcher.h"

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/poller.h>

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

namespace NYT::NPipes {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Poller_(BIND([] { return CreateThreadPoolPoller(1, "Pipes"); }))
{ }

TIODispatcher::~TIODispatcher()
{ }

TIODispatcher* TIODispatcher::Get()
{
    return Singleton<TIODispatcher>();
}

IInvokerPtr TIODispatcher::GetInvoker()
{
    return Poller_.Value()->GetInvoker();
}

IPollerPtr TIODispatcher::GetPoller()
{
    return Poller_.Value();
}

void TIODispatcher::StaticShutdown()
{
    Get()->Shutdown();
}

void TIODispatcher::Shutdown()
{
    if (Poller_.HasValue()) {
        Poller_->Shutdown();
    }
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(6, TIODispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
