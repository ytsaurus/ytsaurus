#include "pipe_io_dispatcher.h"

#include "config.h"

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/poller.h>

namespace NYT::NPipeIO {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TPipeIODispatcher::TPipeIODispatcher()
    : Poller_(BIND([] { return CreateThreadPoolPoller(1, "Pipes"); }))
{ }

TPipeIODispatcher::~TPipeIODispatcher() = default;

TPipeIODispatcher* TPipeIODispatcher::Get()
{
    return Singleton<TPipeIODispatcher>();
}

void TPipeIODispatcher::Configure(const TPipeIODispatcherConfigPtr& config)
{
    Poller_->SetPollingPeriod(config->ThreadPoolPollingPeriod);
}

IInvokerPtr TPipeIODispatcher::GetInvoker()
{
    return Poller_.Value()->GetInvoker();
}

IPollerPtr TPipeIODispatcher::GetPoller()
{
    return Poller_.Value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipeIO
