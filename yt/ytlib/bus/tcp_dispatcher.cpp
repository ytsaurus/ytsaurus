#include "stdafx.h"
#include "tcp_dispatcher.h"
#include "tcp_dispatcher_impl.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcherStatistics::TTcpDispatcherStatistics()
    : PendingInCount(0)
    , PendingInSize(0)
    , PendingOutCount(0)
    , PendingOutSize(0)
    , ClientConnectionCount(0)
    , ServerConnectionCount(0)
{ }

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TTcpDispatcher()
    : Impl(new TImpl())
{ }

TTcpDispatcher* TTcpDispatcher::Get()
{
    return Singleton<TTcpDispatcher>();
}

void TTcpDispatcher::Shutdown()
{
    Impl->Shutdown();
}

TTcpDispatcherStatistics TTcpDispatcher::GetStatistics()
{
    return Impl->Statistics();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
