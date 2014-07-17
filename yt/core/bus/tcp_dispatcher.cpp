#include "stdafx.h"
#include "tcp_dispatcher.h"
#include "tcp_dispatcher_impl.h"
#include "tcp_connection.h"

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

TTcpDispatcherStatistics operator + (
    const TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TTcpDispatcherStatistics& operator += (
    TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs)
{
    lhs.PendingInCount += rhs.PendingInCount;
    lhs.PendingInSize += rhs.PendingInSize;
    lhs.PendingOutCount += rhs.PendingOutCount;
    lhs.PendingOutSize += rhs.PendingOutSize;
    lhs.ClientConnectionCount += rhs.ClientConnectionCount;
    lhs.ServerConnectionCount += rhs.ServerConnectionCount;
    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TTcpDispatcher()
    : Impl(new TImpl())
{
    auto afterFork = [] () {
        TTcpDispatcher::Get()->AfterFork();
    };

    YCHECK(pthread_atfork(nullptr, afterFork, afterFork) == 0);
}

TTcpDispatcher* TTcpDispatcher::Get()
{
    return Singleton<TTcpDispatcher>();
}

void TTcpDispatcher::AfterFork()
{
    if (!Impl) {
        Impl.reset(new TImpl());
    }
}

void TTcpDispatcher::Shutdown()
{
    if (Impl) {
        Impl->Shutdown();
        Impl.reset();
    }
}

TTcpDispatcherStatistics TTcpDispatcher::GetStatistics(ETcpInterfaceType interfaceType) const
{
    return Impl->GetStatistics(interfaceType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
