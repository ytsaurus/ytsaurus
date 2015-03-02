#include "stdafx.h"
#include "tcp_dispatcher.h"
#include "tcp_dispatcher_impl.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

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
    lhs.PendingInPackets += rhs.PendingInPackets;
    lhs.PendingInBytes += rhs.PendingInBytes;
    lhs.PendingOutPackets += rhs.PendingOutPackets;
    lhs.PendingOutBytes += rhs.PendingOutBytes;
    lhs.ClientConnections += rhs.ClientConnections;
    lhs.ServerConnections += rhs.ServerConnections;
    return lhs;
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TTcpDispatcher()
    : Impl_(new TImpl())
{ }

TTcpDispatcher::~TTcpDispatcher()
{ }

TTcpDispatcher* TTcpDispatcher::Get()
{
    return Singleton<TTcpDispatcher>();
}

void TTcpDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

TTcpDispatcherStatistics TTcpDispatcher::GetStatistics(ETcpInterfaceType interfaceType)
{
    return Impl_->GetStatistics(interfaceType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
