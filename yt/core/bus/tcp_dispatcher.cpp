#include "stdafx.h"
#include "tcp_dispatcher.h"
#include "tcp_dispatcher_impl.h"

#include <core/misc/singleton.h>

#include <core/profiling/profile_manager.h>

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

TTcpProfilingData::TTcpProfilingData()
    : TagId(-1)
    , ReceiveTimeCounter("/receive_time")
    , ReceiveSizeCounter("/receive_size")
    , InHandlerTimeCounter("/in_handler_time")
    , InByteCounter("/in_bytes")
    , InPacketCounter("/in_packets")
    , SendTimeCounter("/send_time")
    , SendSizeCounter("/send_size")
    , OutHandlerTimeCounter("/out_handler_time")
    , OutBytesCounter("/out_bytes")
    , OutPacketCounter("/out_packets")
    , PendingOutPacketCounter("/pending_out_packets")
    , PendingOutByteCounter("/pending_out_bytes")
{ }

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TTcpDispatcher()
    : Impl_(new TImpl())
{ }

TTcpDispatcher::~TTcpDispatcher()
{ }

TTcpDispatcher* TTcpDispatcher::Get()
{
    return TSingletonWithFlag<TTcpDispatcher>::Get();
}

void TTcpDispatcher::StaticShutdown()
{
    if (TSingletonWithFlag<TTcpDispatcher>::WasCreated()) {
        TSingletonWithFlag<TTcpDispatcher>::Get()->Shutdown();
    }
}

void TTcpDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

TTcpDispatcherStatistics TTcpDispatcher::GetStatistics(ETcpInterfaceType interfaceType)
{
    return Impl_->GetStatistics(interfaceType);
}

TTcpProfilingData* TTcpDispatcher::GetProfilingData(ETcpInterfaceType interfaceType)
{
    return Impl_->GetProfilingData(interfaceType);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
