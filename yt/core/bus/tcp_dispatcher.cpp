#include "tcp_dispatcher.h"
#include "tcp_dispatcher_impl.h"

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/profiling/profile_manager.h>

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
    lhs.InBytes += rhs.InBytes;
    lhs.InPackets += rhs.InPackets;
    lhs.OutBytes += rhs.OutBytes;
    lhs.OutPackets += rhs.OutPackets;
    lhs.PendingOutBytes += rhs.PendingOutBytes;
    lhs.PendingOutPackets += rhs.PendingOutPackets;
    lhs.ClientConnections += rhs.ClientConnections;
    lhs.ServerConnections += rhs.ServerConnections;
    lhs.StalledReads += rhs.StalledReads;
    lhs.StalledWrites += rhs.StalledWrites;
    lhs.ReadErrors += rhs.ReadErrors;
    lhs.WriteErrors += rhs.WriteErrors;
    lhs.EncoderErrors += rhs.EncoderErrors;
    lhs.DecoderErrors += rhs.DecoderErrors;
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

void TTcpDispatcher::StaticShutdown()
{
    Get()->Shutdown();
}

void TTcpDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

TTcpDispatcherStatistics TTcpDispatcher::GetStatistics(ETcpInterfaceType interfaceType)
{
    return Impl_->GetStatistics(interfaceType);
}

void TTcpDispatcher::SetClientThreadCount(int clientThreadCount)
{
    Get()->Impl_->SetClientThreadCount(clientThreadCount);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(6, TTcpDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
