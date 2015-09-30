#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpDispatcherStatistics
{
    int PendingInPackets = 0;
    i64 PendingInBytes = 0;

    int PendingOutPackets = 0;
    i64 PendingOutBytes = 0;

    int ClientConnections = 0;
    int ServerConnections = 0;
};

TTcpDispatcherStatistics operator + (
    const TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs);

TTcpDispatcherStatistics& operator += (
    TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

struct TTcpProfilingData
{
    TTcpProfilingData();

    NProfiling::TTagId TagId;

    NProfiling::TAggregateCounter ReceiveTimeCounter;
    NProfiling::TAggregateCounter ReceiveSizeCounter;
    NProfiling::TAggregateCounter InHandlerTimeCounter;
    NProfiling::TSimpleCounter InByteCounter;
    NProfiling::TSimpleCounter InPacketCounter;

    NProfiling::TAggregateCounter SendTimeCounter;
    NProfiling::TAggregateCounter SendSizeCounter;
    NProfiling::TAggregateCounter OutHandlerTimeCounter;
    NProfiling::TSimpleCounter OutBytesCounter;
    NProfiling::TSimpleCounter OutPacketCounter;
    NProfiling::TAggregateCounter PendingOutPacketCounter;
    NProfiling::TAggregateCounter PendingOutByteCounter;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETcpInterfaceType,
    (Local)  // UNIX domain sockets
    (Remote) // regular TCP sockets
);

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher
    : public IShutdownable
{
public:
    ~TTcpDispatcher();

    static TTcpDispatcher* Get();

    static void StaticShutdown();

    virtual void Shutdown() override;

    TTcpDispatcherStatistics GetStatistics(ETcpInterfaceType interfaceType);
    TTcpProfilingData* GetProfilingData(ETcpInterfaceType interfaceType);

private:
    TTcpDispatcher();

    DECLARE_SINGLETON_FRIEND(TTcpDispatcher);
    friend class TTcpConnection;
    friend class TTcpBusClient;
    friend class TTcpBusServerBase;
    template <class TServer>
    friend class TTcpBusServerProxy;

    class TImpl;
    const std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
