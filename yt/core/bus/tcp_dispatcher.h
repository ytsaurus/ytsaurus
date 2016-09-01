#pragma once

#include "public.h"

#include <yt/core/misc/shutdownable.h>

#include <yt/core/profiling/profiler.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpDispatcherStatistics
{
    i64 InBytes = 0;
    i64 InPackets = 0;

    i64 OutBytes = 0;
    i64 OutPackets = 0;

    i64 PendingOutPackets = 0;
    i64 PendingOutBytes = 0;

    int ClientConnections = 0;
    int ServerConnections = 0;

    i64 StalledReads = 0;
    i64 StalledWrites = 0;

    i64 ReadErrors = 0;
    i64 WriteErrors = 0;

    i64 EncoderErrors = 0;
    i64 DecoderErrors = 0;
};

TTcpDispatcherStatistics operator + (
    const TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs);

TTcpDispatcherStatistics& operator += (
    TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETcpInterfaceType,
    (Local)       // UNIX domain socket or local TCP socket
    (Remote)      // remote TCP socket
);

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher
    : public IShutdownable
{
public:
    ~TTcpDispatcher();

    static TTcpDispatcher* Get();

    static void StaticShutdown();

    static void SetClientThreadCount(int clientThreadCount);

    virtual void Shutdown() override;

    TTcpDispatcherStatistics GetStatistics(ETcpInterfaceType interfaceType);

private:
    TTcpDispatcher();

    Y_DECLARE_SINGLETON_FRIEND();
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
