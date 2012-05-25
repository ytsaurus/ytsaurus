#pragma once

#include "public.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpDispatcherStatistics
{
    TTcpDispatcherStatistics();
    
    int PendingInCount;
    i64 PendingInSize;
    
    int PendingOutCount;
    i64 PendingOutSize;

    int ClientConnectionCount;
    int ServerConnectionCount;
};

class TTcpDispatcher
{
public:
    // TODO(babenko): move to private, required for Singleton
    TTcpDispatcher();

    static TTcpDispatcher* Get();
    void Shutdown();

    TTcpDispatcherStatistics GetStatistics();

private:
    friend class TTcpConnection;
    friend class TTcpClientBusProxy;
    friend class TTcpBusServer;
    friend class TTcpBusServerProxy;

    class TImpl;
    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
