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
    static TTcpDispatcher* Get();

    void Shutdown();

    TTcpDispatcherStatistics GetStatistics();

private:
    TTcpDispatcher();
    DECLARE_SINGLETON_FRIEND(TTcpDispatcher);

    friend class TTcpConnection;
    friend class TTcpClientBusProxy;
    friend class TBusServerBase;
    template <class TServer>
    friend class TTcpBusServerProxy;

    class TImpl;
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
