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

TTcpDispatcherStatistics operator + (
    const TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs);

TTcpDispatcherStatistics& operator += (
    TTcpDispatcherStatistics& lhs,
    const TTcpDispatcherStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher
{
public:
    static TTcpDispatcher* Get();

    void AfterFork();

    void Shutdown();

    TTcpDispatcherStatistics GetStatistics(ETcpInterfaceType interfaceType) const;

private:
    TTcpDispatcher();

    DECLARE_SINGLETON_FRIEND(TTcpDispatcher);
    friend class TTcpConnection;
    friend class TTcpClientBusProxy;
    friend class TTcpBusServerBase;
    template <class TServer>
    friend class TTcpBusServerProxy;

    class TImpl;
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
