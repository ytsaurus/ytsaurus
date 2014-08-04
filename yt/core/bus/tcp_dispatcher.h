#pragma once

#include "public.h"

#include <core/misc/public.h>

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

    TTcpDispatcherStatistics GetStatistics(ETcpInterfaceType interfaceType);

    DECLARE_SINGLETON_DEFAULT_MIXIN(TTcpDispatcher);

private:
    TTcpDispatcher();

    ~TTcpDispatcher();

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
