#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

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

//! Local means UNIX domain sockets.
//! Remove means standard TCP sockets.
DEFINE_ENUM(ETcpInterfaceType,
    (Local)
    (Remote)
);

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher
    : public IShutdownable
{
public:
    ~TTcpDispatcher();

    static TTcpDispatcher* Get();

    virtual void Shutdown() override;

    TTcpDispatcherStatistics GetStatistics(ETcpInterfaceType interfaceType);

private:
    TTcpDispatcher();

    DECLARE_SINGLETON_FRIEND(TTcpDispatcher);
    friend class TTcpConnection;
    friend class TTcpClientBusProxy;
    friend class TTcpBusServerBase;
    template <class TServer>
    friend class TTcpBusServerProxy;

    class TImpl;
    const std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
