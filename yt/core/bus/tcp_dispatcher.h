#pragma once

#include "public.h"

#include <core/misc/shutdownable.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpDispatcherStatistics
{
    int PendingInCount = 0;
    i64 PendingInSize = 0;

    int PendingOutCount = 0;
    i64 PendingOutSize = 0;

    int ClientConnectionCount = 0;
    int ServerConnectionCount = 0;
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
    std::unique_ptr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
