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

    //! Typically not needed since initialization happens on-demand.
    //! Calling this method explicitly, however, ensures that dispatcher threads
    //! are up and running. This might be useful since root permissions are
    //! required for adjusting their priorities.
    void Initialize();

    void Shutdown();

    TTcpDispatcherStatistics GetStatistics();

private:
    friend class TTcpConnection;
    friend class TTcpClientBusProxy;
    friend class TBusServerBase;
    template <class TServer>
    friend class TTcpBusServerProxy;

    class TImpl;
    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
