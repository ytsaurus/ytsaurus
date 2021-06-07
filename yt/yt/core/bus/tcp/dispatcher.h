#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpDispatcherCounters
    : public TRefCounted
{
    std::atomic<i64> InBytes = {0};
    std::atomic<i64> InPackets = {0};

    std::atomic<i64> OutBytes = {0};
    std::atomic<i64> OutPackets = {0};

    std::atomic<i64> PendingOutPackets = {0};
    std::atomic<i64> PendingOutBytes = {0};

    std::atomic<int> ClientConnections = {0};
    std::atomic<int> ServerConnections = {0};

    std::atomic<i64> StalledReads = {0};
    std::atomic<i64> StalledWrites = {0};

    std::atomic<i64> ReadErrors = {0};
    std::atomic<i64> WriteErrors = {0};

    std::atomic<i64> EncoderErrors = {0};
    std::atomic<i64> DecoderErrors = {0};

    TTcpDispatcherStatistics ToStatistics() const;
};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherCounters)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher
{
public:
    ~TTcpDispatcher();

    static TTcpDispatcher* Get();

    static void StaticShutdown();

    void Shutdown();

    const TTcpDispatcherCountersPtr& GetCounters(const TString& networkName);

    //! Returns the poller used by TCP transport.
    //! May return null if shutdown is already initiated.
    NConcurrency::IPollerPtr GetXferPoller();

    void Configure(const TTcpDispatcherConfigPtr& config);

    //! Disables all networking. Safety measure for local runs and snapshot validation.
    void DisableNetworking();

private:
    TTcpDispatcher();

    Y_DECLARE_SINGLETON_FRIEND();
    friend class TTcpConnection;
    friend class TTcpBusClient;
    friend class TTcpBusServerBase;
    template <class TServer>
    friend class TTcpBusServerProxy;

    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
