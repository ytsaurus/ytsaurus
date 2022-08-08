#include "connection_pool.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/net/connection.h>

namespace NYT::NHttp {

using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

TReusableConnectionState::TReusableConnectionState(
    NNet::IConnectionPtr connection, 
    TConnectionPoolPtr owningPool)
    : Reusable(true)
    , Connection(std::move(connection))
    , OwningPool(std::move(owningPool))
{ }

TReusableConnectionState::~TReusableConnectionState()
{
    if (Reusable && OwningPool && Connection->IsIdle()) {
        OwningPool->Release(std::move(Connection));
    }
}

DEFINE_REFCOUNTED_TYPE(TReusableConnectionState)

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TConnectionPool::TConnectionPool(
    const IDialerPtr& dialer, 
    const TClientConfigPtr& config,
    const IInvokerPtr& invoker)
    : Dialer_(dialer)
    , Config_(config)
    , Connections_(config->MaxIdleConnections)
    , ExpiredConnectionsCollector_(New<TPeriodicExecutor>(
        invoker, 
        BIND([this] { DropExpiredConnections(); }),
        TPeriodicExecutorOptions::WithJitter(config->ConnectionIdleTimeout)))
{
    if (Config_->MaxIdleConnections > 0) {
        ExpiredConnectionsCollector_->Start();
    }
}

IConnectionPtr TConnectionPool::Connect(const TNetworkAddress& address)
{
    {
        auto guard = Guard(SpinLock_);
        
        while (auto item = Connections_.Extract(address)) {
            if (item->IdleTime() < Config_->ConnectionIdleTimeout) {
                return std::move(item->Connection);
            }       
        }
    }

    return WaitFor(Dialer_->Dial(address)).ValueOrThrow();
}

void TConnectionPool::Release(IConnectionPtr connection)
{
    auto guard = Guard(SpinLock_);
    Connections_.Insert(connection->RemoteAddress(), {connection, TInstant::Now()});
}

TDuration TConnectionPool::TIdleConnection::IdleTime() const
{
    return TInstant::Now() - InsertionTime;
}

void TConnectionPool::DropExpiredConnections()
{
    auto guard = Guard(SpinLock_);

    TMultiLruCache<TNetworkAddress, TIdleConnection> validConnections(
        Config_->MaxIdleConnections);

    while (Connections_.GetSize() > 0) {
        auto idleConn = Connections_.Pop();
        if (idleConn.IdleTime() < Config_->ConnectionIdleTimeout) {
            validConnections.Insert(idleConn.Connection->RemoteAddress(), idleConn);
        }
    }

    Connections_ = std::move(validConnections);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
