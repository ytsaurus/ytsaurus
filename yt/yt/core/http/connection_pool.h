#pragma once

#include "config.h"
#include "stream.h"

#include <yt/yt/core/misc/sync_cache.h>
#include <yt/yt/core/net/connection.h>
#include <yt/yt/core/net/dialer.h>

#include <library/cpp/yt/memory/ref.h>
#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! Responsible for returning the connection to the owning pool 
//! if it could be reused
struct TReusableConnectionState
    : public virtual TRefCounted
{
    std::atomic<bool> Reusable;
    NNet::IConnectionPtr Connection;
    TConnectionPoolPtr OwningPool;

    TReusableConnectionState(NNet::IConnectionPtr connection, TConnectionPoolPtr owningPool);
    ~TReusableConnectionState() override;
};

DECLARE_REFCOUNTED_STRUCT(TReusableConnectionState)

//! Reports to the shared state whether the connection could be reused
//! (by calling T::IsSafeToReuse() in the destructor)
template <class T>
class TConnectionReuseWrapper
    : public T
{
public:
    using T::T;

    ~TConnectionReuseWrapper() override;
    
    void SetReusableState(TReusableConnectionStatePtr reusableState);

private:
    TReusableConnectionStatePtr ReusableState_;
};

} // namespace NYT::NHttp::NDetail

////////////////////////////////////////////////////////////////////////////////

class TConnectionPool
    : public virtual TRefCounted
{
public:
    TConnectionPool(
        const NNet::IDialerPtr& dialer, 
        const TClientConfigPtr& config,
        const IInvokerPtr& invoker);

    NNet::IConnectionPtr Connect(const NNet::TNetworkAddress& address);

    void Release(NNet::IConnectionPtr connection);

private:
    struct TIdleConnection
    {
        NNet::IConnectionPtr Connection;
        TInstant InsertionTime;

        TDuration IdleTime() const;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    
    const NNet::IDialerPtr Dialer_;
    const TClientConfigPtr Config_;

    TMultiLruCache<NNet::TNetworkAddress, TIdleConnection> Connections_;
    NConcurrency::TPeriodicExecutorPtr ExpiredConnectionsCollector_;

    void DropExpiredConnections();
};

DEFINE_REFCOUNTED_TYPE(TConnectionPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp

#define CONNECTION_POOL_INL_H
#include "connection_pool-inl.h"
#undef CONNECTION_POOL_INL_H
