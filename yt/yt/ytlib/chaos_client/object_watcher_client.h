#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/rpc/public.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

class TObjectWatcherClientBase
{
public:
    explicit TObjectWatcherClientBase(
        TWeakPtr<NApi::NNative::IConnection> connection,
        NRpc::IChannelPtr chaosCacheChannel = nullptr);

    virtual ~TObjectWatcherClientBase() = default;

protected:
    void WatchObject(TChaosObjectId objectId);
    bool StopWatchingObject(TChaosObjectId objectId);

    bool RemoveWatch(TChaosObjectId objectId);
    bool RearmWatch(TChaosObjectId objectId);
    bool RearmWatch(
        TChaosObjectId objectId,
        NTransactionClient::TTimestamp timestamp);

    NApi::NNative::IConnectionPtr GetConnection() const;
    NRpc::IChannelPtr GetWatchChannel(
        const NApi::NNative::IConnectionPtr& connection,
        TChaosObjectId objectId) const;

private:
    struct TWatchState
    {
        TFuture<void> Future;
        NTransactionClient::TTimestamp Timestamp = NTransactionClient::MinTimestamp;
    };

    const TWeakPtr<NApi::NNative::IConnection> Connection_;
    const NRpc::IChannelPtr ChaosCacheChannel_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TChaosObjectId, TWatchState> WatchStates_;

    virtual TFuture<void> WatchUpstream(
        TChaosObjectId objectId,
        NTransactionClient::TTimestamp timestamp) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
