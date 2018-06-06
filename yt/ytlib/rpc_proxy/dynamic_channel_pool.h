#pragma once

#include "private.h"

#include <yt/ytlib/api/connection.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TExpiringChannel
    : public TIntrinsicRefCounted
{
public:
    TFuture<NRpc::IChannelPtr> GetChannel();
    bool IsExpired();

private:
    // Protected by TDynamicChannelPool::Lock_.
    TString Address_;
    bool IsActive_ = false;

    TFuture<NRpc::IChannelPtr> Channel_;
    std::atomic<bool> IsExpired_ = {false};

    friend class TDynamicChannelPool;
};

DECLARE_REFCOUNTED_CLASS(TExpiringChannel)

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPool
    : public TRefCounted
{
public:
    explicit TDynamicChannelPool(
        NRpc::IChannelFactoryPtr channelFactory);

    TExpiringChannelPtr CreateChannel();
    void EvictChannel(TExpiringChannelPtr channelCookie);
    TErrorOr<NRpc::IChannelPtr> TryCreateChannel();

    void SetAddressList(TFuture<std::vector<TString>> addresses);

    void Terminate();

protected:
    const NRpc::IChannelFactoryPtr ChannelFactory_;

    TSpinLock SpinLock_;
    bool Terminated_ = false;
    TFuture<std::vector<TString>> Addresses_;
    THashMap<TString, THashSet<TExpiringChannelPtr>> ActiveChannels_;

    void OnAddressListChange(const TErrorOr<std::vector<TString>>& addressesOrError);
};

DECLARE_REFCOUNTED_CLASS(TDynamicChannelPool)

////////////////////////////////////////////////////////////////////////////////

//! Create roaming channel over dynamic pool of addresses.
NRpc::IChannelPtr CreateDynamicChannel(
    TDynamicChannelPoolPtr pool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
