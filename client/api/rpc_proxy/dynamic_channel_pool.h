#pragma once

#include "public.h"
#include "private.h"

#include <yt/client/api/connection.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPool
    : public TRefCounted
{
public:
    TDynamicChannelPool(
        NRpc::IChannelFactoryPtr channelFactory,
        TConnectionConfigPtr config,
        NLogging::TLogger logger);

    TFuture<NRpc::IChannelPtr> GetRandomChannel();

    void SetAddressList(const TErrorOr<std::vector<TString>>& addressesOrError);

    void Terminate();

protected:
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TConnectionConfigPtr Config_;
    const NLogging::TLogger Logger;

    struct TChannelSlot
        : public TRefCounted
    {
        TPromise<NRpc::IChannelPtr> Channel = NewPromise<NRpc::IChannelPtr>();
        TInstant CreationTime = TInstant::Now();
        TString Address;
        std::atomic<bool> SeemsBroken{false};

        bool IsWarm(TInstant now);
    };

    typedef TIntrusivePtr<TChannelSlot> TChannelSlotPtr;

    NConcurrency::TReaderWriterSpinLock SpinLock_;
    std::vector<TChannelSlotPtr> Slots_;
    bool Terminated_ = false;
    TInstant LastRebalance_ = TInstant::Now();

    TSpinLock OpenChannelsLock_;
    THashMap<TString, NRpc::IChannelPtr> OpenChannels_;
    NRpc::IChannelPtr CreateChannel(const TString& address);
    void TerminateIdleChannels();
};

DEFINE_REFCOUNTED_TYPE(TDynamicChannelPool)

////////////////////////////////////////////////////////////////////////////////

//! Create roaming channel over dynamic pool of addresses.
NRpc::IChannelPtr CreateDynamicChannel(
    TDynamicChannelPoolPtr pool);

NRpc::IChannelPtr CreateStickyChannel(
    TDynamicChannelPoolPtr pool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
