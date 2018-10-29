#pragma once

#include "public.h"
#include "private.h"

#include <yt/client/api/connection.h>

#include <yt/core/concurrency/rw_spinlock.h>

namespace NYT {
namespace NApi {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPool
    : public TRefCounted
{
public:
    explicit TDynamicChannelPool(
        NRpc::IChannelFactoryPtr channelFactory,
        TConnectionConfigPtr config);

    TFuture<NRpc::IChannelPtr> GetRandomChannel();

    void SetAddressList(const TErrorOr<std::vector<TString>>& addressesOrError);

    void Terminate();

protected:
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TConnectionConfigPtr Config_;

    struct TChannelSlot
        : public TRefCounted
    {
        TPromise<NRpc::IChannelPtr> Channel = NewPromise<NRpc::IChannelPtr>();
        TInstant CreationTime = TInstant::Now();
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

DECLARE_REFCOUNTED_CLASS(TDynamicChannelPool)

////////////////////////////////////////////////////////////////////////////////

//! Create roaming channel over dynamic pool of addresses.
NRpc::IChannelPtr CreateDynamicChannel(
    TDynamicChannelPoolPtr pool);

NRpc::IChannelPtr CreateStickyChannel(
    TDynamicChannelPoolPtr pool);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NApi
} // namespace NYT
