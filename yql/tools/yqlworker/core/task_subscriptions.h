#pragma once

#include <yql/essentials/providers/common/metrics/sensors_group.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/generic/ptr.h>
#include <util/network/address.h>
#include <util/system/rwlock.h>


namespace NYql {

using TChannelId = ui32;    // unique websocket channel id
using TNetAddrId = TString;  // address + channelId
using TSubscrId = TString;   // taskId or username
using TSubscrMask = ui64;

//////////////////////////////////////////////////////////////////////////////
// TTaskSubscription
//////////////////////////////////////////////////////////////////////////////
class TTaskSubscription: public TAtomicRefCount<TTaskSubscription> {
public:
    TTaskSubscription(
            const NAddr::IRemoteAddr& addr,
            TChannelId channelId,
            TSubscrMask mask)
        : Address_(&addr)
        , ChannelId_(channelId)
        , Mask_(mask)
    {
    }

    const NAddr::IRemoteAddr& GetAddress() const {
        return Address_;
    }

    TChannelId GetChannelId() const {
        return ChannelId_;
    }

    TSubscrMask GetMask() const {
        return Mask_;
    }

    void SetMask(TSubscrMask mask) {
        Mask_ = mask;
    }

private:
    const NAddr::TOpaqueAddr Address_;
    const TChannelId ChannelId_;
    TSubscrMask Mask_;
};

using TTaskSubscriptionPtr = TIntrusivePtr<TTaskSubscription>;

//////////////////////////////////////////////////////////////////////////////
// TTaskSubscriptions
//////////////////////////////////////////////////////////////////////////////
class TTaskSubscriptions: public TAtomicRefCount<TTaskSubscriptions> {
    struct TSubscriptionItem: public TIntrusiveListItem<TSubscriptionItem> {
        TTaskSubscriptionPtr S;
    };

    using TSubscriptionsList = TIntrusiveList<TSubscriptionItem>;
    using TSubscriptionsMap = THashMap<TSubscrId, TSubscriptionItem>;

public:
    TTaskSubscriptions()
        : TTaskSubscriptions(new TSensorCounter)
    {
    }

    explicit TTaskSubscriptions(TSensorCounterPtr subscrsCounter)
        : SubscrsCounter_(std::move(subscrsCounter))
        , Size_(0)
    {
        *SubscrsCounter_ = Size_;
    }

    void Subscribe(
            const NAddr::IRemoteAddr& addr, TChannelId channelId,
            const TSubscrId& id, TSubscrMask mask);

    bool SubscribeIf(
            const NAddr::IRemoteAddr& addr, TChannelId channelId,
            const TSubscrId& id, TSubscrMask mask,
            std::function<bool()> condition);

    void Unsubscribe(const NAddr::IRemoteAddr& addr, TChannelId channelId);
    void Unsubscribe(const NAddr::IRemoteAddr& addr, TChannelId channelId, const TSubscrId& id);
    TVector<TTaskSubscriptionPtr> Unsubscribe(const TSubscrId& id);

    TVector<TTaskSubscriptionPtr> Find(
            const TSubscrId& id, TSubscrMask mask) const;

    size_t Size() const;

private:
    void SubscribeImpl(
            const TNetAddrId& netAddrId,
            const NAddr::IRemoteAddr& addr, TChannelId channelId,
            const TSubscrId& id, TSubscrMask mask);

private:
    mutable TRWMutex Mutex_;
    THashMap<TSubscrId, TSubscriptionsList> Subscriptions_;
    THashMap<TNetAddrId, TSubscriptionsMap> SubscriptionsByAddress_;
    TSensorCounterPtr SubscrsCounter_;
    size_t Size_;
};

using TTaskSubscriptionsPtr = TIntrusivePtr<TTaskSubscriptions>;

} // namespace NYql
