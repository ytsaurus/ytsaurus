#include "task_subscriptions.h"
#ifdef _win_
#define in_port_t u_short
#endif

namespace NYql {
namespace {

/**
 * puts address, port and channel id into buffer with next structure:
 *
 * +-------------------------+-----------+-----------+
 * | address                 | port      | channelId |
 * +-------------------------+-----------+-----------+
 */
template <typename TAddr>
void AppendToBuf(TAddr& addr, in_port_t port, TChannelId chId, TString* buf) {
    buf->ReserveAndResize(sizeof(addr) + sizeof(port) + sizeof(chId));
    char* p = buf->begin();

    memcpy(p, &addr, sizeof(addr)); p += sizeof(addr);
    memcpy(p, &port, sizeof(port)); p += sizeof(port);
    memcpy(p, &chId, sizeof(chId));
}

TNetAddrId ToNetAddrId(const NAddr::IRemoteAddr& addr, TChannelId channelId) {
    TNetAddrId buf;

    const sockaddr* saddr = addr.Addr();
    if (saddr->sa_family == AF_INET) {
        const sockaddr_in* in = (sockaddr_in*) saddr;
        AppendToBuf(in->sin_addr, in->sin_port, channelId, &buf);
    } else if (saddr->sa_family == AF_INET6) {
        const sockaddr_in6* in6 = (sockaddr_in6*) saddr;
        AppendToBuf(in6->sin6_addr, in6->sin6_port, channelId, &buf);
    } else {
        ythrow yexception() << "unsupported address family: "
                            << saddr->sa_family;
    }

    return buf;
}

} // namespace

//////////////////////////////////////////////////////////////////////////////
// TTaskSubscriptions
//////////////////////////////////////////////////////////////////////////////
void TTaskSubscriptions::Subscribe(
        const NAddr::IRemoteAddr& addr, TChannelId channelId,
        const TSubscrId& id, TSubscrMask mask)
{
    TNetAddrId netAddrId = ToNetAddrId(addr, channelId);
    TWriteGuard guard(Mutex_);
    SubscribeImpl(netAddrId, addr, channelId, id, mask);
}

bool TTaskSubscriptions::SubscribeIf(
        const NAddr::IRemoteAddr& addr, TChannelId channelId,
        const TSubscrId& id, TSubscrMask mask,
        std::function<bool()> condition)
{
    TNetAddrId netAddrId = ToNetAddrId(addr, channelId);
    TWriteGuard guard(Mutex_);
    if (condition()) {
        SubscribeImpl(netAddrId, addr, channelId, id, mask);
        return true;
    }
    return false;
}

void TTaskSubscriptions::Unsubscribe(
        const NAddr::IRemoteAddr& addr, TChannelId channelId)
{
    TNetAddrId netAddrId = ToNetAddrId(addr, channelId);
    TWriteGuard guard(Mutex_);

    auto subscrsMapIt = SubscriptionsByAddress_.find(netAddrId);
    if (subscrsMapIt == SubscriptionsByAddress_.end()) {
        return;
    }

    TSubscriptionsMap& subscrsMap = subscrsMapIt->second;
    for (auto& it: subscrsMap) {
        const TSubscrId& subscrId = it.first;
        TSubscriptionItem& subscrItem = it.second;

        subscrItem.Unlink();
        SubscrsCounter_->Dec();
        Size_--;

        auto subscrsListIt = Subscriptions_.find(subscrId);
        if (Y_LIKELY(subscrsListIt != Subscriptions_.end())) {
            TSubscriptionsList& subscrsList = subscrsListIt->second;
            if (subscrsList.Empty()) {
                Subscriptions_.erase(subscrsListIt);
            }
        } else {
            Y_DEBUG_ABORT("invariant violation");
        }
    }

    SubscriptionsByAddress_.erase(subscrsMapIt);
}

void TTaskSubscriptions::Unsubscribe(
        const NAddr::IRemoteAddr& addr, TChannelId channelId,
        const TSubscrId& id)
{
    TNetAddrId netAddrId = ToNetAddrId(addr, channelId);
    TWriteGuard guard(Mutex_);

    auto subscrsMapIt = SubscriptionsByAddress_.find(netAddrId);
    if (subscrsMapIt == SubscriptionsByAddress_.end()) {
        return;
    }

    TSubscriptionsMap& subscrsMap = subscrsMapIt->second;
    auto subscrIt = subscrsMap.find(id);
    if (subscrIt != subscrsMap.end()) {
        subscrIt->second.Unlink();
        SubscrsCounter_->Dec();
        Size_--;

        auto subscrsListIt = Subscriptions_.find(id);
        if (Y_LIKELY(subscrsListIt != Subscriptions_.end())) {
            TSubscriptionsList& subscrsList = subscrsListIt->second;
            if (subscrsList.Empty()) {
                Subscriptions_.erase(subscrsListIt);
            }
        } else {
            Y_DEBUG_ABORT("invariant violation");
        }

        subscrsMap.erase(subscrIt);
    }
}

TVector<TTaskSubscriptionPtr> TTaskSubscriptions::Unsubscribe(
        const TSubscrId& id)
{
    TWriteGuard guard(Mutex_);

    auto subscrsListIt = Subscriptions_.find(id);
    if (subscrsListIt == Subscriptions_.end()) {
        return {};
    }

    TVector<TTaskSubscriptionPtr> result;
    TSubscriptionsList& subscrsList = subscrsListIt->second;
    for (auto it = subscrsList.begin(), end = subscrsList.end(); it != end; ) {
        TSubscriptionItem& subscrItem = *it;
        result.push_back(subscrItem.S);
        SubscrsCounter_->Dec();
        Size_--;
        ++it; // move iterator forward before erasing item from subscrsMap

        auto netAddrId = ToNetAddrId(
                    subscrItem.S->GetAddress(),
                    subscrItem.S->GetChannelId());

        auto subscrsMapIt = SubscriptionsByAddress_.find(netAddrId);
        if (Y_LIKELY(subscrsMapIt != SubscriptionsByAddress_.end())) {
            TSubscriptionsMap& subscsrsMap = subscrsMapIt->second;
            subscsrsMap.erase(id);
            if (subscsrsMap.empty()) {
                SubscriptionsByAddress_.erase(subscrsMapIt);
            }
        } else {
            Y_DEBUG_ABORT("invariant violation");
        }
    }

    Subscriptions_.erase(subscrsListIt);
    return result;
}

TVector<TTaskSubscriptionPtr> TTaskSubscriptions::Find(
        const TSubscrId& id, TSubscrMask mask) const
{
    TReadGuard guard(Mutex_);

    auto subscrsListIt = Subscriptions_.find(id);
    if (subscrsListIt == Subscriptions_.end()) {
        return {};
    }

    TVector<TTaskSubscriptionPtr> result;
    const TSubscriptionsList& subscrsList = subscrsListIt->second;
    for (const TSubscriptionItem& item: subscrsList) {
        if (item.S->GetMask() & mask) {
            result.push_back(item.S);
        }
    }
    return result;
}

size_t TTaskSubscriptions::Size() const {
    TReadGuard guard(Mutex_);
    return Size_;
}

// NOTE: must be protected with the write mutex locked
void TTaskSubscriptions::SubscribeImpl(
        const TNetAddrId& netAddrId,
        const NAddr::IRemoteAddr& addr, TChannelId channelId,
        const TSubscrId& id, TSubscrMask mask)
{
    TSubscriptionsMap& subscrsMap = SubscriptionsByAddress_[netAddrId];
    TSubscriptionItem& subscrItem = subscrsMap[id];
    if (subscrItem.S) {
        subscrItem.S->SetMask(mask);
    } else {
        subscrItem.S.Reset(new TTaskSubscription(addr, channelId, mask));
        TSubscriptionsList& subscrsList = Subscriptions_[id];
        subscrsList.PushBack(&subscrItem);
        SubscrsCounter_->Inc();
        Size_++;
    }
}

} // namespace NYql
