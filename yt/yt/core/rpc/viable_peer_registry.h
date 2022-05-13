#pragma once

#include "public.h"
#include "hedging_channel.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

//! A registry maintaining a set of viable peers.
//! Maintains no more than a configured number of channels, keeping the remaining addresses in a backlog.
//! When peers with active channels are unregistered, their places are filled with addresses from the backlog.
/*
 *  Thread affinity: single-threaded.
 */
struct IViablePeerRegistry
    : public TRefCounted
{
    virtual IChannelPtr PickRandomChannel(
        const IClientRequestPtr& request,
        const std::optional<THedgingChannelOptions>& hedgingOptions) const = 0;
    virtual IChannelPtr PickStickyChannel(const IClientRequestPtr& request) const = 0;
    virtual std::optional<IChannelPtr> GetChannel(const TString& address) const = 0;

    //! Returns true if a new peer was actually registered and false if it already existed.
    virtual bool RegisterPeer(const TString& address) = 0;
    //! Returns true if this peer was actually unregistered.
    virtual bool UnregisterPeer(const TString& address) = 0;
    //! Tries to evict a random active peer and replace it with a peer from the backlog.
    //! The evicted peer is stored in the backlog instead.
    //! Returns the address of the rotated peer.
    virtual std::optional<TString> MaybeRotateRandomPeer() = 0;

    virtual std::vector<IChannelPtr> GetActiveChannels() const = 0;
    virtual void Clear() = 0;
};

DEFINE_REFCOUNTED_TYPE(IViablePeerRegistry)

using TCreateChannelCallback = TCallback<IChannelPtr(const TString& address)>;

IViablePeerRegistryPtr CreateViablePeerRegistry(
    TViablePeerRegistryConfigPtr config,
    TCreateChannelCallback createChannel,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
