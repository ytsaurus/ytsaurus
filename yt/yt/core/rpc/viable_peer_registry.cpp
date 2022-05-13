#include "viable_peer_registry.h"

#include "client.h"
#include "config.h"
#include "indexed_hash_map.h"

#include <yt/yt/core/misc/compact_set.h>
#include <yt/yt/core/misc/random.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TViablePeerRegistry
    : public IViablePeerRegistry
{
public:
    TViablePeerRegistry(
        TViablePeerRegistryConfigPtr config,
        TCreateChannelCallback createChannel,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , CreateChannel_(std::move(createChannel))
        , Logger(logger)
    { }

    bool RegisterPeer(const TString& address) override
    {
        switch (Config_->PeerPriorityStrategy) {
            case EPeerPriorityStrategy::None:
                return RegisterPeerWithPriority(address, /*priority*/ 0);
        }
    }

    bool UnregisterPeer(const TString& address) override
    {
        // Check if the peer is in the backlog.
        if (auto backlogPeerIt = BacklogPeers_.find(address); backlogPeerIt != BacklogPeers_.end()) {
            BacklogPeers_.Erase(address);
            YT_LOG_DEBUG("Unregistered backlog peer (Address: %v)", address);
            return true;
        }

        auto activePeerIt = ActivePeerToPriority_.find(address);

        if (activePeerIt == ActivePeerToPriority_.end()) {
            return false;
        }

        GeneratePeerHashes(address, [&] (size_t hash) {
            HashToActiveChannel_.erase(std::make_pair(hash, address));
        });

        PriorityToActivePeers_[activePeerIt->second].Erase(address);

        ActivePeerToPriority_.Erase(address);

        YT_LOG_DEBUG("Unregistered active peer (Address: %v)", address);

        ActivateBacklogPeers();

        return true;
    }

    std::vector<IChannelPtr> GetActiveChannels() const override
    {
        std::vector<IChannelPtr> allActivePeers;
        allActivePeers.reserve(ActivePeerToPriority_.Size());
        for (const auto& [priority, activePeers] : PriorityToActivePeers_) {
            for (const auto& [address, channel] : activePeers) {
                allActivePeers.push_back(channel);
            }
        }
        return allActivePeers;
    }

    void Clear() override
    {
        BacklogPeers_.Clear();
        HashToActiveChannel_.clear();
        ActivePeerToPriority_.Clear();
        PriorityToActivePeers_.clear();
    }

    std::optional<TString> MaybeRotateRandomPeer() override
    {
        if (BacklogPeers_.Size() > 0 && ActivePeerToPriority_.Size() > 0) {
            auto addressToEvict = ActivePeerToPriority_[RandomNumber<size_t>(ActivePeerToPriority_.Size())];
            YT_LOG_DEBUG("Moving random viable peer to backlog (Address: %v)", addressToEvict.first);
            // This call will automatically activate a random peer from the backlog.
            UnregisterPeer(addressToEvict.first);
            // The rotated peer will end up in the backlog after this call.
            RegisterPeerWithPriority(addressToEvict.first, addressToEvict.second);
            return addressToEvict.first;
        }
        return {};
    }

    IChannelPtr PickStickyChannel(const IClientRequestPtr& request) const override
    {
        const auto& balancingExt = request->Header().GetExtension(NProto::TBalancingExt::balancing_ext);
        auto hash = request->GetHash();
        auto randomNumber = balancingExt.enable_client_stickiness() ? ClientStickinessRandomNumber_ : RandomNumber<size_t>();
        int stickyGroupSize = balancingExt.sticky_group_size();
        auto randomIndex = randomNumber % stickyGroupSize;

        if (ActivePeerToPriority_.Size() == 0) {
            return nullptr;
        }

        auto it = HashToActiveChannel_.lower_bound(std::make_pair(hash, TString()));
        auto rebaseIt = [&] {
            if (it == HashToActiveChannel_.end()) {
                it = HashToActiveChannel_.begin();
            }
        };

        TCompactSet<TStringBuf, 16> seenAddresses;
        auto currentRandomIndex = randomIndex % ActivePeerToPriority_.Size();
        while (true) {
            rebaseIt();
            const auto& address = it->first.second;
            if (seenAddresses.count(address) == 0) {
                if (currentRandomIndex == 0) {
                    break;
                }
                seenAddresses.insert(address);
                --currentRandomIndex;
            } else {
                ++it;
            }
        }

        YT_LOG_DEBUG(
            "Sticky peer selected (RequestId: %v, RequestHash: %llx, RandomIndex: %v/%v, Address: %v)",
            request->GetRequestId(),
            hash,
            randomIndex,
            stickyGroupSize,
            it->first.second);

        return it->second;
    }

    IChannelPtr PickRandomChannel(
        const IClientRequestPtr& request,
        const std::optional<THedgingChannelOptions>& hedgingOptions) const override
    {
        if (ActivePeerToPriority_.Size() == 0) {
            return nullptr;
        }

        const auto& viablePeers = PriorityToActivePeers_.begin()->second;

        int peerIndex = RandomNumber<size_t>(viablePeers.Size());

        IChannelPtr channel;
        if (hedgingOptions && viablePeers.Size() > 1) {
            const auto& primaryPeer = viablePeers[peerIndex];
            const auto& backupPeer = peerIndex + 1 == viablePeers.Size()
                ? viablePeers[0]
                : viablePeers[peerIndex + 1];
            channel = CreateHedgingChannel(
                primaryPeer.second,
                backupPeer.second,
                *hedgingOptions);

            YT_LOG_DEBUG(
                "Random peers selected (RequestId: %v, PrimaryAddress: %v, BackupAddress: %v)",
                request ? request->GetRequestId() : TRequestId(),
                primaryPeer.first,
                backupPeer.first);
        } else {
            const auto& peer = viablePeers[peerIndex];
            channel = peer.second;

            YT_LOG_DEBUG(
                "Random peer selected (RequestId: %v, Address: %v)",
                request ? request->GetRequestId() : TRequestId(),
                peer.first);
        }

        return channel;
    }

    std::optional<IChannelPtr> GetChannel(const TString& address) const override
    {
        if (auto it = ActivePeerToPriority_.find(address); it != ActivePeerToPriority_.end()) {
            return GetOrCrash(PriorityToActivePeers_, it->second).Get(address);
        }
        return {};
    }

private:
    const TViablePeerRegistryConfigPtr Config_;
    const TCallback<IChannelPtr(TString address)> CreateChannel_;
    const NLogging::TLogger Logger;

    // Information for active peers with created channels.
    std::map<int, TIndexedHashMap<TString, IChannelPtr>> PriorityToActivePeers_;
    TIndexedHashMap<TString, int> ActivePeerToPriority_;
    // A consistent-hashing storage for serving sticky requests.
    std::map<std::pair<size_t, TString>, IChannelPtr> HashToActiveChannel_;

    // Information for non-active peers which go over the max peer count limit.
    TIndexedHashMap<TString, int> BacklogPeers_;

    const size_t ClientStickinessRandomNumber_ = RandomNumber<size_t>();

    //! Returns true if a new peer was successfully registered and false if it already existed.
    //! Trying to call this method for a currently viable address with a different priority than stored leads to failure.
    bool RegisterPeerWithPriority(const TString& address, int priority)
    {
        // Check for an existing active peer for this address.
        if (auto it = ActivePeerToPriority_.find(address); it != ActivePeerToPriority_.end()) {
            // Peers should have a fixed priority.
            YT_VERIFY(it->second == priority);
            return false;
        } else {
            // Peer is new, we need to check that we won't be adding more than MaxPeerCount active peers.
            if (ActivePeerToPriority_.Size() >= Config_->MaxPeerCount) {
                // Add peer to backlog.

                // Check for an existing backlog entry for this peer.
                if (auto backlogPeerIt = BacklogPeers_.find(address); backlogPeerIt != BacklogPeers_.end()) {
                    // Peers should have a fixed priority.
                    YT_VERIFY(backlogPeerIt->second == priority);
                    return false;
                } else {
                    YT_LOG_DEBUG(
                        "Viable peer added to backlog (Address: %v, Priority: %v)",
                        address,
                        priority);
                    BacklogPeers_.Set(address, priority);
                    return true;
                }
            }
        }

        ActivePeerToPriority_.Set(address, priority);

        auto channel = CreateChannel_(address);

        // Save the created channel for the given address for sticky requests.
        GeneratePeerHashes(address, [&] (size_t hash) {
            HashToActiveChannel_[std::make_pair(hash, address)] = channel;
        });

        // Save the channel for the given address at its priority.
        PriorityToActivePeers_[priority].Set(address, channel);

        YT_LOG_DEBUG(
            "Activated viable peer (Address: %v, Priority: %v)",
            address,
            priority);

        return true;
    }

    template <class F>
    void GeneratePeerHashes(const TString& address, F f)
    {
        TRandomGenerator generator(ComputeHash(address));
        for (int index = 0; index < Config_->HashesPerPeer; ++index) {
            f(generator.Generate<size_t>());
        }
    }

    void ActivateBacklogPeers()
    {
        while (BacklogPeers_.Size() > 0 && ActivePeerToPriority_.Size() < Config_->MaxPeerCount) {
            const auto& randomBacklogPeer = BacklogPeers_[RandomNumber<size_t>(BacklogPeers_.Size())];
            RegisterPeerWithPriority(randomBacklogPeer.first, randomBacklogPeer.second);
            YT_LOG_DEBUG("Activated peer from backlog (Address: %v)", randomBacklogPeer.first);
            BacklogPeers_.Erase(randomBacklogPeer.first);
        }
    }
};

IViablePeerRegistryPtr CreateViablePeerRegistry(
    TViablePeerRegistryConfigPtr config,
    TCreateChannelCallback createChannel,
    const NLogging::TLogger& logger)
{
    return New<TViablePeerRegistry>(std::move(config), std::move(createChannel), logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
