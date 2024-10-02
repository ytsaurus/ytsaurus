#ifndef PEER_DISCOVERY_INL_H_
#error "Direct inclusion of this file is not allowed, include peer_discovery.h"
// For the sake of sane code completion.
#include "peer_discovery.h"
#endif

#include "discovery_client.h"

#include <yt/yt/core/rpc/client.h>

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

template <class TDiscoveryServiceProxy>
class TOrmPeerDiscovery
    : public IOrmPeerDiscovery
{
public:
    TOrmPeerDiscovery(
        NLogging::TLogger logger,
        std::optional<TString> balancerAddress,
        THashMap<TString, TString> addressesToIP6Addresses)
        : Logger(logger)
        , BalancerAddress_(std::move(balancerAddress))
        , AddressesToIP6Addresses_(std::move(addressesToIP6Addresses))
    { }

    TFuture<NRpc::TPeerDiscoveryResponse> Discover(
        NRpc::IChannelPtr channel,
        const std::string& address,
        TDuration timeout,
        TDuration /*replyDelay*/,
        const std::string& /*serviceName*/) override
    {
        auto masters = GetMasters(channel, timeout);
        return masters.ApplyUnique(BIND([this, this_ = MakeStrong(this), address] (TGetMastersResult&& masters) {
            NRpc::TPeerDiscoveryResponse response;

            bool responseContainsRegularPeer = false;
            response.Addresses.reserve(masters.MasterInfos.size());
            for (auto& masterInfo : masters.MasterInfos) {
                responseContainsRegularPeer |= masterInfo.GrpcAddress != BalancerAddress_;
                response.Addresses.push_back(std::move(masterInfo.GrpcAddress));
            }

            response.IsUp = BalancerAddress_ != address || !responseContainsRegularPeer;
            return response;
        }));
    }

    TFuture<TGetMastersResult> GetMasters(NRpc::IChannelPtr channel, TDuration timeout) override
    {
        return NDetail::TDiscoveryServiceTraits<TDiscoveryServiceProxy>::GetMasters(channel, Logger, timeout)
            .ApplyUnique(BIND([this, this_ = MakeStrong(this)] (TGetMastersResult&& masters)
        {
            UpdateCache(masters.MasterInfos);
            return masters;
        }));
    }

    std::optional<TString> GetAddress(TMasterInstanceTag instanceTag) const override
    {
        auto guard = NThreading::ReaderGuard(SpinLock_);
        if (auto* address = MapFindPtr(InstanceTagsToAddresses_, instanceTag)) {
            return *address;
        }

        return std::nullopt;
    }

    virtual std::optional<TString> ResolveIP6Address(const TString& address) const override
    {
        auto guard = NThreading::ReaderGuard(SpinLock_);
        if (auto* ip6Address = MapFindPtr(AddressesToIP6Addresses_, address)) {
            return *ip6Address;
        }

        return std::nullopt;
    }

private:
    NLogging::TLogger Logger;
    std::optional<TString> BalancerAddress_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TMasterInstanceTag, TString> InstanceTagsToAddresses_;
    THashMap<TString, TString> AddressesToIP6Addresses_;

    void UpdateCache(const std::vector<TMasterInfo>& masterInfos)
    {
        auto guard = NThreading::WriterGuard(SpinLock_);
        for (const auto& masterInfo : masterInfos) {
            InstanceTagsToAddresses_[masterInfo.InstanceTag] = masterInfo.GrpcAddress;
            AddressesToIP6Addresses_[masterInfo.GrpcAddress] = masterInfo.GrpcIP6Address;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TDiscoveryServiceProxy>
IOrmPeerDiscoveryPtr CreateOrmPeerDiscovery(
    NLogging::TLogger logger,
    std::optional<TString> balancerAddress,
    const std::optional<TString>& balancerIP6Address)
{
    THashMap<TString, TString> addressesToIP6Addresses;
    if (balancerIP6Address) {
        YT_VERIFY(balancerAddress);
        addressesToIP6Addresses.emplace(*balancerAddress, *balancerIP6Address);
    }

    return New<TOrmPeerDiscovery<TDiscoveryServiceProxy>>(
        std::move(logger),
        std::move(balancerAddress),
        std::move(addressesToIP6Addresses));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
