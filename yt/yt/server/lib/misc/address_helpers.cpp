#include "address_helpers.h"

#include <yt/yt/core/net/local_address.h>
#include <yt/yt/core/net/address.h>

namespace NYT::NServer {

using namespace NNodeTrackerClient;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

TAddressMap GetLocalAddresses(const TNetworkAddressList& addresses, int port)
{
    // Append port number.
    TAddressMap result;
    result.reserve(addresses.size());
    for (const auto& [networkName, networkAddress] : addresses) {
        YT_VERIFY(result.emplace(networkName, BuildServiceAddress(networkAddress, port)).second);
    }

    // Add default address.
    auto [it, inserted] = result.emplace(DefaultNetworkName, TString());
    if (inserted) {
        it->second = BuildServiceAddress(GetLocalHostName(), port);
    }

    return result;
}

std::optional<std::string> FindDefaultAddress(const NNodeTrackerClient::TNetworkAddressList& addresses, int port)
{
    for (const auto& [networkName, networkAddress] : addresses) {
        if (networkName == DefaultNetworkName) {
            return BuildServiceAddress(networkAddress, port);
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
