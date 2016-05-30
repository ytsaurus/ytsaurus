#include "address_helpers.h"

#include <yt/core/misc/address.h>

namespace NYT {

using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

TAddressMap GetLocalAddresses(const TAddressMap& addresses, int port)
{
    // Аppend port number.
    TAddressMap result;
    for (const auto& pair : addresses) {
        YCHECK(result.emplace(
            pair.first,
            BuildServiceAddress(pair.second, port)).second);
    }

    // Add default address.
    const auto def = result.emplace(DefaultNetworkName, Stroka());
    if (def.second)
        def.first->second = BuildServiceAddress(TAddressResolver::Get()->GetLocalHostName(), port);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
