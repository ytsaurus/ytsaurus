#include "public.h"

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

const std::string DefaultNetworkName("default");
const TNetworkPreferenceList DefaultNetworkPreferences{DefaultNetworkName};

////////////////////////////////////////////////////////////////////////////////

void ValidateFeasibleRealNodeId(TNodeId nodeId)
{
    YT_VERIFY(nodeId <= MaxRealNodeId);
}

bool IsAddressOffshore(std::string_view address)
{
    return address.starts_with(OffshoreNodeAddress);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient

