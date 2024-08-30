#pragma once

#include <yt/yt/client/hydra/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

// TODO(aleksandra-zh): get rid of unnecessary arguments.
void SwitchLeader(
    const std::vector<NRpc::IChannelPtr>& peerChannels,
    const NRpc::IChannelPtr& currentLeaderChannel,
    const NRpc::IChannelPtr& newLeaderChannel,
    const std::vector<std::string>& addresses,
    const std::string& newLeaderAddress,
    const std::optional<TDuration>& timeout,
    const std::optional<std::string>& user);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
