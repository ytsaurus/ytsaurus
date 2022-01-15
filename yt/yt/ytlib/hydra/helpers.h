#pragma once

#include <yt/yt/client/hydra/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

// TODO(aleksandra-zh): get rid of unnecessary arguments.
void SwitchLeader(
    const std::vector<NRpc::IChannelPtr>& peerChannels,
    const NRpc::IChannelPtr& currentLeaderChannel,
    const NRpc::IChannelPtr& newLeaderChannel,
    const std::vector<TString>& addresses,
    const TString& newLeaderAddress,
    const std::optional<TDuration>& timeout,
    const std::optional<TString>& user);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
