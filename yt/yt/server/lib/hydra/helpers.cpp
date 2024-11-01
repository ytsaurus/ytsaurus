#include "private.h"
#include "distributed_hydra_manager.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

namespace NYT::NHydra {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

bool IsPersistenceEnabled(
    const TCellManagerPtr& cellManager,
    const TDistributedHydraManagerOptions& options)
{
    auto selfId = cellManager->GetSelfPeerId();
    auto voting = cellManager->GetPeerConfig(selfId)->Voting;
    return voting || options.EnableObserverPersistence;
}

std::optional<TSharedRef> SanitizeLocalHostName(
    const THashSet<TString>& clusterPeersAddresses,
    TStringBuf host)
{
    if (!clusterPeersAddresses.contains(host)) {
        return TSharedRef::FromString(ToString(host));
    }

    TString unifiedHost(host);
    for (int i = 0; i < ssize(host); ++i) {
        for (const auto& peerAddress : clusterPeersAddresses) {
            if (host.size() != peerAddress.size()) {
                return std::nullopt;
            }
            if (host[i] != peerAddress[i]) {
                unifiedHost[i] = '*';
                break;
            }
        }
    }
    return TSharedRef::FromString(unifiedHost);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
