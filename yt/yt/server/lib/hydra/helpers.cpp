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
    const THashSet<std::string>& clusterPeersAddresses,
    const std::string& host)
{
    if (!clusterPeersAddresses.contains(host)) {
        return {};
    }

    if (std::ssize(clusterPeersAddresses) == 1) {
        return TSharedRef::FromString(TString(host));
    }

    auto getChar = [] (TStringBuf str, i64 position, bool reverse) -> std::optional<char> {
        if (position < 0 || position >= std::ssize(str)) {
            return std::nullopt;
        }
        return reverse ? str[std::ssize(str) - position - 1] : str[position];
    };

    auto allEqual = [&] (i64 position, bool reverse) {
        for (const auto& peerAddress : clusterPeersAddresses) {
            if (getChar(peerAddress, position, reverse) != getChar(host, position, reverse)) {
                return false;
            }
        }
        return true;
    };

    auto minPeerSize = std::ssize(host);
    for (const auto& peerAddress : clusterPeersAddresses) {
        minPeerSize = std::min(minPeerSize, std::ssize(peerAddress));
    }

    i64 commonPrefixSize = 0;
    while (commonPrefixSize < minPeerSize && allEqual(commonPrefixSize, /*reverse*/ false)) {
        ++commonPrefixSize;
    }

    // We do not want the prefix to overlap with the suffix, so instead of using the original
    // peers we essentially find the common suffix of the set of peers with the common prefix
    // cut from the beginning of each peer.
    i64 commonSuffixSize = 0;
    while (commonSuffixSize < minPeerSize - commonPrefixSize && allEqual(commonSuffixSize, /*reverse*/ true)) {
        ++commonSuffixSize;
    }

    auto unifiedHost = Format(
        "%v*%v",
        host.substr(0, commonPrefixSize),
        host.substr(std::ssize(host) - commonSuffixSize));
    return TSharedRef::FromString(unifiedHost);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
