#include "private.h"
#include "distributed_hydra_manager.h"
#include "hydra_service_proxy.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/actions/future.h>

#include <algorithm>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;
using namespace NRpc;

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

std::vector<i64> SampleMutationsSequenceNumbers(
    i64 firstSequenceNumber,
    i64 lastSequenceNumber,
    i64 rate)
{
    std::vector<i64> result;

    auto startSequenceNumber = (firstSequenceNumber + rate - 1) / rate * rate;
    auto endSequenceNumber = lastSequenceNumber / rate * rate;
    if (startSequenceNumber > endSequenceNumber) {
        return result;
    }

    result.reserve((endSequenceNumber - startSequenceNumber) / rate);
    for (auto sequenceNumber = startSequenceNumber; sequenceNumber <= endSequenceNumber; sequenceNumber += rate) {
        result.push_back(sequenceNumber);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ReportMutationStateHashesToLeader(
    const TCellManagerPtr& cellManager,
    int leaderId,
    const std::vector<std::pair<i64, ui64>>& sequenceNumbersToStateHashes,
    TDuration timeout,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    if (sequenceNumbersToStateHashes.empty()) {
        return MakeFuture(TError());
    }

    auto sortedSequenceNumbersToStateHashes = sequenceNumbersToStateHashes;
    std::sort(sortedSequenceNumbersToStateHashes.begin(), sortedSequenceNumbersToStateHashes.end());

    auto channel = cellManager->GetPeerChannel(leaderId);
    YT_VERIFY(channel);

    TInternalHydraServiceProxy proxy(std::move(channel));
    auto request = proxy.ReportMutationsStateHashes();
    request->set_peer_id(cellManager->GetSelfPeerId());

    for (auto [sequenceNumber, stateHash] : sortedSequenceNumbersToStateHashes) {
        auto* mutationInfo = request->add_mutations_info();
        mutationInfo->set_sequence_number(sequenceNumber);
        mutationInfo->set_state_hash(stateHash);
    }

    auto startSequenceNumber = sortedSequenceNumbersToStateHashes.front().first;
    auto endSequenceNumber = sortedSequenceNumbersToStateHashes.back().first;

    request->SetTimeout(timeout);
    return request->Invoke()
        .Apply(BIND([=] (const TInternalHydraServiceProxy::TErrorOrRspReportMutationsStateHashesPtr& rspOrError) {
        if (rspOrError.IsOK()) {
            YT_TLOG_DEBUG("Mutations state hashes reported")
                .With("StartSequenceNumber", startSequenceNumber)
                .With("EndSequenceNumber", endSequenceNumber);
        } else {
            YT_TLOG_DEBUG("Error reporting mutations state hashes")
                .With("StartSequenceNumber", startSequenceNumber)
                .With("EndSequenceNumber", endSequenceNumber)
                .With(rspOrError);
        }
    }));
}

////////////////////////////////////////////////////////////////////////////////

TError WrapHydraError(TError&& error)
{
    if (error.GetCode() == EErrorCode::ExpectedMutationHandlerException) {
        return std::move(error);
    }
    return std::move(error).Wrap(EErrorCode::ExpectedMutationHandlerException, "Error executing mutation");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
