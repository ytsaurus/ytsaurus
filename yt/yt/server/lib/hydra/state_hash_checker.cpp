#include "state_hash_checker.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TStateHashChecker::TStateHashChecker(
    int limit,
    int totalPeerCount,
    NLogging::TLogger logger)
    : Logger(std::move(logger))
    , Limit_(limit)
    , TotalPeerCount_(totalPeerCount)
{
    YT_TLOG_ALERT_IF(TotalPeerCount_ <= 0, "Total peer count must be a positive integer")
        .With("TotalPeerCount", TotalPeerCount_);
}

void TStateHashChecker::Report(i64 sequenceNumber, ui64 stateHash, int peerId)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    auto it = SequenceNumberToStateHash_.find(sequenceNumber);
    if (it == SequenceNumberToStateHash_.end()) {
        it = EmplaceOrCrash(
            SequenceNumberToStateHash_,
            sequenceNumber,
            TReportedStateHash{
                .PeerId = peerId,
                .StateHash = stateHash,
                .ReportedPeerIds = {peerId},
            });
    } else {
        auto& reported = it->second;
        reported.ReportedPeerIds.insert(peerId);
        if (reported.StateHash != stateHash) {
            reported.Diverged = true;
            if (!FirstDivergedSequenceNumber_ || sequenceNumber < *FirstDivergedSequenceNumber_) {
                FirstDivergedSequenceNumber_ = sequenceNumber;
                YT_TLOG_ALERT("State hashes differ")
                    .With("SequenceNumber", sequenceNumber)
                    .WithFormat("FirstStateHash", "%x", reported.StateHash)
                    .With("FirstPeerId", reported.PeerId)
                    .WithFormat("SecondStateHash", "%x", stateHash)
                    .With("SecondPeerId", peerId);
            } else {
                YT_TLOG_DEBUG("State hashes differ, but an earlier divergence is already known")
                    .With("SequenceNumber", sequenceNumber)
                    .With("FirstDivergedSequenceNumber", *FirstDivergedSequenceNumber_)
                    .WithFormat("FirstStateHash", "%x", reported.StateHash)
                    .With("FirstPeerId", reported.PeerId)
                    .WithFormat("SecondStateHash", "%x", stateHash)
                    .With("SecondPeerId", peerId);
            }
        }
    }

    // if all peers have reported the same state hash they are considered converged again
    if (FirstDivergedSequenceNumber_ && sequenceNumber > *FirstDivergedSequenceNumber_ &&
        std::ssize(it->second.ReportedPeerIds) >= TotalPeerCount_ &&
        !it->second.Diverged)
    {
        YT_TLOG_DEBUG("State hashes converged again, resetting first diverged sequence number")
            .With("FirstDivergedSequenceNumber", *FirstDivergedSequenceNumber_)
            .With("SequenceNumber", sequenceNumber);
        FirstDivergedSequenceNumber_.reset();
    }

    while (std::ssize(SequenceNumberToStateHash_) > Limit_) {
        const auto& [evictedSequenceNumber, evictedStateHash] = *SequenceNumberToStateHash_.begin();
        if (std::ssize(evictedStateHash.ReportedPeerIds) < TotalPeerCount_) {
            YT_TLOG_DEBUG("Evicting state hash before all peers have reported it")
                .With("SequenceNumber", evictedSequenceNumber)
                .With("ReportedPeerCount", std::ssize(evictedStateHash.ReportedPeerIds))
                .With("TotalPeerCount", TotalPeerCount_);
        }
        SequenceNumberToStateHash_.erase(SequenceNumberToStateHash_.begin());
    }
}

void TStateHashChecker::ReconfigureLimit(int limit)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    Limit_ = limit;
}

std::vector<std::pair<i64, ui64>> TStateHashChecker::GetStateHashes(const std::vector<i64>& sequenceNumbers)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(Lock_);

    std::vector<std::pair<i64, ui64>> result;
    for (auto sequenceNumber : sequenceNumbers) {
        auto it = SequenceNumberToStateHash_.find(sequenceNumber);
        if (it != SequenceNumberToStateHash_.end()) {
            result.emplace_back(sequenceNumber, it->second.StateHash);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
