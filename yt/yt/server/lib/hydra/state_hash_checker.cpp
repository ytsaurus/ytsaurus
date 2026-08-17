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
    YT_LOG_ALERT_IF(TotalPeerCount_ <= 0, "State hash checker constructor fail; total peer count should be a positive integer, have: %x",
        TotalPeerCount_);
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
                YT_LOG_ALERT("State hashes differ "
                    "(SequenceNumber: %v, FirstStateHash: %x, FirstPeerId: %v, SecondStateHash: %x, SecondPeerId: %v)",
                    sequenceNumber,
                    reported.StateHash,
                    reported.PeerId,
                    stateHash,
                    peerId);
            } else {
                YT_LOG_DEBUG("State hashes differ, but an earlier divergence is already known "
                    "(SequenceNumber: %v, FirstDivergedSequenceNumber: %v, FirstStateHash: %x, FirstPeerId: %v, "
                    "SecondStateHash: %x, SecondPeerId: %v)",
                    sequenceNumber,
                    *FirstDivergedSequenceNumber_,
                    reported.StateHash,
                    reported.PeerId,
                    stateHash,
                    peerId);
            }
        }
    }

    // if all peers have reported the same state hash they are considered converged again
    if (FirstDivergedSequenceNumber_ && sequenceNumber > *FirstDivergedSequenceNumber_ &&
        std::ssize(it->second.ReportedPeerIds) >= TotalPeerCount_ &&
        !it->second.Diverged)
    {
        YT_LOG_DEBUG("State hashes converged again, resetting first diverged sequence number "
            "(FirstDivergedSequenceNumber: %v, SequenceNumber: %v)",
            *FirstDivergedSequenceNumber_,
            sequenceNumber);
        FirstDivergedSequenceNumber_.reset();
    }

    while (std::ssize(SequenceNumberToStateHash_) > Limit_) {
        const auto& [evictedSequenceNumber, evictedStateHash] = *SequenceNumberToStateHash_.begin();
        if (std::ssize(evictedStateHash.ReportedPeerIds) < TotalPeerCount_) {
            YT_LOG_DEBUG("Evicting state hash before all peers have reported it "
                "(SequenceNumber: %v, ReportedPeerCount: %v, TotalPeerCount: %v)",
                evictedSequenceNumber,
                std::ssize(evictedStateHash.ReportedPeerIds),
                TotalPeerCount_);
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
