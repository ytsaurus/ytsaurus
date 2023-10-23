#include "state_hash_checker.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TStateHashChecker::TStateHashChecker(
    int limit,
    NLogging::TLogger logger)
    : Logger(std::move(logger))
    , Limit_(limit)
{ }

void TStateHashChecker::Report(i64 sequenceNumber, ui64 stateHash, int peerId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    auto it = SequenceNumberToStateHash_.find(sequenceNumber);
    if (it == SequenceNumberToStateHash_.end()) {
        EmplaceOrCrash(SequenceNumberToStateHash_, sequenceNumber, TReportedStateHash{peerId, stateHash});

        if (std::ssize(SequenceNumberToStateHash_) > Limit_) {
            SequenceNumberToStateHash_.erase(SequenceNumberToStateHash_.begin());
        }
    } else if (it->second.StateHash != stateHash) {
        YT_LOG_ALERT("State hashes differ "
            "(SequenceNumber: %v, FirstStateHash: %x, FirstPeerId: %v, SecondStateHash: %x, SecondPeerId: %v)",
            sequenceNumber,
            it->second.StateHash,
            it->second.PeerId,
            stateHash,
            peerId);
    }
}

void TStateHashChecker::ReconfigureLimit(int limit)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    Limit_ = limit;
}

THashMap<i64, ui64> TStateHashChecker::GetStateHashes(const std::vector<i64>& sequenceNumbers)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = ReaderGuard(Lock_);

    THashMap<i64, ui64> result;
    for (auto sequenceNumber : sequenceNumbers) {
        auto it = SequenceNumberToStateHash_.find(sequenceNumber);
        if (it != SequenceNumberToStateHash_.end()) {
            EmplaceOrCrash(result, sequenceNumber, it->second.StateHash);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
