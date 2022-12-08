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

void TStateHashChecker::Report(i64 sequenceNumber, ui64 stateHash)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto guard = WriterGuard(Lock_);

    auto it = SequenceNumberToStateHash_.find(sequenceNumber);
    if (it == SequenceNumberToStateHash_.end()) {
        YT_VERIFY(SequenceNumberToStateHash_.emplace(sequenceNumber, stateHash).second);

        if (std::ssize(SequenceNumberToStateHash_) > Limit_) {
            SequenceNumberToStateHash_.erase(SequenceNumberToStateHash_.begin());
        }
    } else if (it->second != stateHash) {
        YT_LOG_ALERT("State hashes differ (SequenceNumber: %v, ExpectedStateHash: %x, ActualStateHash: %x)",
            sequenceNumber,
            it->second,
            stateHash);
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
            EmplaceOrCrash(result, std::make_pair(sequenceNumber, it->second));
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
