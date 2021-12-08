#include "state_hash_checker.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TStateHashChecker::TStateHashChecker(
    int limit,
    NLogging::TLogger logger)
    : Limit_(limit)
    , Logger(std::move(logger))
{ }

void TStateHashChecker::Report(i64 sequenceNumber, ui64 stateHash)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto it = SequenceNumberToStateHash_.find(sequenceNumber);
    if (it == SequenceNumberToStateHash_.end()) {
        YT_VERIFY(SequenceNumberToStateHash_.emplace(sequenceNumber, stateHash).second);

        if (std::ssize(SequenceNumberToStateHash_) > Limit_) {
            SequenceNumberToStateHash_.erase(SequenceNumberToStateHash_.begin());
        }
    } else if (it->second != stateHash) {
        YT_LOG_ALERT("State hashes differ (SequenceNumber: %v, ExpectedStateHash: %llx, ActualStateHash: %llx)",
            sequenceNumber,
            it->second,
            stateHash);
    }
}

std::optional<ui64> TStateHashChecker::GetStateHash(i64 sequenceNumber)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto it = SequenceNumberToStateHash_.find(sequenceNumber);

    return it == SequenceNumberToStateHash_.end() ? std::nullopt : std::make_optional(it->second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
