#ifndef ERASURE_ADAPTIVE_REPAIR_INL_H_
#error "Direct inclusion of this file is not allowed, include erasure_adaptive_repair.h"
// For the sake of sane code completion.
#include "erasure_adaptive_repair.h"
#endif

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TResultType>
TFuture<TResultType> TAdaptiveErasureRepairingSession::Run(TDoRepairAttempt<TResultType> doRepairAttempt)
{
    return BIND(&TAdaptiveErasureRepairingSession::DoRun<TResultType>, MakeStrong(this), doRepairAttempt)
        .AsyncVia(Invoker_)
        .Run();
}

template <typename TResultType>
TResultType TAdaptiveErasureRepairingSession::DoRun(TDoRepairAttempt<TResultType> doRepairAttempt)
{
    std::optional<NErasure::TPartIndexSet> erasedIndicesOnPreviousIteration;
    std::vector<TError> innerErrors;

    static const int MaxAttemptCount = 5;

    for (int attempt = 0; attempt < MaxAttemptCount; ++attempt) {
        const auto bannedPartIndices = CalculateBannedParts();
        const auto bannedPartIndicesList = ToReadersIndexList(bannedPartIndices);

        if (erasedIndicesOnPreviousIteration && bannedPartIndices == *erasedIndicesOnPreviousIteration) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::AutoRepairFailed,
                "Error reading chunk %v with repair; cannot proceed since the list of valid underlying part readers did not change",
                ChunkId_)
                << TErrorAttribute("banned_part_indexes", bannedPartIndicesList)
                << innerErrors;
        }

        erasedIndicesOnPreviousIteration = bannedPartIndices;

        auto optionalRepairIndices = Codec_->GetRepairIndices(bannedPartIndicesList);
        if (!optionalRepairIndices) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::AutoRepairFailed,
                "Not enough parts to read chunk %v with repair",
                ChunkId_)
                << TErrorAttribute("banned_part_indexes", bannedPartIndicesList)
                << innerErrors;
        }

        // Figure out which indices are available for read.
        NErasure::TPartIndexSet alivePartIndices;
        auto markAlivePartIndices = [&] (const NErasure::TPartIndexList& parts) {
            for (auto targetPart : parts) {
                if (!bannedPartIndices.test(targetPart)) {
                    alivePartIndices.set(targetPart);
                }
            };
        };
        markAlivePartIndices(*optionalRepairIndices);
        markAlivePartIndices(Target_.Existing);

        std::vector<IChunkReaderAllowingRepairPtr> readers;
        for (int index = 0; index < std::ssize(Readers_); ++index) {
            if (alivePartIndices.test(index)) {
                readers.push_back(Readers_[index]);
            }
        }

        auto unpackValue = [] (const TErrorOr<TResultType>& maybeValue) {
            if constexpr(!std::is_void<TResultType>::value) {
                return maybeValue.Value();
            }
        };

        auto future = doRepairAttempt(bannedPartIndicesList, readers);
        auto result = NConcurrency::WaitFor(future);
        if (result.IsOK()) {

            if (attempt) {
                AdaptivelyRepairedCounter_.Increment();
            }

            return unpackValue(result);
        } else {
            innerErrors.push_back(result);
        }
    }

    THROW_ERROR_EXCEPTION(
        NChunkClient::EErrorCode::AutoRepairFailed,
        "Max attempt count exceeded")
        << TErrorAttribute("max_attempt_count", MaxAttemptCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
