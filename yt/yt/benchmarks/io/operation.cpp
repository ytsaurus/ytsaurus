#include "operation.h"
#include "meters.h"
#include "throttler.h"

#include <util/random/random.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

class TStopper
{
public:
    explicit TStopper(const TConfigurationPtr& configuration)
        : TimeLimit_(configuration->TimeLimit)
        , TransferLimit_(configuration->TransferLimit)
        , ShotCountLimit_(configuration->ShotCount)
        , Throttler_(CreateCombinedThrottler(configuration->Throttler))
    { }

    EBurstAction Account(i64 transfer)
    {
        if (TimeLimit_ && TimeMeter_.GetElapsed() >= *TimeLimit_) {
            return EBurstAction::Stop;
        }

        if (ShotCountLimit_ && ShotCount_ >= *ShotCountLimit_) {
            return EBurstAction::Stop;
        }

        if (TransferLimit_ && Transferred_ >= *TransferLimit_) {
            return EBurstAction::Stop;
        }

        if (!Throttler_->IsAvailable(transfer)) {
            return EBurstAction::Wait;
        }

        Throttler_->Acquire(transfer);

        ++ShotCount_;
        Transferred_ += transfer;

        return EBurstAction::Continue;
    }

private:
    const std::optional<TDuration> TimeLimit_;
    const std::optional<i64> TransferLimit_;
    const std::optional<i64> ShotCountLimit_;
    IThrottlerPtr Throttler_;
    TLatencyMeter TimeMeter_;
    i64 Transferred_ = 0;
    i64 ShotCount_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

EOperationType RandomOperationType(int readPercentage)
{
    return 1 + RandomNumber<unsigned int>(99) <= static_cast<size_t>(readPercentage)
        ? EOperationType::Read
        : EOperationType::Write;
}

TOperationGenerator CreateRandomOperationGenerator(TConfigurationPtr configuration)
{
    return [
            configuration,
            stopper = TStopper(configuration)
        ] () mutable {
            auto blockSizeLog = configuration->BlockSizeLog;
            i64 size = (1L << blockSizeLog);

            auto action = stopper.Account(size);
            if (action == EBurstAction::Wait) {
                TOperation operation;
                operation.BurstAction = EBurstAction::Wait;
                return operation;
            }

            auto fileIndex = RandomNumber<unsigned int>(configuration->Files.size());
            auto& file = configuration->Files[fileIndex];

            i64 totalBlocks = file.Size >> blockSizeLog;
            i64 start = totalBlocks * configuration->Zone.first / 100;
            i64 end = totalBlocks * configuration->Zone.second / 100;

            i64 blockIndex = start + RandomNumber<unsigned long>(end - start);

            auto type = RandomOperationType(configuration->ReadPercentage);

            return TOperation{
                .Type = type,
                .FileIndex = fileIndex,
                .Offset = blockIndex << blockSizeLog,
                .Size = size,
                .BurstAction = action
            };
        };
}

TOperationGenerator CreateSequentialOperationGenerator(TConfigurationPtr configuration, bool startFromBegin, int threadIndex)
{
    // TODO(savrus) Support zones.

    auto positions = std::vector<i64>(configuration->Files.size());
    if (!startFromBegin) {
        for (int index = 0; index < std::ssize(positions); ++index) {
            auto& file = configuration->Files[index];
            positions[index] = RandomNumber<unsigned long>(file.Size) & ((1L << configuration->BlockSizeLog) - 1);
        }
    }
    return [
            configuration,
            threadIndex,
            positions = std::move(positions),
            stopper = TStopper(configuration)
        ] () mutable {
            i64 size = (1L << configuration->BlockSizeLog);

            auto action = stopper.Account(size);
            if (action == EBurstAction::Wait) {
                TOperation operation;
                operation.BurstAction = EBurstAction::Wait;
                return operation;
            }

            //auto fileIndex = RandomNumber<unsigned int>(configuration->Files.size());
            unsigned int fileIndex = threadIndex % configuration->Files.size();
            auto& file = configuration->Files[fileIndex];
            i64 offset = positions[fileIndex];
            if (offset + size > file.Size) {
                offset = 0;
            }
            positions[fileIndex] = offset + size;
            auto type = RandomOperationType(configuration->ReadPercentage);

            return TOperation {
                .Type = type,
                .FileIndex = fileIndex,
                .Offset = offset,
                .Size = size,
                .BurstAction = action
            };
        };
}

TOperationGenerator CreateOperationGenerator(TConfigurationPtr configuration, int threadIndex)
{
    switch (configuration->Pattern) {
        case EPattern::Sequential:
            return CreateSequentialOperationGenerator(std::move(configuration), true, threadIndex);

        case EPattern::Linear:
            return CreateSequentialOperationGenerator(std::move(configuration), false, threadIndex);

        case EPattern::Random:
            return CreateRandomOperationGenerator(std::move(configuration));

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
