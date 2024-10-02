#include "batch_size_backoff.h"

#include "config.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

TBatchSizeBackoff::TBatchSizeBackoff(const NLogging::TLogger& logger, TBatchSizeBackoffConfigPtr config, i64 initialBatchSize)
    : Logger(logger)
    , Config_(std::move(config))
    , Current_(initialBatchSize)
    , Additive_(Config_->BatchSizeAdditive)
    , Limit_(Config_->BatchSizeLimit)
{ }

i64 TBatchSizeBackoff::operator*() const
{
    return Current_;
}

void TBatchSizeBackoff::Next()
{
    auto nextBatchSize = Current_ + Additive_;
    if (IncreasingExponentially_) {
        nextBatchSize = std::max(nextBatchSize, Current_ * Config_->BatchSizeMultiplier);
    }
    Current_ = std::min(nextBatchSize, Limit_);
}

void TBatchSizeBackoff::Rollback()
{
    IncreasingExponentially_ = false;
    if (Config_->BatchSizeAdditiveRelativeToMax) {
        Additive_ = std::max(
            i64(1),
            static_cast<i64>(*Config_->BatchSizeAdditiveRelativeToMax * Current_));
        YT_LOG_DEBUG(
            "Adjusted batch size additive "
            "relative to the current maximum possible batch size ("
            "BatchSizeAdditive: %v, "
            "MaxBatchSize: %v)",
            Additive_,
            Current_);
    }
    Current_ /= Config_->BatchSizeMultiplier;
    Current_ = std::max(i64(1), Current_);
}

void TBatchSizeBackoff::RestrictLimit(i64 value)
{
    Limit_ = std::min(Limit_, value);
    Current_ = std::min(Current_, Limit_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
