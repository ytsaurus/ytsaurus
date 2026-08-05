#include "stream_inflight_limits.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 ComputeSeq(const TStreamUsage& usage)
{
    return static_cast<ui64>(usage.CumulativeByteIn) + static_cast<ui64>(usage.CumulativeByteOut) + static_cast<ui64>(usage.CumulativeCountIn) + static_cast<ui64>(usage.CumulativeCountOut);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TStreamLimitUsageState::TStreamLimitUsageState(i64 inflationPerMessage)
    : InflationPerMessage_(inflationPerMessage)
{ }

void TStreamLimitUsageState::Update(const TStreamUsage& usage)
{
    i64 inflatedInflightBytes = usage.GetInflatedInflightBytes(InflationPerMessage_);
    i64 maxSeen = MaxInflatedInflightBytes_.load(std::memory_order_relaxed);
    while (inflatedInflightBytes > maxSeen &&
        !MaxInflatedInflightBytes_.compare_exchange_weak(maxSeen, inflatedInflightBytes, std::memory_order_relaxed))
    {
    }

    PendingInflatedBytes_.store(static_cast<ui64>(usage.PendingInflatedBytes), std::memory_order_relaxed);
    CumulativeByteIn_.store(static_cast<ui64>(usage.CumulativeByteIn), std::memory_order_relaxed);
    CumulativeByteOut_.store(static_cast<ui64>(usage.CumulativeByteOut), std::memory_order_relaxed);
    CumulativeCountIn_.store(static_cast<ui64>(usage.CumulativeCountIn), std::memory_order_relaxed);
    CumulativeCountOut_.store(static_cast<ui64>(usage.CumulativeCountOut), std::memory_order_relaxed);
    Seq_.store(ComputeSeq(usage), std::memory_order_release);
}

// Sum-as-seq seqlock: the single writer publishes a release store of the
// monotonically-growing sum of the four cumulative counters after the relaxed
// stores of the counters themselves. The reader spins until the sum of its
// snapshot matches the seq it loaded — any torn read strictly increases the
// sum (writers are monotonic), so the equality check rejects it. PendingInflatedBytes
// is diagnostic and rides outside the seq.
TStreamUsage TStreamLimitUsageState::Read() const
{
    while (true) {
        auto seq = Seq_.load(std::memory_order_acquire);
        TStreamUsage usage{
            .CumulativeByteIn = static_cast<i64>(CumulativeByteIn_.load(std::memory_order_relaxed)),
            .CumulativeByteOut = static_cast<i64>(CumulativeByteOut_.load(std::memory_order_relaxed)),
            .CumulativeCountIn = static_cast<i64>(CumulativeCountIn_.load(std::memory_order_relaxed)),
            .CumulativeCountOut = static_cast<i64>(CumulativeCountOut_.load(std::memory_order_relaxed)),
            .PendingInflatedBytes = static_cast<i64>(PendingInflatedBytes_.load(std::memory_order_relaxed)),
        };
        if (ComputeSeq(usage) == seq) {
            return usage;
        }
    }
}

i64 TStreamLimitUsageState::ReadAndResetMaxInflatedInflightBytes()
{
    return MaxInflatedInflightBytes_.exchange(0, std::memory_order_relaxed);
}

void TStreamLimitUsageState::SetEstimatedInflatedSpeed(double inflatedBytesPerSecond)
{
    EstimatedInflatedSpeed_.store(inflatedBytesPerSecond, std::memory_order_relaxed);
}

double TStreamLimitUsageState::GetEstimatedInflatedSpeed() const
{
    return EstimatedInflatedSpeed_.load(std::memory_order_relaxed);
}

void TStreamLimitUsageState::SetOfferedInflatedBytesPerSecond(double rate)
{
    OfferedInflatedBytesPerSecond_.store(rate, std::memory_order_relaxed);
}

void TStreamLimitUsageState::SetOfferedRawRate(double bytesPerSecond, double messagesPerSecond)
{
    SetOfferedInflatedBytesPerSecond(bytesPerSecond + messagesPerSecond * InflationPerMessage_);
}

double TStreamLimitUsageState::GetOfferedInflatedBytesPerSecond() const
{
    return OfferedInflatedBytesPerSecond_.load(std::memory_order_relaxed);
}

void TStreamLimitUsageState::SetLimitBytes(i64 limitBytes)
{
    LimitBytes_.store(limitBytes, std::memory_order_relaxed);
}

i64 TStreamLimitUsageState::GetLimitBytes() const
{
    return LimitBytes_.load(std::memory_order_relaxed);
}

i64 TStreamLimitUsageState::GetInflationPerMessage() const
{
    return InflationPerMessage_;
}

bool TStreamLimitUsageState::IsUsageWithinLimits(const TStreamUsage& usage) const
{
    return usage.GetInflatedInflightBytes(InflationPerMessage_) <= GetLimitBytes();
}

////////////////////////////////////////////////////////////////////////////////

THashSet<TStreamId> GetStreamsWithinLimits(const TStreamLimitUsageStateMap& states)
{
    THashSet<TStreamId> result;
    for (const auto& [streamId, state] : states) {
        if (state->IsUsageWithinLimits(state->Read())) {
            result.insert(streamId);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
