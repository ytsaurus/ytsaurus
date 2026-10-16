#include "partition_buffer_state.h"

#include <library/cpp/yt/memory/new.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

class TDetachedPartitionBufferState
    : public IPartitionBufferState
{
public:
    explicit TDetachedPartitionBufferState(TStreamLimitUsageStateMap outputStreamLimitUsageStates)
        : OutputStreamLimitUsageStates_(std::move(outputStreamLimitUsageStates))
    { }

    void SeedWarmup(const TPartitionBufferWarmup& /*warmup*/) override
    { }

    TPartitionBufferWarmup GetWarmup() override
    {
        return {};
    }

    bool IsWarmupEnabled() override
    {
        return false;
    }

    TDuration GetWarmupRefreshPeriod() override
    {
        return DefaultWarmupRefreshPeriod;
    }

    const TStreamLimitUsageStateMap& GetOutputStreamLimitUsageStates() const override
    {
        return OutputStreamLimitUsageStates_;
    }

private:
    const TStreamLimitUsageStateMap OutputStreamLimitUsageStates_;
};

} // namespace

IPartitionBufferStatePtr CreateDetachedPartitionBufferState(TStreamLimitUsageStateMap outputStreamLimitUsageStates)
{
    return New<TDetachedPartitionBufferState>(std::move(outputStreamLimitUsageStates));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
