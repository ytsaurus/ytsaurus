#pragma once

#include <library/cpp/yt/cpu_clock/public.h>

#include <util/datetime/base.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TFirstBatchTimeTrackingBase
{
public:
    TFirstBatchTimeTrackingBase(TCpuInstant startTime);
    std::optional<TDuration> GetTimeToFirstBatch() const;

protected:
    void TryUpdateFirstBatchTime();

private:
    const TCpuInstant StartTime_;
    std::atomic<TCpuInstant> FirstBatchTime_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
