#pragma once

#include <yt/yt/core/logging/log.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TAnyToYsonConverter)

YT_DEFINE_GLOBAL(const NLogging::TLogger, JobProxyClientLogger, "JobProxyClient");

////////////////////////////////////////////////////////////////////////////////


class TFirstBatchTimeTrackingBase
{
public:
    TFirstBatchTimeTrackingBase(TCpuInstant startTime);
    std::optional<TCpuDuration> GetTimeToFirstBatch() const;

protected:
    void TryUpdateFirstBatchTime();

private:
    const TCpuInstant StartTime_;
    std::atomic<TCpuInstant> FirstBatchTime_ = 0;
};

} // namespace NYT::NJobProxy
