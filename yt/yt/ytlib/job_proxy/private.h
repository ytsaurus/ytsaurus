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
    TFirstBatchTimeTrackingBase(TInstant startTime);
    std::optional<TDuration> GetTimeToFirstBatch() const;

protected:
    void TryUpdateFirstBatchTime();

private:
    const TInstant StartTime_;
    std::atomic<TInstant> FirstBatchTime_ = TInstant::Zero();
};

} // namespace NYT::NJobProxy
