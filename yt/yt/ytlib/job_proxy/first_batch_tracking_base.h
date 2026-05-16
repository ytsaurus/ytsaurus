#pragma once

#include <util/datetime/base.h>

namespace NYT::NJobProxy {

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
