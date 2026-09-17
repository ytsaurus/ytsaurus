#pragma once

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

class TLineageAggregator
{
public:
    void AddWorkerRatios(
        TIncarnationId workerIncarnationId,
        TLineageRatios ratios);

    void Update(
        const TFlowViewPtr& flowView,
        TInstant now = TInstant::Now());

private:
    struct TWorkerSnapshot
    {
        TLineageRatios Ratios;
        std::optional<TInstant> InactiveSince;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PendingWorkerRatiosLock_);
    THashMap<TIncarnationId, TLineageRatios> PendingWorkerRatios_;

    THashMap<TIncarnationId, TWorkerSnapshot> WorkerSnapshots_;
    TInstant NextAggregationTime_;
    TVersion LastPipelineSpecVersion_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
