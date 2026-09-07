#pragma once

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

class TLineageRateAggregator
{
public:
    void AddWorkerRates(
        TIncarnationId workerIncarnationId,
        TLineageRates rates);

    void Update(
        const TFlowViewPtr& flowView,
        TInstant now = TInstant::Now());

private:
    struct TWorkerSnapshot
    {
        TLineageRates Rates;
        std::optional<TInstant> InactiveSince;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PendingWorkerRatesLock_);
    THashMap<TIncarnationId, TLineageRates> PendingWorkerRates_;

    THashMap<TIncarnationId, TWorkerSnapshot> WorkerSnapshots_;
    TInstant NextAggregationTime_;
    TVersion LastPipelineSpecVersion_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
