#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TStatisticsReporter
    : public TRefCounted
{
public:
    explicit TStatisticsReporter(IBootstrap* const bootstrap);

    void Start();

    void Reconfigure(
        const NClusterNode::TClusterNodeDynamicConfigPtr& oldConfig,
        const NClusterNode::TClusterNodeDynamicConfigPtr& newConfig);

private:
    void ReportStatistics();
    void DoReportStatistics();

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Spinlock_);

    IBootstrap* const Bootstrap_;
    NConcurrency::TActionQueuePtr ActionQueue_;
    NConcurrency::TPeriodicExecutorPtr Executor_;
    bool Enable_;
    NYPath::TYPath TablePath_;
};

DEFINE_REFCOUNTED_TYPE(TStatisticsReporter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
