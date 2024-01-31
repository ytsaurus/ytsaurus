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

    void Reconfigure(const NClusterNode::TClusterNodeDynamicConfigPtr& config);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Spinlock_);

    bool Started_ = false;
    bool Enable_ = false;

    IBootstrap* const Bootstrap_;

    NConcurrency::TActionQueuePtr ActionQueue_;
    NConcurrency::TPeriodicExecutorPtr Executor_;

    NYPath::TYPath TablePath_;
    i64 MaxTabletsPerTransaction_;
    TDuration ReportBackoffTime_;

    NProfiling::TProfiler Profiler_;
    NProfiling::TCounter ReportCount_;
    NProfiling::TCounter ReportErrorCount_;
    NProfiling::TCounter ReportedTabletCount_;
    NProfiling::TEventTimer ReportTime_;

    static std::pair<i64, i64> GetDataSizes(const TTabletSnapshotPtr& tabletSnapshot);

    static NTableClient::TUnversionedRow MakeUnversionedRow(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NTableClient::TRowBufferPtr& rowBuffer);

    void WriteRows(
        const NYPath::TYPath& tablePath,
        TRange<TUnversionedRow> rows,
        NTableClient::TRowBufferPtr&& rowBuffer);

    void ReportStatistics();
    void DoReportStatistics(const NYPath::TYPath& tablePath, i64 maxTabletCountInTransaction);
};

DEFINE_REFCOUNTED_TYPE(TStatisticsReporter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
