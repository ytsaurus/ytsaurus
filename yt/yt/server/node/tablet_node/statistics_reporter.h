#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/client/api/public.h>

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
    using TOnRowCallback = TCallback<void(
        const NYPath::TYPath&,
        TRange<TUnversionedRow>,
        NTableClient::TRowBufferPtr&&,
        const std::vector<TTabletSnapshotPtr>&)>;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Spinlock_);

    IBootstrap* const Bootstrap_;

    NConcurrency::TActionQueuePtr ActionQueue_;
    NConcurrency::TPeriodicExecutorPtr Executor_;

    NProfiling::TProfiler Profiler_;
    NProfiling::TCounter ReportCount_;
    NProfiling::TCounter ReportErrorCount_;
    NProfiling::TCounter ReportedTabletCount_;
    NProfiling::TCounter LoadErrorCount_;
    NProfiling::TEventTimer ReportTime_;

    bool Started_ = false;
    bool Enable_ = false;
    int MaxTabletsPerTransaction_;
    TDuration ReportBackoffTime_;
    NYPath::TYPath TablePath_;

    static std::pair<i64, i64> GetDataSizes(const TTabletSnapshotPtr& tabletSnapshot);

    static NTableClient::TUnversionedRow MakeUnversionedRow(
        const TTabletSnapshotPtr& tabletSnapshot,
        const NTableClient::TRowBufferPtr& rowBuffer,
        bool keyColumnsOnly = false);

    void WriteRows(
        const NYPath::TYPath& tablePath,
        TRange<TUnversionedRow> rows,
        NTableClient::TRowBufferPtr&& rowBuffer,
        const std::vector<TTabletSnapshotPtr>& tabletSnapshots);

    TIntrusivePtr<NApi::IUnversionedRowset> LookupRows(
        const NYPath::TYPath& tablePath,
        TRange<TUnversionedRow> keys,
        NTableClient::TRowBufferPtr&& rowBuffer);

    void LoadStatistics(
        const NYPath::TYPath& tablePath,
        TRange<TUnversionedRow> keys,
        NTableClient::TRowBufferPtr&& rowBuffer,
        const std::vector<TTabletSnapshotPtr>& tabletSnapshots);

    THashMap<TTabletId, TTabletSnapshotPtr> GetLatestTabletSnapshots();

    void ReportStatistics(
        const std::vector<TTabletSnapshotPtr>& tabletsWithStatistics,
        const NYPath::TYPath& tablePath,
        i64 maxTabletsPerTransaction,
        TDuration loadBackoffTime);

    void LoadUninitializedStatistics(
        const std::vector<TTabletSnapshotPtr>& tabletsWithoutStatistics,
        const NYPath::TYPath& tablePath,
        i64 maxTabletsPerTransaction,
        TDuration loadBackoffTime);

    void ProcessStatistics();

    void DoProcessStatistics(
        const NYPath::TYPath& tablePath,
        i64 maxTabletsPerTransaction,
        bool keyColumnsOnly,
        const std::vector<TTabletSnapshotPtr>& tabletSnapshots,
        TOnRowCallback processRows);
};

DEFINE_REFCOUNTED_TYPE(TStatisticsReporter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
