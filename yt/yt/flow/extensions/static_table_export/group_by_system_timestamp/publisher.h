#pragma once

#include "writer_storage.h"

#include <yt/yt/flow/library/cpp/misc/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <library/cpp/yt/system/spin_lock.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TStaticTableExportPublisherInput
{
    //! Zero while the source topology is unknown.
    int PartitionCount = 0;
    //! The persisted event watermark of the export stream; it advances only after durable commits.
    TSystemTimestamp EventWatermark;
    //! Every partition is drained and acknowledged, so nothing can still target an unpublished window.
    bool AllDrained = false;
    //! The earliest first write time reported by the current jobs and the number of partitions that reported one.
    //! A partition without messages reports the start of its job.
    std::optional<TSystemTimestamp> FirstWriteTimestamp;
    int FirstWriteReportCount = 0;
    //! Partitions whose draining jobs got no message; they do not constrain the start.
    int SilentDrainingCount = 0;
    //! Silent partitions whose jobs complete a finite source; when all are, the source has nothing to export.
    int SilentCompletingCount = 0;
    TDynamicStaticTableExportParametersPtr Dynamic;
};

//! Returns the write time the export starts from, or null until every partition reported one or drains silently.
std::optional<TSystemTimestamp> GetStaticTableExportStart(const TStaticTableExportPublisherInput& input);

//! Bootstraps the export and publishes closed windows in a background fiber.
class TStaticTableExportPublisher
    : public TRefCounted
{
public:
    TStaticTableExportPublisher(
        TStaticTableExportWriterStoragePtr storage,
        TStaticTableExportParametersPtr parameters,
        TBuildStaticTableExportPublication buildPublication,
        IInvokerPtr invoker,
        IStatusErrorStatePtr errorState,
        const NLogging::TLogger& logger,
        const NProfiling::TProfiler& profiler);
    ~TStaticTableExportPublisher() override;

    void Update(TStaticTableExportPublisherInput input);

private:
    const TStaticTableExportWriterStoragePtr Storage_;
    const TStaticTableExportParametersPtr Parameters_;
    const TBuildStaticTableExportPublication BuildPublication_;
    const IStatusErrorStatePtr ErrorState_;
    const NLogging::TLogger Logger;
    const NProfiling::TGauge ClosedThrough_;
    const NConcurrency::TPeriodicExecutorPtr Executor_;

    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    TStaticTableExportPublisherInput Input_;

    TInstant RetryAfter_;

    void Publish();
};

DEFINE_REFCOUNTED_TYPE(TStaticTableExportPublisher);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
