#pragma once

#include "public.h"
#include "tablet_profiling_base.h"

#include <yt/yt/server/lib/misc/profiling_helpers.h>

#include <yt/yt/client/api/journal_client.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct THunkWriteCounters
{
    THunkWriteCounters() = default;

    explicit THunkWriteCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter RowCount;
    NProfiling::TCounter DataWeight;
    NProfiling::TCounter SuccessfulRowCount;
    NProfiling::TCounter SuccessfulDataWeight;
};

struct THunkTabletServiceCounters
{
    THunkTabletServiceCounters() = default;

    explicit THunkTabletServiceCounters(const NProfiling::TProfiler& profiler);

    NServer::TMethodCounters WriteHunks;
};

struct THunkTabletCounters
{
    THunkTabletCounters() = default;

    explicit THunkTabletCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TGauge StoreCount;
    NProfiling::TGauge PassiveStoreCount;

    NProfiling::TGauge LockingTabletCount;
};

struct THunkTabletScannerCounters
{
    THunkTabletScannerCounters() = default;

    explicit THunkTabletScannerCounters(const NProfiling::TProfiler& profiler);

    NProfiling::TCounter StoreAllocationCount;
    NProfiling::TCounter StoreRotationCount;
    NProfiling::TCounter StoreSealCount;
    NProfiling::TCounter StoreRemovalCount;

    NProfiling::TCounter FailedScanCount;
};

////////////////////////////////////////////////////////////////////////////////

class THunkTabletProfiler
    : public TRefCounted
{
public:
    THunkTabletProfiler() = default;

    THunkTabletProfiler(const NProfiling::TProfiler& profiler);

    static THunkTabletProfilerPtr GetDisabled();

    THunkWriteCounters* GetWriteCounters(const std::optional<std::string>& userTag);
    THunkTabletServiceCounters* GetTabletServiceCounters(const std::optional<std::string>& userTag);
    THunkTabletCounters* GetHunkTabletCounters();
    THunkTabletScannerCounters* GetHunkTabletScannerCounters();
    NApi::TJournalWriterPerformanceCounters* GetJournalWriterCounters();

private:
    const bool Disabled_ = true;
    const NProfiling::TProfiler Profiler_ = {};

    TUserTaggedCounter<THunkWriteCounters> WriteCounters_;
    TUserTaggedCounter<THunkTabletServiceCounters> TabletServiceCounters_;
    std::optional<THunkTabletCounters> HunkTabletCounters_;
    std::optional<THunkTabletScannerCounters> HunkTabletScannerCounters_;
    std::optional<NApi::TJournalWriterPerformanceCounters> JournalWriterCounters_;
};

DEFINE_REFCOUNTED_TYPE(THunkTabletProfiler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
