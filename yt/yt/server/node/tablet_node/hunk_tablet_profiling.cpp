#include "hunk_tablet_profiling.h"

#include "private.h"
#include "hunk_tablet.h"

#include <library/cpp/yt/memory/leaky_ref_counted_singleton.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

THunkWriteCounters::THunkWriteCounters(const TProfiler& profiler)
    : RowCount(profiler.Counter("/write/row_count"))
    , DataWeight(profiler.Counter("/write/data_weight"))
    , SuccessfulRowCount(profiler.Counter("/write/successful_row_count"))
    , SuccessfulDataWeight(profiler.Counter("/write/successful_data_weight"))
{ }

THunkTabletServiceCounters::THunkTabletServiceCounters(const TProfiler& profiler)
    : WriteHunks(profiler.WithPrefix("/write_hunks"))
{ }

THunkTabletCounters::THunkTabletCounters(const NProfiling::TProfiler& profiler)
    : StoreCount(profiler.GaugeSummary("/hunk_tablet/store_count"))
    , PassiveStoreCount(profiler.GaugeSummary("/hunk_tablet/passive_store_count"))
    , LockingTabletCount(profiler.GaugeSummary("/hunk_tablet/locking_tablet_count"))
{ }

THunkTabletScannerCounters::THunkTabletScannerCounters(const NProfiling::TProfiler& profiler)
    : StoreAllocationCount(profiler.Counter("/hunk_tablet_scanner/store_allocation_count"))
    , StoreRotationCount(profiler.Counter("/hunk_tablet_scanner/store_rotation_count"))
    , StoreSealCount(profiler.Counter("/hunk_tablet_scanner/store_seal_count"))
    , StoreRemovalCount(profiler.Counter("/hunk_tablet_scanner/store_removal_count"))
    , FailedScanCount(profiler.Counter("/hunk_tablet_scanner/failed_scan_count"))
{ }

////////////////////////////////////////////////////////////////////////////////

THunkTabletProfiler::THunkTabletProfiler(const NProfiling::TProfiler& profiler)
    : Disabled_(false)
    , Profiler_(profiler)
{
    if (!Disabled_) {
        HunkTabletCounters_ = THunkTabletCounters(Profiler_);
        HunkTabletScannerCounters_ = THunkTabletScannerCounters(Profiler_);
        JournalWriterCounters_ = TJournalWriterPerformanceCounters(Profiler_.WithPrefix("/journal_writer"));
    }
}

THunkTabletProfilerPtr THunkTabletProfiler::GetDisabled()
{
    return LeakyRefCountedSingleton<THunkTabletProfiler>();
}

THunkWriteCounters* THunkTabletProfiler::GetWriteCounters(const std::optional<std::string>& userTag)
{
    return WriteCounters_.Get(Disabled_, userTag, Profiler_);
}

THunkTabletServiceCounters* THunkTabletProfiler::GetTabletServiceCounters(const std::optional<std::string>& userTag)
{
    return TabletServiceCounters_.Get(Disabled_, userTag, Profiler_);
}

THunkTabletCounters* THunkTabletProfiler::GetHunkTabletCounters()
{
    if (Disabled_) {
        static THunkTabletCounters staticCounters;
        return &staticCounters;
    }

    return &HunkTabletCounters_.value();
}

THunkTabletScannerCounters* THunkTabletProfiler::GetHunkTabletScannerCounters()
{
    if (Disabled_) {
        static THunkTabletScannerCounters staticCounters;
        return &staticCounters;
    }

    return &HunkTabletScannerCounters_.value();
}

TJournalWriterPerformanceCounters* THunkTabletProfiler::GetJournalWriterCounters()
{
    if (Disabled_) {
        static TJournalWriterPerformanceCounters staticCounters;
        return &staticCounters;
    }

    return &JournalWriterCounters_.value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
