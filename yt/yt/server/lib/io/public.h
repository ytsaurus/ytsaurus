#pragma once

#include <yt/yt/ytlib/misc/public.h>

#include <library/cpp/yt/cpu_clock/public.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EHugeManagerType,
    (Preallocated)
    (Transparent)
);

DEFINE_ENUM(EIOEngineType,
    (ThreadPool)
    (Uring)
    (FairShareThreadPool)
    (FairShareUring)
    (FairShareHierarchical)
);

DEFINE_ENUM(EDirectIOPolicy,
    (Always)
    (Never)
    (OnDemand)
);

DECLARE_REFCOUNTED_STRUCT(IIOEngine)
DECLARE_REFCOUNTED_STRUCT(IDynamicIOEngine)

DECLARE_REFCOUNTED_CLASS(TChunkFileReader)
DECLARE_REFCOUNTED_CLASS(TChunkFileWriter)

DECLARE_REFCOUNTED_STRUCT(TIOTrackerConfig)
DECLARE_REFCOUNTED_STRUCT(TCongestionDetectorConfig)
DECLARE_REFCOUNTED_STRUCT(TGentleLoaderConfig)

DECLARE_REFCOUNTED_STRUCT(IIOTracker)

DECLARE_REFCOUNTED_STRUCT(IIOEngineWorkloadModel)
DECLARE_REFCOUNTED_STRUCT(IRandomFileProvider)
DECLARE_REFCOUNTED_STRUCT(IGentleLoader)

DECLARE_REFCOUNTED_STRUCT(IReadRequestCombiner)

DECLARE_REFCOUNTED_STRUCT(ILoadAdjuster)

DECLARE_REFCOUNTED_CLASS(TIOJobState)

DECLARE_REFCOUNTED_STRUCT(THugePageManagerConfig)
DECLARE_REFCOUNTED_STRUCT(THugePageManagerDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(IHugePageManager)

struct TIOFairShareState
{
    i64 IOConsumed = 0;
    double IOFairShareWeight = 0;
    //! Identifies the job; requests of one job share the fair-share state.
    //! If empty, the request gets its own job state that is neither shared nor retained.
    TGuid ConsumerId;
    //! The monotonic instant at which this node started handling the report.
    TCpuInstant SnapshotAt = 0;

    bool operator==(const TIOFairShareState& other) const = default;
};

class TIOEngineHandle;
using TIOEngineHandlePtr = TIntrusivePtr<TIOEngineHandle>;

struct TChunkFragmentDescriptor;

struct IBlocksExtCache;

struct TIOEvent;

struct TRequestSizes;

constexpr i64 SectorSize = 512;

DECLARE_REFCOUNTED_STRUCT(TBlocksExt)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
