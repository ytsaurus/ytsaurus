#pragma once

#include <yt/yt/ytlib/misc/public.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EIOEngineType,
    (ThreadPool)
    (Uring)
);

DECLARE_REFCOUNTED_STRUCT(IIOEngine)

DECLARE_REFCOUNTED_CLASS(TChunkFileReader)
DECLARE_REFCOUNTED_CLASS(TChunkFileWriter)

DECLARE_REFCOUNTED_CLASS(TIOTrackerConfig)

DECLARE_REFCOUNTED_STRUCT(IIOTracker)

class TIOEngineHandle;

using TIOEngineHandlePtr = TIntrusivePtr<TIOEngineHandle>;

struct TChunkFragmentDescriptor;

struct IBlocksExtCache;

struct TIOEvent;

constexpr i64 SectorSize = 512;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
