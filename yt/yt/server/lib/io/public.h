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

struct TChunkFragmentDescriptor;

struct IBlocksExtCache;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
