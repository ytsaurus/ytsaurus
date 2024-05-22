#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChunkClientLogger, "ChunkClient");
YT_DEFINE_GLOBAL(const NLogging::TLogger, ReaderMemoryManagerLogger, "ReaderMemoryManager");

//! A suffix to distinguish chunk meta files.
inline const TString ChunkMetaSuffix(".meta");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

