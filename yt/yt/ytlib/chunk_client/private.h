#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ChunkClientLogger("ChunkClient");
inline const NLogging::TLogger ReaderMemoryManagerLogger("ReaderMemoryManager");

//! A suffix to distinguish chunk meta files.
inline const TString ChunkMetaSuffix(".meta");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

