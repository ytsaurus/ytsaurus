#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkClientLogger;
extern const NLogging::TLogger ReaderMemoryManagerLogger;

//! A suffix to distinguish chunk meta files.
extern const TString ChunkMetaSuffix;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

