#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkClientLogger;

//! A suffix to distinguish chunk meta files.
extern const Stroka ChunkMetaSuffix;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

