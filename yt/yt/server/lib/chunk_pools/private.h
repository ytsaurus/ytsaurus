#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/persistence.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using NControllerAgent::TSaveContext;
using NControllerAgent::TLoadContext;
using NControllerAgent::TPersistenceContext;
using NControllerAgent::IPersistent;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISortedJobBuilder)

DECLARE_REFCOUNTED_CLASS(TJobManager)

struct IShuffleChunkPool;

class TInputStreamDirectory;

class TJobStub;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, ChunkPoolLogger, "ChunkPool");
YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, ChunkPoolStructuredLogger, "ChunkPool");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

