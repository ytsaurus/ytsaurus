#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/persistence.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/phoenix.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using NControllerAgent::TSaveContext;
using NControllerAgent::TLoadContext;
using NControllerAgent::TPersistenceContext;
using NControllerAgent::IPersistent;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ILegacySortedJobBuilder)
DECLARE_REFCOUNTED_STRUCT(INewSortedJobBuilder)

DECLARE_REFCOUNTED_CLASS(TNewJobManager)
DECLARE_REFCOUNTED_CLASS(TLegacyJobManager)

DECLARE_REFCOUNTED_STRUCT(IJobSizeTracker)

DECLARE_REFCOUNTED_STRUCT(ISortedStagingArea)

struct IShuffleChunkPool;

class TInputStreamDirectory;

class TNewJobStub;

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger ChunkPoolLogger("ChunkPool");
inline const NLogging::TLogger ChunkPoolStructuredLogger("ChunkPool");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

