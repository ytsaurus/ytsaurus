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

DECLARE_REFCOUNTED_STRUCT(ILegacySortedJobBuilder)
DECLARE_REFCOUNTED_STRUCT(INewSortedJobBuilder)

DECLARE_REFCOUNTED_CLASS(TNewJobManager)
DECLARE_REFCOUNTED_CLASS(TLegacyJobManager)

struct IShuffleChunkPool;

class TInputStreamDirectory;

class TNewJobStub;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, ChunkPoolLogger, "ChunkPool");
YT_DEFINE_GLOBAL(const NLogging::TLogger, ChunkPoolStructuredLogger, "ChunkPool");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

