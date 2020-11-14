#pragma once

#include "public.h"

#include <yt/server/lib/controller_agent/persistence.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/phoenix.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using NControllerAgent::TSaveContext;
using NControllerAgent::TLoadContext;
using NControllerAgent::TPersistenceContext;
using NControllerAgent::IPersistent;

using TIntCookie = int;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ILegacySortedJobBuilder);
DECLARE_REFCOUNTED_STRUCT(INewSortedJobBuilder);

DECLARE_REFCOUNTED_CLASS(TJobManager)

DECLARE_REFCOUNTED_CLASS(TOutputOrder)

struct IShuffleChunkPool;

class TInputStreamDirectory;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkPoolLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

