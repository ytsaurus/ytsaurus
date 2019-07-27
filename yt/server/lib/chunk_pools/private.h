#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/phoenix.h>

#include <yt/client/table_client/serialize.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSaveContext;
using NTableClient::TLoadContext;
using NTableClient::TPersistenceContext;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

using TIntCookie = int;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ISortedJobBuilder);

DECLARE_REFCOUNTED_CLASS(TJobManager)

DECLARE_REFCOUNTED_CLASS(TOutputOrder)

DECLARE_REFCOUNTED_CLASS(TBernoulliSampler)

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct IShuffleChunkPool;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkPoolLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

