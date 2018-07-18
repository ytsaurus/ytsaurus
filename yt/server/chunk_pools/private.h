#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

#include <yt/core/misc/phoenix.h>

#include <yt/client/table_client/serialize.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSaveContext;
using NTableClient::TLoadContext;
using NTableClient::TPersistenceContext;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

using TIntCookie = int;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobManager)

DECLARE_REFCOUNTED_CLASS(TOutputOrder)

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct IShuffleChunkPool;

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger ChunkPoolLogger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

