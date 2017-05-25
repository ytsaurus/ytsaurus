#pragma once

#include "public.h"

#include <yt/core/misc/phoenix.h>

#include <yt/ytlib/table_client/serialize.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

using NTableClient::TSaveContext;
using NTableClient::TLoadContext;
using NTableClient::TPersistenceContext;
using IPersistent = NPhoenix::ICustomPersistent<TPersistenceContext>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TJobManager)

DECLARE_REFCOUNTED_STRUCT(TChunkStripe)

struct IChunkPoolInput;
struct IChunkPoolOutput;
struct IChunkPool;
struct IShuffleChunkPool;

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

