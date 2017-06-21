#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>
#include <yt/ytlib/chunk_client/chunk_meta.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TRefCountedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
