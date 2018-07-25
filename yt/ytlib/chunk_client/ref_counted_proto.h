#pragma once

#include "public.h"

#include <yt/client/chunk_client/proto/chunk_spec.pb.h>
#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TRefCountedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
