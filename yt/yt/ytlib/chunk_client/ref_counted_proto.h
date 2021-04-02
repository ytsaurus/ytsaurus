#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>
#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TRefCountedChunkMeta)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
