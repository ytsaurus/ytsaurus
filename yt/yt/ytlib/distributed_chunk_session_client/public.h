#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NDistributedChunkSessionClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDistributedChunkSessionController)
DECLARE_REFCOUNTED_STRUCT(IDistributedChunkWriter)

DECLARE_REFCOUNTED_STRUCT(TDistributedChunkSessionControllerConfig);
DECLARE_REFCOUNTED_STRUCT(TDistributedChunkWriterConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
