#pragma once

#include <yt/yt/client/file_client/public.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkOutput;

DECLARE_REFCOUNTED_STRUCT(IFileReader)

DECLARE_REFCOUNTED_STRUCT(IFileChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IFileMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
