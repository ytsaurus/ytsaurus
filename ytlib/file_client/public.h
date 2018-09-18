#pragma once

#include <yt/client/file_client/public.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

constexpr int FormatVersion = 1;

class TFileChunkOutput;

DECLARE_REFCOUNTED_STRUCT(IFileReader)

DECLARE_REFCOUNTED_STRUCT(IFileChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IFileMultiChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
