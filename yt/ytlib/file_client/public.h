#pragma once

#include <core/misc/public.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

const int FormatVersion = 1;

class TFileChunkOutput;

DECLARE_REFCOUNTED_STRUCT(IFileChunkReader)
DECLARE_REFCOUNTED_STRUCT(IFileMultiChunkReader)

DECLARE_REFCOUNTED_STRUCT(IFileChunkWriter)
DECLARE_REFCOUNTED_STRUCT(IFileMultiChunkWriter)

DECLARE_REFCOUNTED_CLASS(TFileChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
