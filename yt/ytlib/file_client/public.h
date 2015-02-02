#pragma once

#include <core/misc/public.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

const int FormatVersion = 1;

class TFileChunkOutput;

DECLARE_REFCOUNTED_CLASS(TFileChunkReader)
DECLARE_REFCOUNTED_CLASS(TFileChunkReaderProvider)
DECLARE_REFCOUNTED_CLASS(TFileChunkWriter)
DECLARE_REFCOUNTED_CLASS(TFileChunkWriterProvider)

DECLARE_REFCOUNTED_CLASS(TFileChunkWriterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
