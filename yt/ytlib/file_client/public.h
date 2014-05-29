#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

const int FormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

class TFileChunkOutput;

class TFileChunkReader;
typedef TIntrusivePtr<TFileChunkReader> TFileChunkReaderPtr;

class TFileChunkReaderProvider;
typedef TIntrusivePtr<TFileChunkReaderProvider> TFileChunkReaderProviderPtr;

class TFileChunkWriter;
typedef TIntrusivePtr<TFileChunkWriter> TFileChunkWriterPtr;

class TFileChunkWriterProvider;
typedef TIntrusivePtr<TFileChunkWriterProvider> TFileChunkWriterProviderPtr;

class TFileChunkWriterConfig;
typedef TIntrusivePtr<TFileChunkWriterConfig> TFileChunkWriterConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
