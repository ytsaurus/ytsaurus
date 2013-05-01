#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NFileClient {

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

class TFileWriter;
typedef TIntrusivePtr<TFileWriter> TFileWriterPtr;

class TFileReader;
typedef TIntrusivePtr<TFileReader> TFileReaderPtr;

struct TFileChunkWriterConfig;
typedef TIntrusivePtr<TFileChunkWriterConfig> TFileChunkWriterConfigPtr;

struct TFileWriterConfig;
typedef TIntrusivePtr<TFileWriterConfig> TFileWriterConfigPtr;

class TFileReaderConfig;
typedef TIntrusivePtr<TFileReaderConfig> TFileReaderConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
