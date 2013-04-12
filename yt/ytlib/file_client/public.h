#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkOutput;

class TFileReaderBase;

class TFileWriter;
typedef TIntrusivePtr<TFileWriter> TFileWriterPtr;

class TFileReader;
typedef TIntrusivePtr<TFileReader> TFileReaderPtr;

class TFileWriterConfig;
typedef TIntrusivePtr<TFileWriterConfig> TFileWriterConfigPtr;

class TFileReaderConfig;
typedef TIntrusivePtr<TFileReaderConfig> TFileReaderConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
