#pragma once 

#include <ytlib/misc/common.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileChunkOutput;

class TFileReaderBase;
typedef TIntrusivePtr<TFileReaderBase> TFileReaderBasePtr;

class TFileWriter;
typedef TIntrusivePtr<TFileWriter> TFileWriterPtr;

class TFileReader;
typedef TIntrusivePtr<TFileReader> TFileReaderPtr;

struct TFileWriterConfig;
typedef TIntrusivePtr<TFileWriterConfig> TFileWriterConfigPtr;

struct TFileReaderConfig;
typedef TIntrusivePtr<TFileReaderConfig> TFileReaderConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
