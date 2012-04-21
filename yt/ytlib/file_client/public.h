#pragma once 

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

class TFileWriterBase;
typedef TIntrusivePtr<TFileWriterBase> TFileWriterBasePtr;

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
