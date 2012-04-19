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

struct TFileWriterBaseConfig;
typedef TIntrusivePtr<TFileWriterBaseConfig> TFileWriterBaseConfigPtr;

struct TFileReaderBaseConfig;
typedef TIntrusivePtr<TFileReaderBaseConfig> TFileReaderBaseConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
