#pragma once

#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter;
typedef TIntrusivePtr<IAsyncWriter> IAsyncWriterPtr;

struct ISyncWriter;
typedef TIntrusivePtr<ISyncWriter> ISyncWriterPtr;

struct IAsyncReader;
typedef TIntrusivePtr<IAsyncReader> IAsyncReaderPtr;

class TKey;

struct TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

class TChunkWriter;
typedef TIntrusivePtr<TChunkWriter> TChunkWriterPtr;

class TChunkReader;
typedef TIntrusivePtr<TChunkReader> TChunkReaderPtr;

struct TChunkSequenceWriter;
typedef TIntrusivePtr<TChunkSequenceWriter> TChunkSequenceWriterPtr;

class TChannelWriter;
typedef TIntrusivePtr<TChannelWriter> TChannelWriterPtr;

class TChannelReader;
typedef TIntrusivePtr<TChannelReader> TChannelReaderPtr;

struct TChunkSequenceWriterConfig;
typedef TIntrusivePtr<TChunkSequenceWriterConfig> TChunkSequenceWriterConfigPtr;

struct TChunkSequenceReaderConfig;
typedef TIntrusivePtr<TChunkSequenceReaderConfig> TChunkSequenceReaderConfigPtr;

typedef std::vector< std::pair<TStringBuf, TStringBuf> > TRow;
typedef std::vector<Stroka> TKeyColumns;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
