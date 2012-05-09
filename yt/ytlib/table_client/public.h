#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter;
typedef TIntrusivePtr<IAsyncWriter> IAsyncWriterPtr;

struct ISyncWriter;
typedef TIntrusivePtr<ISyncWriter> ISyncWriterPtr;

struct ISyncReader;
typedef TIntrusivePtr<ISyncReader> ISyncReaderPtr;

struct IAsyncReader;
typedef TIntrusivePtr<IAsyncReader> IAsyncReaderPtr;

class TKey;

struct TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

class TChunkWriter;
typedef TIntrusivePtr<TChunkWriter> TChunkWriterPtr;

class TChunkReader;
typedef TIntrusivePtr<TChunkReader> TChunkReaderPtr;

class TChunkSequenceWriter;
typedef TIntrusivePtr<TChunkSequenceWriter> TChunkSequenceWriterPtr;

class TChunkSequenceReader;
typedef TIntrusivePtr<TChunkSequenceReader> TChunkSequenceReaderPtr;

class TChannelWriter;
typedef TIntrusivePtr<TChannelWriter> TChannelWriterPtr;

class TChannelReader;
typedef TIntrusivePtr<TChannelReader> TChannelReaderPtr;

struct TChunkSequenceWriterConfig;
typedef TIntrusivePtr<TChunkSequenceWriterConfig> TChunkSequenceWriterConfigPtr;

struct TChunkSequenceReaderConfig;
typedef TIntrusivePtr<TChunkSequenceReaderConfig> TChunkSequenceReaderConfigPtr;

class TTableProducer;
class TTableConsumer;

typedef std::vector< std::pair<TStringBuf, TStringBuf> > TRow;
typedef std::vector<Stroka> TKeyColumns;

class TKey;
class TKeyPart;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
