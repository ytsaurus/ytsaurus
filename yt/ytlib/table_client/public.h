#pragma once

#include <ytlib/misc/intrusive_ptr.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter;
typedef TIntrusivePtr<IAsyncWriter> IAsyncWriterPtr;

struct ISyncWriter;
typedef TIntrusivePtr<ISyncWriter> ISyncWriterPtr;

class TColumnMap;
typedef TIntrusivePtr<TColumnMap> TColumnMapPtr;

class TKey;

struct TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

class TChunkWriter;
typedef TIntrusivePtr<TChunkWriter> TChunkWriterPtr;

struct TChunkSequenceWriter;
typedef TIntrusivePtr<TChunkSequenceWriter> TChunkSequenceWriterPtr;

class TChannelWriter;
typedef TIntrusivePtr<TChannelWriter> TChannelWriterPtr;

struct TChunkSequenceWriterConfig;
typedef TIntrusivePtr<TChunkSequenceWriterConfig> TChunkSequenceWriterConfigPtr;

struct TChunkSequenceReaderConfig;
typedef TIntrusivePtr<TChunkSequenceReaderConfig> TChunkSequenceReaderConfigPtr;

typedef std::vector< std::pair<TStringBuf, TStringBuf> > TRow;
typedef std::vector<Stroka> TKeyColumns;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
