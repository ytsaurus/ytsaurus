#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/small_vector.h>

// Forward declarations.
namespace NYT {

class TBlobOutput;
class TFakeStringBufStore;

} // namespace NYT

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

struct TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

class TTableChunkWriter;
typedef TIntrusivePtr<TTableChunkWriter> TTableChunkWriterPtr;

class TPartitionChunkWriter;
typedef TIntrusivePtr<TPartitionChunkWriter> TPartitionChunkWriterPtr;

class TChunkReader;
typedef TIntrusivePtr<TChunkReader> TChunkReaderPtr;

class TTableChunkSequenceWriter;
typedef TIntrusivePtr<TTableChunkSequenceWriter> TTableChunkSequenceWriterPtr;

class TPartitionChunkSequenceWriter;
typedef TIntrusivePtr<TPartitionChunkSequenceWriter> TPartitionChunkSequenceWriterPtr;

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

struct TTableConsumerConfig;
typedef TIntrusivePtr<TTableConsumerConfig> TTableConsumerConfigPtr;

typedef TSmallVector< std::pair<TStringBuf, TStringBuf>, 32 > TRow;
typedef std::vector<Stroka> TKeyColumns;

template <class TBuffer>
class TKey;

template <class TStrType>
class TKeyPart;

typedef TKey<TBlobOutput> TOwningKey;
typedef TKey<TFakeStringBufStore> TNonOwningKey;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
