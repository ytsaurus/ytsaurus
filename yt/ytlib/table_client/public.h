#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/small_vector.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EErrorCode,
    ((MasterCommunicationFailed)  (300))
    ((SortOrderViolation)         (301))
);

extern const int DefaultPartitionTag;

////////////////////////////////////////////////////////////////////////////////

struct IAsyncWriter;
typedef TIntrusivePtr<IAsyncWriter> IAsyncWriterPtr;

struct ISyncWriter;
typedef TIntrusivePtr<ISyncWriter> ISyncWriterPtr;

struct ISyncWriterUnsafe;
typedef TIntrusivePtr<ISyncWriterUnsafe> ISyncWriterUnsafePtr;

struct ISyncReader;
typedef TIntrusivePtr<ISyncReader> ISyncReaderPtr;

struct IAsyncReader;
typedef TIntrusivePtr<IAsyncReader> IAsyncReaderPtr;

class TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

class TTableChunkWriter;
typedef TIntrusivePtr<TTableChunkWriter> TTableChunkWriterPtr;

class TTableChunkWriterFacade;

class TTableChunkWriterProvider;
typedef TIntrusivePtr<TTableChunkWriterProvider> TTableChunkWriterProviderPtr;

class TPartitionChunkWriter;
typedef TIntrusivePtr<TPartitionChunkWriter> TPartitionChunkWriterPtr;

class TPartitionChunkWriterFacade;

class TPartitionChunkWriterProvider;
typedef TIntrusivePtr<TPartitionChunkWriterProvider> TPartitionChunkWriterProviderPtr;

class TTableChunkReader;
typedef TIntrusivePtr<TTableChunkReader> TTableChunkReaderPtr;

template <class TChunkReader>
class TMultiChunkSequentialReader;

typedef TMultiChunkSequentialReader<TTableChunkReader> TTableChunkSequenceReader;
typedef TIntrusivePtr<TTableChunkSequenceReader> TTableChunkSequenceReaderPtr;

class TTableChunkSequenceWriter;
typedef TIntrusivePtr<TTableChunkSequenceWriter> TTableChunkSequenceWriterPtr;

class TPartitionChunkReader;
typedef TIntrusivePtr<TPartitionChunkReader> TPartitionChunkReaderPtr;

class TPartitionChunkSequenceWriter;
typedef TIntrusivePtr<TPartitionChunkSequenceWriter> TPartitionChunkSequenceWriterPtr;

class TChannelWriter;
typedef TIntrusivePtr<TChannelWriter> TChannelWriterPtr;

class TChannelReader;
typedef TIntrusivePtr<TChannelReader> TChannelReaderPtr;

class TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

struct TChunkWriterOptions;
typedef TIntrusivePtr<TChunkWriterOptions> TChunkWriterOptionsPtr;

class TTableWriterConfig;
typedef TIntrusivePtr<TTableWriterConfig> TTableWriterConfigPtr;

struct TTableWriterOptions;
typedef TIntrusivePtr<TTableWriterOptions> TTableWriterOptionsPtr;

class TTableReaderConfig;
typedef TIntrusivePtr<TTableReaderConfig> TTableReaderConfigPtr;

struct TChunkReaderOptions;
typedef TIntrusivePtr<TChunkReaderOptions> TChunkReaderOptionsPtr;

class TTableProducer;
class TTableConsumer;

class TTableConsumerConfig;
typedef TIntrusivePtr<TTableConsumerConfig> TTableConsumerConfigPtr;

typedef TSmallVector< std::pair<TStringBuf, TStringBuf>, 32 > TRow;
typedef std::vector<Stroka> TKeyColumns;

struct IPartitioner;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
