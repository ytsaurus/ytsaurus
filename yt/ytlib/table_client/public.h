#pragma once

#include <core/misc/public.h>
#include <core/misc/small_vector.h>

#include <ytlib/table_client/public.h>

#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((MasterCommunicationFailed)  (300))
    ((SortOrderViolation)         (301))
);

DEFINE_ENUM(EControlAttribute,
    (TableIndex)
);

////////////////////////////////////////////////////////////////////////////////

const int DefaultPartitionTag = -1;
const i64 MaxRowWeightLimit = (i64) 128 * 1024 * 1024;
const size_t MaxColumnNameSize = 256;
const int MaxColumnCount = 1024;
const size_t MaxKeySize = (i64) 4 * 1024;
const int FormatVersion = 1;

////////////////////////////////////////////////////////////////////////////////

struct IWriterBase;
typedef TIntrusivePtr<IWriterBase> IWriterBasePtr;

struct IAsyncWriter;
typedef TIntrusivePtr<IAsyncWriter> IAsyncWriterPtr;

struct ISyncReader;
typedef TIntrusivePtr<ISyncReader> ISyncReaderPtr;

struct IAsyncReader;
typedef TIntrusivePtr<IAsyncReader> IReaderPtr;

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

class TTableChunkReaderProvider;
typedef TIntrusivePtr<TTableChunkReaderProvider> TTableChunkReaderProviderPtr;

class TPartitionChunkReader;
typedef TIntrusivePtr<TPartitionChunkReader> TPartitionChunkReaderPtr;

class TPartitionChunkReaderProvider;
typedef TIntrusivePtr<TPartitionChunkReaderProvider> TPartitionChunkReaderProviderPtr;

class TChannelWriter;
typedef TIntrusivePtr<TChannelWriter> TChannelWriterPtr;

class TChannelReader;
typedef TIntrusivePtr<TChannelReader> TChannelReaderPtr;

class TChunkWriterConfig;
typedef TIntrusivePtr<TChunkWriterConfig> TChunkWriterConfigPtr;

class TChunkWriterOptions;
typedef TIntrusivePtr<TChunkWriterOptions> TChunkWriterOptionsPtr;

class TTableWriterConfig;
typedef TIntrusivePtr<TTableWriterConfig> TTableWriterConfigPtr;

class TBufferedTableWriterConfig;
typedef TIntrusivePtr<TBufferedTableWriterConfig> TBufferedTableWriterConfigPtr;

class TTableWriterOptions;
typedef TIntrusivePtr<TTableWriterOptions> TTableWriterOptionsPtr;

class TChunkReaderOptions;
typedef TIntrusivePtr<TChunkReaderOptions> TChunkReaderOptionsPtr;

class TTableReaderConfig;
typedef TIntrusivePtr<TTableReaderConfig> TTableReaderConfigPtr;

class TAsyncTableReader;
typedef TIntrusivePtr<TAsyncTableReader> TAsyncTableReaderPtr;

class TAsyncWriter;
typedef TIntrusivePtr<TAsyncWriter> TAsyncWriterPtr;

class TTableProducer;
class TLegacyTableConsumer;

class TWritingTableConsumer;

typedef SmallVector< std::pair<TStringBuf, TStringBuf>, 32 > TRow;
typedef std::vector<Stroka> TKeyColumns;

struct IPartitioner;

typedef NChunkClient::TOldMultiChunkParallelReader<TTableChunkReader> TTableChunkParallelReader;
typedef TIntrusivePtr<TTableChunkParallelReader> TTableChunkParallelReaderPtr;

typedef NChunkClient::TOldMultiChunkSequentialReader<TTableChunkReader> TTableChunkSequenceReader;
typedef TIntrusivePtr<TTableChunkSequenceReader> TTableChunkSequenceReaderPtr;

typedef NChunkClient::TOldMultiChunkSequentialWriter<TTableChunkWriterProvider> TTableChunkSequenceWriter;
typedef TIntrusivePtr<TTableChunkSequenceWriter> TTableChunkSequenceWriterPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
