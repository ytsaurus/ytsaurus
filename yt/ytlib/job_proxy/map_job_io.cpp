#include "stdafx.h"

#include "map_job_io.h"
#include "table_output.h"
#include "config.h"
#include "stderr_output.h"

// ToDo(psushin): use public.h everywhere.
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/table_producer.h>
#include <ytlib/table_client/table_consumer.h>
#include <ytlib/table_client/schema.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////

TMapJobIO::TMapJobIO(
    TJobIOConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    const NScheduler::NProto::TMapJobSpec& ioSpec)
    : Config(config)
    , MasterChannel(masterChannel)
    , IoSpec(ioSpec)
{ }

int TMapJobIO::GetInputCount() const 
{
    // Always single input for map.
    return 1;
}

int TMapJobIO::GetOutputCount() const
{
    return IoSpec.output_specs_size();
}

TAutoPtr<NTableClient::TTableProducer> 
TMapJobIO::CreateTableInput(int index, NYTree::IYsonConsumer* consumer) const
{
    YASSERT(index < GetInputCount());

    auto blockCache = CreateClientBlockCache(~New<TClientBlockCacheConfig>());

    std::vector<NTableClient::NProto::TInputChunk> chunks(
        IoSpec.input_spec().chunks().begin(),
        IoSpec.input_spec().chunks().end());

    LOG_DEBUG("Creating %d input from %d chunks", 
        index, 
        static_cast<int>(chunks.size()));

    auto reader = New<TChunkSequenceReader>(
        Config->ChunkSequenceReader,
        MasterChannel,
        blockCache,
        chunks);
    auto syncReader = New<TSyncReaderAdapter>(reader);
    syncReader->Open();

    // ToDo(psushin): extract format from operation spec.
    return new TTableProducer(syncReader, consumer);
}

NTableClient::ISyncWriterPtr TMapJobIO::CreateTableOutput(int index) const
{
    YASSERT(index < GetOutputCount());
    const TYson& channels = IoSpec.output_specs(index).channels();
    YASSERT(!channels.empty());

    auto chunkSequenceWriter = New<TTableChunkSequenceWriter>(
        Config->ChunkSequenceWriter,
        MasterChannel,
        TTransactionId::FromProto(IoSpec.output_transaction_id()),
        TChunkListId::FromProto(IoSpec.output_specs(index).chunk_list_id()),
        ChannelsFromYson(channels));

    auto syncWriter = CreateSyncWriter(chunkSequenceWriter);
    syncWriter->Open();

    return syncWriter;
}

void TMapJobIO::UpdateProgress()
{
    YUNIMPLEMENTED();
}

double TMapJobIO::GetProgress() const
{
    YUNIMPLEMENTED();
}

TAutoPtr<TErrorOutput> TMapJobIO::CreateErrorOutput() const
{
    return new TErrorOutput(
        Config->ErrorFileWriter, 
        MasterChannel, 
        TTransactionId::FromProto(IoSpec.output_transaction_id()));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
