#include "stdafx.h"
#include "private.h"
#include "config.h"
#include "partition_job.h"

#include <ytlib/misc/sync.h>
#include <ytlib/object_server/id.h>
#include <ytlib/election/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/partition_chunk_sequence_writer.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/ytree/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NYTree;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

TPartitionJob::TPartitionJob(
    TJobIOConfigPtr ioConfig,
    TLeaderLookup::TConfigPtr masterConfig,
    const TPartitionJobSpec& jobSpec)
{
    auto masterChannel = CreateLeaderChannel(masterConfig);
    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());

    std::vector<NTableClient::NProto::TInputChunk> chunks(
        jobSpec.input_spec().chunks().begin(),
        jobSpec.input_spec().chunks().end());

    Reader = New<TChunkSequenceReader>(
        ioConfig->ChunkSequenceReader, 
        masterChannel, 
        blockCache, 
        chunks);


    std::vector<NTableClient::NProto::TKey> partitionKeys(
        jobSpec.partition_keys().begin(), 
        jobSpec.partition_keys().end());

    Writer = New<TPartitionChunkSequenceWriter>(
        ioConfig->ChunkSequenceWriter,
        masterChannel,
        TTransactionId::FromProto(jobSpec.output_transaction_id()),
        TChunkListId::FromProto(jobSpec.output_spec().chunk_list_id()),
        ChannelsFromYson(jobSpec.output_spec().channels()),
        FromProto<Stroka>(jobSpec.key_columns()),
        MoveRV(partitionKeys));
}

TJobResult TPartitionJob::Run()
{
    PROFILE_TIMING ("/partition_time") {
        LOG_INFO("Initializing");

        Sync(~Reader, &TChunkSequenceReader::AsyncOpen);

        Sync(~Writer, &TPartitionChunkSequenceWriter::AsyncOpen);

        LOG_INFO("Partitioning.");
//        Sleep(TDuration::Seconds(60));

        while (Reader->IsValid()) {
            Sync(~Writer, &TPartitionChunkSequenceWriter::AsyncWriteRow, Reader->GetRow());
            Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
        }

        Sync(~Writer, &TPartitionChunkSequenceWriter::AsyncClose);

        LOG_INFO("Finalizing");
        {
            TPartitionJobResult partitionResult;
            ToProto(partitionResult.mutable_chunks(), Writer->GetWrittenChunks());

            TJobResult result;
            *result.mutable_error() = TError().ToProto();
            *result.MutableExtension(TPartitionJobResult::partition_job_result) = partitionResult;

            return result;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
