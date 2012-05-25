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
    TJobProxyConfigPtr proxyConfig,
    const TJobSpec& jobSpec)
{
    YCHECK(jobSpec.input_specs_size() == 1);
    YCHECK(jobSpec.output_specs_size() == 1);

    auto masterChannel = CreateLeaderChannel(proxyConfig->Masters);
    auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());
    auto jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);

    std::vector<NTableClient::NProto::TInputChunk> chunks(
        jobSpec.input_specs(0).chunks().begin(),
        jobSpec.input_specs(0).chunks().end());

    Reader = New<TChunkSequenceReader>(
        proxyConfig->JobIO->ChunkSequenceReader, 
        masterChannel, 
        blockCache, 
        chunks);

    std::vector<NTableClient::NProto::TKey> partitionKeys(
        jobSpecExt.partition_keys().begin(), 
        jobSpecExt.partition_keys().end());

    Writer = New<TPartitionChunkSequenceWriter>(
        proxyConfig->JobIO->ChunkSequenceWriter,
        masterChannel,
        TTransactionId::FromProto(jobSpec.output_transaction_id()),
        TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
        ChannelsFromYson(jobSpec.output_specs(0).channels()),
        FromProto<Stroka>(jobSpecExt.key_columns()),
        MoveRV(partitionKeys));
}

TJobResult TPartitionJob::Run()
{
    PROFILE_TIMING ("/partition_time") {
        LOG_INFO("Initializing");
        {
            Sync(~Reader, &TChunkSequenceReader::AsyncOpen);
            Sync(~Writer, &TPartitionChunkSequenceWriter::AsyncOpen);
        }
        PROFILE_TIMING_CHECKPOINT("init");

        LOG_INFO("Partitioning");
        {
            while (Reader->IsValid()) {
                Sync(~Writer, &TPartitionChunkSequenceWriter::AsyncWriteRow, Reader->GetRow());
                Sync(~Reader, &TChunkSequenceReader::AsyncNextRow);
            }

            Sync(~Writer, &TPartitionChunkSequenceWriter::AsyncClose);
        }
        PROFILE_TIMING_CHECKPOINT("partition");

        LOG_INFO("Finalizing");
        {
            TJobResult result;
            *result.mutable_error() = TError().ToProto();
            auto* resultExt = result.MutableExtension(TPartitionJobResultExt::partition_job_result_ext);
            ToProto(resultExt->mutable_chunks(), Writer->GetWrittenChunks());
            return result;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
