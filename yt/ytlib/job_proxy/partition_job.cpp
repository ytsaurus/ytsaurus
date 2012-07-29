#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "partition_job.h"

#include <ytlib/misc/sync.h>
#include <ytlib/object_server/id.h>
#include <ytlib/meta_state/leader_channel.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/partition_chunk_sequence_writer.h>
#include <ytlib/table_client/table_chunk_sequence_reader.h>
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

class TPartitionJob
    : public TJob
{
public:
    explicit TPartitionJob(IJobHost* host)
        : TJob(host)
    {
        const auto& jobSpec = Host->GetJobSpec();
        auto config = Host->GetConfig();

        YCHECK(jobSpec.input_specs_size() == 1);
        YCHECK(jobSpec.output_specs_size() == 1);

        auto masterChannel = CreateLeaderChannel(config->Masters);
        auto blockCache = CreateClientBlockCache(New<TClientBlockCacheConfig>());
        auto jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);

        std::vector<NTableClient::NProto::TInputChunk> chunks(
            jobSpec.input_specs(0).chunks().begin(),
            jobSpec.input_specs(0).chunks().end());

        Reader = New<TTableChunkSequenceReader>(
            config->JobIO->ChunkSequenceReader, 
            masterChannel, 
            blockCache, 
            MoveRV(chunks));

        std::vector<NTableClient::NProto::TKey> partitionKeys(
            jobSpecExt.partition_keys().begin(), 
            jobSpecExt.partition_keys().end());

        Writer = New<TPartitionChunkSequenceWriter>(
            config->JobIO->ChunkSequenceWriter,
            masterChannel,
            TTransactionId::FromProto(jobSpec.output_transaction_id()),
            TChunkListId::FromProto(jobSpec.output_specs(0).chunk_list_id()),
            ChannelsFromYson(TYsonString(jobSpec.output_specs(0).channels())),
            FromProto<Stroka>(jobSpecExt.key_columns()),
            MoveRV(partitionKeys));
    }

    virtual NScheduler::NProto::TJobResult Run() OVERRIDE
    {
        PROFILE_TIMING ("/partition_time") {
            LOG_INFO("Initializing");
            {
                Sync(~Reader, &TTableChunkSequenceReader::AsyncOpen);
                Sync(~Writer, &TPartitionChunkSequenceWriter::AsyncOpen);
            }
            PROFILE_TIMING_CHECKPOINT("init");

            LOG_INFO("Partitioning");
            {
                while (Reader->IsValid()) {
                    while (!Writer->TryWriteRow(Reader->GetRow())) {
                        Sync(~Writer, &TPartitionChunkSequenceWriter::GetReadyEvent);
                    }
                    if (!Reader->FetchNextItem()) {
                        Sync(~Reader, &TTableChunkSequenceReader::GetReadyEvent);
                    }
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

private:
    TTableChunkSequenceReaderPtr Reader;
    TPartitionChunkSequenceWriterPtr Writer;

};

TAutoPtr<IJob> CreatePartitionJob(IJobHost* host)
{
    return new TPartitionJob(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
