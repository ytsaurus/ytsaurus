#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "partition_job.h"

#include <server/chunk_server/public.h>

#include <ytlib/misc/sync.h>

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <ytlib/table_client/partition_chunk_writer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/multi_chunk_parallel_reader.h>
#include <ytlib/table_client/partitioner.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/yson/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NYTree;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

typedef TMultiChunkParallelReader<TTableChunkReader> TReader;
typedef TMultiChunkSequentialWriter<TPartitionChunkWriter> TWriter;

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

        auto jobSpecExt = jobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext);

        std::vector<NChunkClient::NProto::TInputChunk> chunks(
            jobSpec.input_specs(0).chunks().begin(),
            jobSpec.input_specs(0).chunks().end());

        auto readerProvider = New<TTableChunkReaderProvider>(config->JobIO->TableReader);
        Reader = New<TReader>(
            config->JobIO->TableReader,
            Host->GetMasterChannel(),
            Host->GetBlockCache(),
            std::move(chunks),
            readerProvider);

        if (jobSpecExt.partition_keys_size() > 0) {
            YCHECK(jobSpecExt.partition_keys_size() + 1 == jobSpecExt.partition_count());
            FOREACH (const auto& key, jobSpecExt.partition_keys()) {
                PartitionKeys.push_back(TOwningKey::FromProto(key));
            }
            Partitioner = CreateOrderedPartitioner(&PartitionKeys);
        } else {
            Partitioner = CreateHashPartitioner(jobSpecExt.partition_count());
        }

        auto transactionId = TTransactionId::FromProto(jobSpec.output_transaction_id());
        const auto& outputSpec = jobSpec.output_specs(0);

        auto chunkListId = TChunkListId::FromProto(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->KeyColumns = FromProto<Stroka>(jobSpecExt.key_columns());

        auto writerProvider = New<TPartitionChunkWriterProvider>(
            config->JobIO->TableWriter,
            options,
            ~Partitioner);

        Writer = CreateSyncWriter<TPartitionChunkWriter>(New<TWriter>(
            config->JobIO->TableWriter,
            options,
            writerProvider,
            Host->GetMasterChannel(),
            transactionId,
            chunkListId));
    }

    virtual NScheduler::NProto::TJobResult Run() override
    {
        PROFILE_TIMING ("/partition_time") {
            LOG_INFO("Initializing");
            {
                Sync(~Reader, &TReader::AsyncOpen);
                Writer->Open();
            }
            PROFILE_TIMING_CHECKPOINT("init");

            LOG_INFO("Partitioning");
            {
                while (Reader->IsValid()) {
                    Writer->WriteRowUnsafe(Reader->CurrentReader()->GetRow());

                    if (!Reader->FetchNextItem()) {
                        Sync(~Reader, &TReader::GetReadyEvent);
                    }
                }

                Writer->Close();
            }
            PROFILE_TIMING_CHECKPOINT("partition");

            LOG_INFO("Finalizing");
            {
                TJobResult result;
                ToProto(result.mutable_error(), TError());
                auto* resultExt = result.MutableExtension(TPartitionJobResultExt::partition_job_result_ext);
                ToProto(resultExt->mutable_chunks(), Writer->GetWrittenChunks());
                return result;
            }
        }
    }

    double GetProgress() const override
    {
        i64 total = Reader->GetItemCount();
        if (total == 0) {
            LOG_WARNING("GetProgress: empty total");
            return 0.0;
        } else {
            double progress = (double) Reader->GetItemIndex() / total;
            LOG_DEBUG("GetProgress: %lf", progress);
            return progress;
        }
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunks() const override
    {
        return Reader->GetFailedChunks();
    }

private:
    TIntrusivePtr<TReader> Reader;
    ISyncWriterUnsafePtr Writer;
    std::vector<TOwningKey> PartitionKeys;
    TAutoPtr<IPartitioner> Partitioner;

};

TJobPtr CreatePartitionJob(IJobHost* host)
{
    return New<TPartitionJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
