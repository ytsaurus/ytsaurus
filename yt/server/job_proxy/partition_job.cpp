#include "stdafx.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"
#include "partition_job.h"

#include <core/misc/sync.h>

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/old_multi_chunk_parallel_reader.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <ytlib/table_client/partition_chunk_writer.h>
#include <ytlib/table_client/channel_writer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/partitioner.h>
#include <ytlib/table_client/sync_writer.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <core/yson/lexer.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;

using NVersionedTableClient::TOwningKey;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static auto& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

typedef TOldMultiChunkParallelReader<TTableChunkReader> TReader;
typedef TOldMultiChunkSequentialWriter<TPartitionChunkWriterProvider> TWriter;

class TPartitionJob
    : public TJob
{
public:
    explicit TPartitionJob(IJobHost* host)
        : TJob(host)
        , JobSpec(host->GetJobSpec())
        , SchedulerJobSpecExt(JobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
        , PartitionJobSpecExt(JobSpec.GetExtension(TPartitionJobSpecExt::partition_job_spec_ext))
    {
        auto config = host->GetConfig();

        YCHECK(SchedulerJobSpecExt.input_specs_size() == 1);
        const auto& inputSpec = SchedulerJobSpecExt.input_specs(0);

        YCHECK(SchedulerJobSpecExt.output_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt.output_specs(0);

        std::vector<TChunkSpec> chunks(inputSpec.chunks().begin(), inputSpec.chunks().end());

        auto readerProvider = New<TTableChunkReaderProvider>(
            chunks,
            config->JobIO->TableReader);

        Reader = New<TReader>(
            config->JobIO->TableReader,
            host->GetMasterChannel(),
            host->GetBlockCache(),
            host->GetNodeDirectory(),
            std::move(chunks),
            readerProvider);

        if (PartitionJobSpecExt.partition_keys_size() > 0) {
            YCHECK(PartitionJobSpecExt.partition_keys_size() + 1 == PartitionJobSpecExt.partition_count());
            for (const auto& protoKey : PartitionJobSpecExt.partition_keys()) {
                TOwningKey key;
                FromProto(&key, protoKey);
                PartitionKeys.push_back(key);
            }
            Partitioner = CreateOrderedPartitioner(&PartitionKeys);
        } else {
            Partitioner = CreateHashPartitioner(PartitionJobSpecExt.partition_count());
        }

        i64 inputDataSize = 0;
        for (const auto& chunkSpec : chunks) {
            i64 dataSize = 0;
            NChunkClient::GetStatistics(chunkSpec, &dataSize);
            inputDataSize += dataSize;
        }

        if (inputDataSize < config->JobIO->TableWriter->MaxBufferSize) {
            config->JobIO->TableWriter->MaxBufferSize = std::max(
                inputDataSize,
                (i64) 2 * TChannelWriter::MinUpperReserveLimit * Partitioner->GetPartitionCount());
        }

        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->KeyColumns = FromProto<Stroka>(PartitionJobSpecExt.key_columns());

        auto writerProvider = New<TPartitionChunkWriterProvider>(
            config->JobIO->TableWriter,
            options,
            Partitioner.get());

        Writer = CreateSyncWriter<TPartitionChunkWriterProvider>(New<TWriter>(
            config->JobIO->TableWriter,
            options,
            writerProvider,
            host->GetMasterChannel(),
            transactionId,
            chunkListId));
    }

    virtual TJobResult Run() override
    {
        PROFILE_TIMING ("/partition_time") {
            LOG_INFO("Initializing");
            {
                Sync(Reader.Get(), &TReader::AsyncOpen);
            }
            PROFILE_TIMING_CHECKPOINT("init");

            LOG_INFO("Partitioning");
            {
                const TReader::TFacade* facade;
                while ((facade = Reader->GetFacade()) != nullptr) {
                    Writer->WriteRowUnsafe(facade->GetRow());

                    if (!Reader->FetchNext()) {
                        Sync(Reader.Get(), &TReader::GetReadyEvent);
                    }
                }

                Writer->Close();
            }
            PROFILE_TIMING_CHECKPOINT("partition");

            LOG_INFO("Finalizing");
            {
                TJobResult result;
                ToProto(result.mutable_error(), TError());

                auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
                Writer->GetNodeDirectory()->DumpTo(schedulerResultExt->mutable_node_directory());
                ToProto(schedulerResultExt->mutable_chunks(), Writer->GetWrittenChunks());

                return result;
            }
        }
    }

    virtual double GetProgress() const override
    {
        i64 total = Reader->GetProvider()->GetRowCount();
        if (total == 0) {
            LOG_WARNING("GetProgress: empty total");
            return 0.0;
        } else {
            double progress = (double) Reader->GetProvider()->GetRowIndex() / total;
            LOG_DEBUG("GetProgress: %lf", progress);
            return progress;
        }
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return Reader->GetFailedChunkIds();
    }

    virtual TJobStatistics GetStatistics() const override
    {
        TJobStatistics result;
        result.set_time(GetElapsedTime().MilliSeconds());
        ToProto(result.mutable_input(), Reader->GetProvider()->GetDataStatistics());
        ToProto(result.mutable_output(), Writer->GetDataStatistics());
        return result;
    }

private:
    const TJobSpec& JobSpec;
    const TSchedulerJobSpecExt& SchedulerJobSpecExt;
    const TPartitionJobSpecExt& PartitionJobSpecExt;

    TIntrusivePtr<TReader> Reader;
    ISyncWriterUnsafePtr Writer;
    std::vector<TOwningKey> PartitionKeys;
    std::unique_ptr<IPartitioner> Partitioner;

};

TJobPtr CreatePartitionJob(IJobHost* host)
{
    return New<TPartitionJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
