#include "stdafx.h"
#include "sorted_merge_job.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/chunk_client/async_reader.h>
#include <ytlib/chunk_client/remote_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/private.h>
#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>
#include <ytlib/table_client/merging_reader.h>

#include <ytlib/ytree/yson_string.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = JobProxyLogger;
static NProfiling::TProfiler& Profiler = JobProxyProfiler;

typedef TMultiChunkSequentialWriter<TTableChunkWriter> TWriter;

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeJob
    : public TJob
{
public:
    explicit TSortedMergeJob(IJobHost* host)
        : TJob(host)
    {
        const auto& jobSpec = Host->GetJobSpec();
        auto config = Host->GetConfig();

        YCHECK(jobSpec.output_specs_size() == 1);

        {
            std::vector<TTableChunkSequenceReaderPtr> readers;
             auto options = New<TChunkReaderOptions>();
            options->ReadKey = true;

            FOREACH (const auto& inputSpec, jobSpec.input_specs()) {
                // ToDo(psushin): validate that input chunks are sorted.
                std::vector<NChunkClient::NProto::TInputChunk> chunks(
                    inputSpec.chunks().begin(),
                    inputSpec.chunks().end());

                auto provider = New<TTableChunkReaderProvider>(
                    chunks,
                    config->JobIO->TableReader,
                    options);

                auto reader = New<TTableChunkSequenceReader>(
                    config->JobIO->TableReader,
                    Host->GetMasterChannel(),
                    Host->GetBlockCache(),
                    std::move(chunks),
                    provider);

                readers.push_back(reader);
            }

            Reader = CreateMergingReader(readers);
        }

        {
            const auto& mergeSpec = jobSpec.GetExtension(TMergeJobSpecExt::merge_job_spec_ext);

            // ToDo(psushin): estimate row count for writer.
            auto transactionId = TTransactionId::FromProto(jobSpec.output_transaction_id());
            const auto& outputSpec = jobSpec.output_specs(0);

            auto chunkListId = TChunkListId::FromProto(outputSpec.chunk_list_id());
            auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
            options->KeyColumns = FromProto<Stroka>(mergeSpec.key_columns());

            auto writerProvider = New<TTableChunkWriterProvider>(
                config->JobIO->TableWriter,
                options);

            Writer = CreateSyncWriter<TTableChunkWriter>(New<TWriter>(
                config->JobIO->TableWriter,
                options,
                writerProvider,
                Host->GetMasterChannel(),
                transactionId,
                chunkListId));
        }
    }

    virtual NScheduler::NProto::TJobResult Run() override
    {
        PROFILE_TIMING ("/sorted_merge_time") {;

            // Open readers, remove invalid ones, and create the initial heap.
            LOG_INFO("Initializing");
            {
                Reader->Open();
                Writer->Open();
            }
            PROFILE_TIMING_CHECKPOINT("init");

            // Run the actual merge.
            LOG_INFO("Merging");
            while (const TRow* row = Reader->GetRow()) {
                Writer->WriteRowUnsafe(*row, Reader->GetKey());
            }
            PROFILE_TIMING_CHECKPOINT("merge");

            LOG_INFO("Finalizing");
            {
                Writer->Close();

                TJobResult result;
                ToProto(result.mutable_error(), TError());
                return result;
            }
        }
    }

    double GetProgress() const override
    {
        i64 total = Reader->GetRowCount();
        if (total == 0) {
            LOG_WARNING("GetProgress: empty total");
            return 0;
        } else {
            double progress = (double) Reader->GetRowIndex() / total;
            LOG_DEBUG("GetProgress: %lf", progress);
            return progress;
        }
    }

    std::vector<NChunkClient::TChunkId> GetFailedChunks() const override
    {
        return Reader->GetFailedChunks();
    }

private:
    ISyncReaderPtr Reader;
    ISyncWriterUnsafePtr Writer;

};

TJobPtr CreateSortedMergeJob(IJobHost* host)
{
    return New<TSortedMergeJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
