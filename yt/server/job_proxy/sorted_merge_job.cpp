#include "stdafx.h"
#include "sorted_merge_job.h"
#include "private.h"
#include "job_detail.h"
#include "config.h"

#include <ytlib/chunk_client/reader.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>
#include <ytlib/chunk_client/old_multi_chunk_sequential_reader.h>

#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/chunk_client/old_multi_chunk_sequential_reader.h>
#include <ytlib/table_client/merging_reader.h>

#include <core/ytree/yson_string.h>

namespace NYT {
namespace NJobProxy {

using namespace NYTree;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NScheduler::NProto;
using namespace NJobTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static auto& Profiler = JobProxyProfiler;

typedef TOldMultiChunkSequentialWriter<TTableChunkWriterProvider> TWriter;

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeJob
    : public TJob
{
public:
    explicit TSortedMergeJob(IJobHost* host)
        : TJob(host)
        , JobSpec(host->GetJobSpec())
        , SchedulerJobSpecExt(JobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
        , MergeJobSpecExt(JobSpec.GetExtension(TMergeJobSpecExt::merge_job_spec_ext))
    {
        auto config = host->GetConfig();

        YCHECK(SchedulerJobSpecExt.output_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt.output_specs(0);

        {
            std::vector<TTableChunkSequenceReaderPtr> readers;
            auto options = New<TChunkReaderOptions>();
            options->ReadKey = true;

            for (const auto& inputSpec : SchedulerJobSpecExt.input_specs()) {
                // ToDo(psushin): validate that input chunks are sorted.
                std::vector<TChunkSpec> chunks(inputSpec.chunks().begin(), inputSpec.chunks().end());

                auto provider = New<TTableChunkReaderProvider>(
                    chunks,
                    config->JobIO->TableReader,
                    options);

                auto reader = New<TTableChunkSequenceReader>(
                    config->JobIO->TableReader,
                    host->GetMasterChannel(),
                    host->GetBlockCache(),
                    host->GetNodeDirectory(),
                    std::move(chunks),
                    provider);

                readers.push_back(reader);
            }

            Reader = CreateMergingReader(readers);
        }

        {
            // ToDo(psushin): estimate row count for writer.
            auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt.output_transaction_id());
            auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

            auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
            options->KeyColumns = FromProto<Stroka>(MergeJobSpecExt.key_columns());

            auto writerProvider = New<TTableChunkWriterProvider>(
                config->JobIO->TableWriter,
                options);

            Writer = CreateSyncWriter<TTableChunkWriterProvider>(New<TWriter>(
                config->JobIO->TableWriter,
                options,
                writerProvider,
                host->GetMasterChannel(),
                transactionId,
                chunkListId));
        }
    }

    virtual TJobResult Run() override
    {
        PROFILE_TIMING ("/sorted_merge_time") {;

            // Open readers, remove invalid ones, and create the initial heap.
            LOG_INFO("Initializing");
            {
                Reader->Open();
            }
            PROFILE_TIMING_CHECKPOINT("init");

            // Run the actual merge.
            LOG_INFO("Merging");

            while (const TRow* row = Reader->GetRow()) {
                if (SchedulerJobSpecExt.enable_sort_verification()) {
                    Writer->WriteRow(*row);
                } else {
                    Writer->WriteRowUnsafe(*row, Reader->GetKey());
                }
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

    virtual double GetProgress() const override
    {
        i64 total = Reader->GetSessionRowCount();
        if (total == 0) {
            LOG_WARNING("GetProgress: empty total");
            return 0;
        } else {
            double progress = (double) Reader->GetSessionRowIndex() / total;
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
        ToProto(result.mutable_input(), Reader->GetDataStatistics());
        ToProto(result.mutable_output(), Writer->GetDataStatistics());
        return result;
    }

private:
    const TJobSpec& JobSpec;
    const TSchedulerJobSpecExt& SchedulerJobSpecExt;
    const TMergeJobSpecExt& MergeJobSpecExt;

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
