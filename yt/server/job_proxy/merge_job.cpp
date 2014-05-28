#include "stdafx.h"

#include "merge_job.h"

#include "config.h"
#include "job_detail.h"
#include "private.h"

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>

#include <ytlib/transaction_client/public.h>

#include <core/concurrency/scheduler.h>

#include <core/ytree/yson_string.h>

namespace NYT {
namespace NJobProxy {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NVersionedTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;
static auto& Profiler = JobProxyProfiler;

////////////////////////////////////////////////////////////////////////////////

class TMergeJob
    : public TJob
{
public:
    explicit TMergeJob(IJobHost* host, bool parallelReader)
        : TJob(host)
        , JobSpec_(host->GetJobSpec())
        , SchedulerJobSpecExt_(JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
        , TotalRowCount_(0)
    {
        auto config = host->GetConfig();

        YCHECK(SchedulerJobSpecExt_.output_specs_size() == 1);

        if (JobSpec_.HasExtension(TMergeJobSpecExt::merge_job_spec_ext)) {
            const auto& mergeJobSpec = JobSpec_.GetExtension(TMergeJobSpecExt::merge_job_spec_ext);
            KeyColumns_ = FromProto<Stroka>(mergeJobSpec.key_columns());
            LOG_INFO("Ordered merge produces sorted output");
        }

        std::vector<TChunkSpec> chunkSpecs;
        for (const auto& inputSpec : SchedulerJobSpecExt_.input_specs()) {
            for (const auto& chunkSpec : inputSpec.chunks()) {
                chunkSpecs.push_back(chunkSpec);
            }
        }

        TotalRowCount_ = GetCumulativeRowCount(chunkSpecs);

        auto nameTable = New<TNameTable>();

        auto readerFactory = parallelReader
            ? CreateSchemalessParallelMultiChunkReader
            : CreateSchemalessSequentialMultiChunkReader;

        Reader_ = readerFactory(
            config->JobIO->NewTableReader,
            New<TMultiChunkReaderOptions>(),
            host->GetMasterChannel(),
            host->GetCompressedBlockCache(),
            host->GetNodeDirectory(),
            std::move(chunkSpecs),
            nameTable,
            TKeyColumns());

        // ToDo(psushin): estimate row count for writer.
        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        const auto& outputSpec = SchedulerJobSpecExt_.output_specs(0);
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));

        Writer_ = CreateSchemalessMultiChunkWriter(
            config->JobIO->NewTableWriter,
            options,
            nameTable,
            KeyColumns_,
            host->GetMasterChannel(),
            transactionId,
            chunkListId,
            true); // Allow value reordering if key columns are present.
    }

    virtual NJobTrackerClient::NProto::TJobResult Run() override
    {
        PROFILE_TIMING ("/merge_time") {
            LOG_INFO("Initializing");

            {
                auto error = WaitFor(Reader_->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            } {
                auto error = WaitFor(Writer_->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }

            PROFILE_TIMING_CHECKPOINT("init");

            LOG_INFO("Merging");
            {
                std::vector<TUnversionedRow> rows;
                rows.reserve(10000);

                while (Reader_->Read(&rows)) {
                    if (rows.empty()) {
                        auto error = WaitFor(Reader_->GetReadyEvent());
                        THROW_ERROR_EXCEPTION_IF_FAILED(error);
                        continue;
                    }

                    if (!Writer_->Write(rows)) {
                        auto error = WaitFor(Writer_->GetReadyEvent());
                        THROW_ERROR_EXCEPTION_IF_FAILED(error);
                    }
                }

                YCHECK(rows.empty());
            }

            PROFILE_TIMING_CHECKPOINT("merge");

            LOG_INFO("Finalizing");
            {
                auto error = WaitFor(Writer_->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);

                TJobResult result;
                ToProto(result.mutable_error(), TError());
                return result;
            }
        }
    }

    virtual double GetProgress() const override
    {
        if (TotalRowCount_ == 0) {
            LOG_WARNING("Job progress: empty total");
            return 0;
        } else {
            i64 rowCount = Reader_->GetDataStatistics().row_count();
            double progress = (double) rowCount / TotalRowCount_;
            LOG_DEBUG("Job progress: %lf, read row count: %" PRId64, progress, rowCount);
            return progress;
        }
    }

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override
    {
        return Reader_->GetFailedChunkIds();
    }

    virtual TJobStatistics GetStatistics() const override
    {
        TJobStatistics result;
        result.set_time(GetElapsedTime().MilliSeconds());
        ToProto(result.mutable_input(), Reader_->GetDataStatistics());
        ToProto(result.add_output(), Writer_->GetDataStatistics());
        return result;
    }

private:
    const TJobSpec& JobSpec_;
    const TSchedulerJobSpecExt& SchedulerJobSpecExt_;

    ISchemalessMultiChunkReaderPtr Reader_;
    ISchemalessMultiChunkWriterPtr Writer_;

    TKeyColumns KeyColumns_;

    i64 TotalRowCount_;

};

IJobPtr CreateOrderedMergeJob(IJobHost* host)
{
    return New<TMergeJob>(host, false);
}

IJobPtr CreateUnorderedMergeJob(IJobHost* host)
{
    return New<TMergeJob>(host, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
