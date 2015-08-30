#include "stdafx.h"

#include "job_detail.h"

#include "private.h"

#include <server/exec_agent/public.h>

#include <ytlib/table_client/helpers.h>
#include <ytlib/table_client/schemaless_chunk_reader.h>
#include <ytlib/table_client/schemaless_chunk_writer.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/query_client/public.h>
#include <ytlib/query_client/config.h>
#include <ytlib/query_client/evaluator.h>
#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/query_statistics.h>

namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NQueryClient;
using namespace NExecAgent;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = JobProxyProfiler;
static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(IJobHost* host)
    : Host(MakeWeak(host))
    , StartTime(TInstant::Now())
{
    YCHECK(host);
}

std::vector<NChunkClient::TChunkId> TJob::DumpInputContext()
{
    THROW_ERROR_EXCEPTION("Dumping input context is not supported for built-in jobs");
}

TYsonString TJob::Strace()
{
    THROW_ERROR_EXCEPTION("Stracing is not supported for built-in jobs");
}

////////////////////////////////////////////////////////////////////////////////

TSimpleJobBase::TSimpleJobBase(IJobHost* host)
    : TJob(host)
    , JobSpec_(host->GetJobSpec())
    , SchedulerJobSpecExt_(JobSpec_.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
    , TotalRowCount_(0)
{ }

TJobResult TSimpleJobBase::Run()
{
    PROFILE_TIMING ("/job_time") {
        LOG_INFO("Initializing");

        auto host = Host.Lock();
        YCHECK(host);

        const auto& jobSpec = host->GetJobSpec().GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext);
        if (jobSpec.has_input_query_spec()) {
            auto writer = CreateSchemafulWriterAdapter(WriterFactory_);
            auto querySpec = jobSpec.input_query_spec();
            auto query = FromProto(querySpec.query());
            std::vector<TUdfDescriptorPtr> descriptors;
            for (const auto& descriptor : FromProto<Stroka>(querySpec.udf_descriptors())) {
                descriptors.push_back(ConvertTo<TUdfDescriptorPtr>(TYsonString(descriptor)));
            }
            auto registry = CreateJobFunctionRegistry(descriptors, SandboxDirectoryNames[ESandboxIndex::Udf]);
            auto evaluator = New<TEvaluator>(New<TExecutorConfig>());
            auto reader = WaitFor(CreateSchemafulReaderAdapter(ReaderFactory_, query->TableSchema))
                .ValueOrThrow();

            LOG_INFO("Reading, evaluating query and writing");
            {
                evaluator->Run(query, reader, writer, registry, true);
            }
        } else {
            CreateReader();
            CreateWriter();
            WaitFor(Reader_->Open())
                .ThrowOnError();
            WaitFor(Writer_->Open())
                .ThrowOnError();

            PROFILE_TIMING_CHECKPOINT("init");

            LOG_INFO("Reading and writing");
            {
                PipeReaderToWriter(Reader_, Writer_, 10000, true);
            }
        }

        PROFILE_TIMING_CHECKPOINT("reading_writing");

        LOG_INFO("Finalizing");
        {
            TJobResult result;
            ToProto(result.mutable_error(), TError());

            // ToDo(psushin): return written chunks only if required.
            auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            Writer_->GetNodeDirectory()->DumpTo(schedulerResultExt->mutable_output_node_directory());
            ToProto(schedulerResultExt->mutable_output_chunks(), Writer_->GetWrittenChunks());

            return result;
        }
    }
}

double TSimpleJobBase::GetProgress() const
{
    if (TotalRowCount_ == 0) {
        LOG_WARNING("Job progress: empty total");
        return 0;
    } else {
        i64 rowCount = Reader_ ? Reader_->GetDataStatistics().row_count() : 0;
        double progress = (double) rowCount / TotalRowCount_;
        LOG_DEBUG("Job progress: %lf, read row count: %" PRId64, progress, rowCount);
        return progress;
    }
}

std::vector<TChunkId> TSimpleJobBase::GetFailedChunkIds() const
{
    return Reader_ ? Reader_->GetFailedChunkIds() : std::vector<TChunkId>();
}

TStatistics TSimpleJobBase::GetStatistics() const
{
    TStatistics result;
    if (Reader_) {
        result.AddComplex("/data/input", Reader_->GetDataStatistics());
    }
    if (Writer_) {
        result.AddComplex(
            "/data/output/" + NYPath::ToYPathLiteral(0),
            Writer_->GetDataStatistics());
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

