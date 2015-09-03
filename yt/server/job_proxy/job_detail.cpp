#include "stdafx.h"

#include "job_detail.h"

#include "private.h"

#include <ytlib/new_table_client/helpers.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler::NProto;
using namespace NVersionedTableClient;
using namespace NYTree;
using namespace NScheduler;

using NJobTrackerClient::TStatistics;

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

        {
            auto error = WaitFor(Reader_->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        } {
            auto error = WaitFor(Writer_->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        PROFILE_TIMING_CHECKPOINT("init");

        LOG_INFO("Reading and writing");
        {
            PipeReaderToWriter(Reader_, Writer_, 10000, true);
        }

        PROFILE_TIMING_CHECKPOINT("reading_writing");

        LOG_INFO("Finalizing");
        {
            TJobResult result;
            ToProto(result.mutable_error(), TError());

            // ToDo(psushin): return written chunks only if required.
            auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            Writer_->GetNodeDirectory()->DumpTo(schedulerResultExt->mutable_node_directory());
            ToProto(schedulerResultExt->mutable_chunks(), Writer_->GetWrittenChunks());

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
        i64 rowCount = Reader_->GetDataStatistics().row_count();
        double progress = (double) rowCount / TotalRowCount_;
        LOG_DEBUG("Job progress: %lf, read row count: %" PRId64, progress, rowCount);
        return progress;
    }
}

std::vector<TChunkId> TSimpleJobBase::GetFailedChunkIds() const
{
    return Reader_->GetFailedChunkIds();
}

TStatistics TSimpleJobBase::GetStatistics() const
{
    TStatistics result;
    result.AddSample("/data/input", Reader_->GetDataStatistics());
    
    result.AddSample(
        "/data/output/" + NYPath::ToYPathLiteral(0),
        Writer_->GetDataStatistics());

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

