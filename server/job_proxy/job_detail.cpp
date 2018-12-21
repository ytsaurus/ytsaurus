#include "job_detail.h"
#include "private.h"
#include "config.h"

#include <yt/server/exec_agent/public.h>

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/job_proxy/helpers.h>

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/client/table_client/schemaless_writer.h>

#include <yt/core/misc/collection_helpers.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NJobTrackerClient::NProto;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NQueryClient;
using namespace NExecAgent;
using namespace NJobAgent;

using NJobTrackerClient::TStatistics;
using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = JobProxyProfiler;
static const auto& Logger = JobProxyLogger;

static const int PipeBufferRowCount = 10240;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(IJobHostPtr host)
    : Host_(std::move(host))
    , StartTime_(TInstant::Now())
{
    YCHECK(Host_);

    BlockReadOptions_.WorkloadDescriptor = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->WorkloadDescriptor;
    BlockReadOptions_.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    BlockReadOptions_.ReadSessionId = TReadSessionId::Create();
}

std::vector<NChunkClient::TChunkId> TJob::DumpInputContext()
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Dumping input context is not supported for built-in jobs");
}

TString TJob::GetStderr()
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Getting stderr is not supported for built-in jobs");
}

std::optional<TString> TJob::GetFailContext()
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Getting stderr is not supported for built-in jobs");
}

std::optional<TJobProfile> TJob::GetProfile()
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Getting profile is not supported for built-in jobs");
}

TYsonString TJob::StraceJob()
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Stracing is not supported for built-in jobs");
}

void TJob::SignalJob(const TString& /*signalName*/)
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Signaling is not supported for built-in jobs");
}

TYsonString TJob::PollJobShell(const TYsonString& /*parameters*/)
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Job shell is not supported for built-in jobs");
}

void TJob::Fail()
{
    THROW_ERROR_EXCEPTION("Failing is not supported for built-in jobs");
}

TCpuStatistics TJob::GetCpuStatistics() const
{
    return TCpuStatistics{};
}

////////////////////////////////////////////////////////////////////////////////

TSimpleJobBase::TSimpleJobBase(IJobHostPtr host)
    : TJob(host)
    , JobSpec_(host->GetJobSpecHelper()->GetJobSpec())
    , SchedulerJobSpecExt_(host->GetJobSpecHelper()->GetSchedulerJobSpecExt())
{ }

TJobResult TSimpleJobBase::Run()
{
    PROFILE_TIMING ("/job_time") {
        YT_LOG_INFO("Initializing");

        Host_->OnPrepared();

        const auto& jobSpec = Host_->GetJobSpecHelper()->GetSchedulerJobSpecExt();
        if (jobSpec.has_input_query_spec()) {
            RunQuery(
                jobSpec.input_query_spec(),
                ReaderFactory_,
                WriterFactory_,
                SandboxDirectoryNames[ESandboxKind::Udf]);
        } else {
            CreateReader();
            Initialized_ = true;

            CreateWriter();

            PROFILE_TIMING_CHECKPOINT("init");

            YT_LOG_INFO("Reading and writing");

            TPipeReaderToWriterOptions options;
            options.BufferRowCount = Host_->GetJobSpecHelper()->GetJobIOConfig()->BufferRowCount;
            options.PipeDelay = Host_->GetJobSpecHelper()->GetJobIOConfig()->Testing->PipeDelay;
            options.ValidateValues = true;
            PipeReaderToWriter(
                Reader_,
                Writer_,
                options);
        }

        PROFILE_TIMING_CHECKPOINT("reading_writing");

        YT_LOG_INFO("Finalizing");
        {
            TJobResult result;
            ToProto(result.mutable_error(), TError());

            // ToDo(psushin): return written chunks only if required.
            auto* schedulerResultExt = result.MutableExtension(TSchedulerJobResultExt::scheduler_job_result_ext);
            ToProto(schedulerResultExt->mutable_output_chunk_specs(), Writer_->GetWrittenChunksMasterMeta());

            if (ShouldSendBoundaryKeys()) {
                *schedulerResultExt->add_output_boundary_keys() = GetWrittenChunksBoundaryKeys(Writer_);
            }

            return result;
        }
    }
}

void TSimpleJobBase::Cleanup()
{ }

bool TSimpleJobBase::ShouldSendBoundaryKeys() const
{
    return true;
}

double TSimpleJobBase::GetProgress() const
{
    if (TotalRowCount_ == 0) {
        YT_LOG_WARNING("Job progress: empty total");
        return 0;
    } else {
        i64 rowCount = Reader_ ? Reader_->GetDataStatistics().row_count() : 0;
        double progress = (double) rowCount / TotalRowCount_;
        YT_LOG_DEBUG("Job progress: %lf, read row count: %" PRId64, progress, rowCount);
        return progress;
    }
}

ui64 TSimpleJobBase::GetStderrSize() const
{
    return 0;
}

std::vector<TChunkId> TSimpleJobBase::GetFailedChunkIds() const
{
    return Reader_ ? Reader_->GetFailedChunkIds() : std::vector<TChunkId>();
}

TStatistics TSimpleJobBase::GetStatistics() const
{
    TStatistics result;
    if (Reader_) {
        result.AddSample("/data/input", Reader_->GetDataStatistics());
        DumpCodecStatistics(Reader_->GetDecompressionStatistics(), "/codec/cpu/decode", &result);
        DumpChunkReaderStatistics(&result, "/chunk_reader_statistics", BlockReadOptions_.ChunkReaderStatistics);
    }

    if (Writer_) {
        result.AddSample("/data/output/0", Writer_->GetDataStatistics());
        DumpCodecStatistics(Writer_->GetCompressionStatistics(), "/codec/cpu/encode/0", &result);
    }

    return result;
}

TTableWriterConfigPtr TSimpleJobBase::GetWriterConfig(const TTableOutputSpec& outputSpec)
{
    auto config = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableWriter;
    if (outputSpec.has_table_writer_config()) {
        config = UpdateYsonSerializable(
            config,
            ConvertTo<INodePtr>(TYsonString(outputSpec.table_writer_config())));
    }
    return config;
}

TInterruptDescriptor TSimpleJobBase::GetInterruptDescriptor() const
{
    if (Interrupted_) {
        YCHECK(Reader_);
        return Reader_->GetInterruptDescriptor(NYT::TRange<TUnversionedRow>());
    } else {
        return {};
    }
}

void TSimpleJobBase::Interrupt()
{
    if (!Host_->GetJobSpecHelper()->IsReaderInterruptionSupported()) {
        THROW_ERROR_EXCEPTION("Interrupting is not supported for this type of jobs")
            << TErrorAttribute("job_type", Host_->GetJobSpecHelper()->GetJobType());
    }

    if (!Initialized_) {
        THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot interrupt uninitialized reader");
    }

    if (!Interrupted_) {
        YCHECK(Reader_);
        Interrupted_ = true;

        if (Reader_->GetDataStatistics().row_count() > 0) {
            Reader_->Interrupt();
        } else {
            THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot interrupt reader that didn't start reading");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

