#include "job_detail.h"
#include "private.h"

#include <yt/yt/server/lib/exec_node/public.h>
#include <yt/yt/server/lib/exec_node/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/parallel_reader_memory_manager.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/ytlib/job_proxy/helpers.h>

#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/yt/client/table_client/unversioned_writer.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NJobProxy {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NScheduler;
using namespace NQueryClient;
using namespace NExecNode;
using namespace NJobAgent;
using namespace NCoreDump;

using NChunkClient::NProto::TDataStatistics;
using NChunkClient::TDataSliceDescriptor;
using NChunkClient::TChunkReaderStatistics;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = JobProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(IJobHostPtr host)
    : Host_(host)
    , StartTime_(TInstant::Now())
{
    YT_VERIFY(Host_);

    ChunkReadOptions_.WorkloadDescriptor = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableReader->WorkloadDescriptor;
    ChunkReadOptions_.ChunkReaderStatistics = New<TChunkReaderStatistics>();
    ChunkReadOptions_.ReadSessionId = TReadSessionId::Create();
}

void TJob::Initialize()
{
    PopulateInputNodeDirectory();

    const auto& schedulerJobSpecExt = Host_->GetJobSpecHelper()->GetJobSpecExt();
    JobProfiler_ = CreateJobProfiler(&schedulerJobSpecExt);
    JobProfiler_->Start();
}

void TJob::PopulateInputNodeDirectory() const
{
    Host_->GetClient()->GetNativeConnection()->GetNodeDirectory()->MergeFrom(
        Host_->GetJobSpecHelper()->GetJobSpecExt().input_node_directory());
}

std::vector<NChunkClient::TChunkId> TJob::DumpInputContext(TTransactionId /*transactionId*/)
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

std::vector<TJobProfile> TJob::GetProfiles()
{
    if (!JobProfiler_) {
        // Job is not initialized yet.
        return {};
    }

    JobProfiler_->Stop();
    return JobProfiler_->GetProfiles();
}

const TCoreInfos& TJob::GetCoreInfos() const
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Getting core infos is not supported for built-in jobs");
}

NApi::TPollJobShellResponse TJob::PollJobShell(
    const NJobProberClient::TJobShellDescriptor& /*jobShellDescriptor*/,
    const TYsonString& /*parameters*/)
{
    THROW_ERROR_EXCEPTION(
        EErrorCode::UnsupportedJobType,
        "Job shell is not supported for built-in jobs");
}

void TJob::GracefulAbort(TError /*error*/)
{
    THROW_ERROR_EXCEPTION("Graceful abort is not supported for built-in jobs");
}

void TJob::Fail()
{
    THROW_ERROR_EXCEPTION("Failing is not supported for built-in jobs");
}

i64 TJob::GetStderrSize() const
{
    return 0;
}

TSharedRef TJob::DumpSensors()
{
    YT_UNIMPLEMENTED();
}

std::optional<TJobEnvironmentCpuStatistics> TJob::GetUserJobCpuStatistics() const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TSimpleJobBase::TSimpleJobBase(IJobHostPtr host)
    : TJob(host)
    , JobSpec_(host->GetJobSpecHelper()->GetJobSpec())
    , JobSpecExt_(host->GetJobSpecHelper()->GetJobSpecExt())
{ }

void TSimpleJobBase::Initialize()
{
    TJob::Initialize();

    // Initialize parallel reader memory manager.
    {
        auto totalReaderMemoryLimit = GetTotalReaderMemoryLimit();
        TParallelReaderMemoryManagerOptions parallelReaderMemoryManagerOptions{
            .TotalReservedMemorySize = totalReaderMemoryLimit,
            .MaxInitialReaderReservedMemory = totalReaderMemoryLimit
        };
        MultiReaderMemoryManager_ = CreateParallelReaderMemoryManager(
            parallelReaderMemoryManagerOptions,
            NChunkClient::TDispatcher::Get()->GetReaderMemoryManagerInvoker());
    }
}

TJobResult TSimpleJobBase::Run()
{
    YT_LOG_INFO("Initializing");

    Host_->OnPrepared();

    const auto& jobSpec = Host_->GetJobSpecHelper()->GetJobSpecExt();
    if (jobSpec.has_input_query_spec()) {
        RunQuery(
            jobSpec.input_query_spec(),
            BIND(&TSimpleJobBase::DoInitializeReader, MakeStrong(this)),
            BIND(&TSimpleJobBase::DoInitializeWriter, MakeStrong(this)),
            GetSandboxRelPath(ESandboxKind::Udf));
    } else {
        InitializeReader();
        InitializeWriter();

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

    YT_LOG_INFO("Finalizing");
    {
        TJobResult result;
        ToProto(result.mutable_error(), TError());

        // ToDo(psushin): return written chunks only if required.
        auto* jobResultExt = result.MutableExtension(TJobResultExt::job_result_ext);
        for (const auto& chunkSpec : Writer_->GetWrittenChunkSpecs()) {
            auto* resultChunkSpec = jobResultExt->add_output_chunk_specs();
            *resultChunkSpec = chunkSpec;
            FilterProtoExtensions(resultChunkSpec->mutable_chunk_meta()->mutable_extensions(), GetSchedulerChunkMetaExtensionTagsFilter());
        }

        if (ShouldSendBoundaryKeys()) {
            *jobResultExt->add_output_boundary_keys() = GetWrittenChunksBoundaryKeys(Writer_);
        }

        return result;
    }
}

void TSimpleJobBase::Cleanup()
{ }

void TSimpleJobBase::PrepareArtifacts()
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

std::vector<TChunkId> TSimpleJobBase::GetFailedChunkIds() const
{
    return Reader_ ? Reader_->GetFailedChunkIds() : std::vector<TChunkId>();
}

IJob::TStatistics TSimpleJobBase::GetStatistics() const
{
    TStatistics result;

    if (Reader_) {
        result.TotalInputStatistics = {
            .DataStatistics = {Reader_->GetDataStatistics()},
            .CodecStatistics = Reader_->GetDecompressionStatistics(),
        },
        result.ChunkReaderStatistics = ChunkReadOptions_.ChunkReaderStatistics;
        result.TimingStatistics = Reader_->GetTimingStatistics();
    }

    if (Writer_) {
        result.OutputStatistics = {{
            .DataStatistics = {Writer_->GetDataStatistics()},
            .CodecStatistics = {Writer_->GetCompressionStatistics()},
        }};
    }

    return result;
}

TTableWriterConfigPtr TSimpleJobBase::GetWriterConfig(const TTableOutputSpec& outputSpec)
{
    auto ioConfig = Host_->GetJobSpecHelper()->GetJobIOConfig();
    auto config = outputSpec.dynamic()
        ? ioConfig->DynamicTableWriter
        : ioConfig->TableWriter;
    if (outputSpec.has_table_writer_config()) {
        config = UpdateYsonStruct(
            config,
            ConvertTo<INodePtr>(TYsonString(outputSpec.table_writer_config())));
    }
    return config;
}

TInterruptDescriptor TSimpleJobBase::GetInterruptDescriptor() const
{
    if (Interrupted_) {
        YT_VERIFY(Reader_);
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
        YT_VERIFY(Reader_);
        Interrupted_ = true;

        if (Reader_->GetDataStatistics().row_count() > 0) {
            Reader_->Interrupt();
        } else {
            THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot interrupt reader that didn't start reading");
        }
    }
}

ISchemalessMultiChunkReaderPtr TSimpleJobBase::DoInitializeReader(
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter)
{
    YT_VERIFY(!Reader_);
    YT_VERIFY(!Initialized_.load());

    Reader_ = ReaderFactory_(nameTable, columnFilter);
    Initialized_ = true;

    YT_LOG_INFO("Reader initialized");

    return Reader_;
}

ISchemalessMultiChunkWriterPtr TSimpleJobBase::DoInitializeWriter(
    TNameTablePtr nameTable,
    TTableSchemaPtr schema)
{
    YT_VERIFY(!Writer_);

    Writer_ = WriterFactory_(nameTable, schema);

    YT_LOG_INFO("Writer initialized");

    return Writer_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
