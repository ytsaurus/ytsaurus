#include "user_job_write_controller.h"
#include "config.h"
#include "job.h"

#include <yt/server/misc/job_table_schema.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/client/object_client/helpers.h>

#include <yt/ytlib/job_proxy/user_job_io_factory.h>

#include <yt/ytlib/table_client/blob_table_writer.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/finally.h>

namespace NYT::NJobProxy {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NObjectClient;
using namespace NYTree;

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

TUserJobWriteController::TUserJobWriteController(IJobHostPtr host)
    : Host_(host)
    , Logger(host->GetLogger())
{ }

TUserJobWriteController::~TUserJobWriteController()
{ }

void TUserJobWriteController::Init()
{
    YT_LOG_INFO("Opening writers");

    auto guard = Finally([&] () {
        Initialized_ = true;
    });

    auto userJobIOFactory = CreateUserJobIOFactory(
        Host_->GetJobSpecHelper(),
        TClientBlockReadOptions(),
        Host_->GetTrafficMeter(),
        Host_->GetInBandwidthThrottler(),
        Host_->GetOutBandwidthThrottler(),
        Host_->GetOutRpsThrottler());

    const auto& schedulerJobSpecExt = Host_->GetJobSpecHelper()->GetSchedulerJobSpecExt();
    auto outputTransactionId = FromProto<TTransactionId>(schedulerJobSpecExt.output_transaction_id());
    for (const auto& outputSpec : schedulerJobSpecExt.output_table_specs()) {
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->EnableValidationOptions();

        auto writerConfig = Host_->GetJobSpecHelper()->GetJobIOConfig()->TableWriter;
        if (outputSpec.has_table_writer_config()) {
            writerConfig = UpdateYsonSerializable(
                writerConfig,
                ConvertTo<INodePtr>(TYsonString(outputSpec.table_writer_config())));
        }

        auto timestamp = static_cast<TTimestamp>(outputSpec.timestamp());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        TTableSchema schema;
        if (outputSpec.has_table_schema()) {
            schema = FromProto<TTableSchema>(outputSpec.table_schema());
        }

        // ToDo(psushin): open writers in parallel.
        auto writer = userJobIOFactory->CreateWriter(
            Host_->GetClient(),
            writerConfig,
            options,
            chunkListId,
            outputTransactionId,
            schema,
            TChunkTimestamps{timestamp, timestamp});

        Writers_.push_back(writer);
    }

    if (schedulerJobSpecExt.user_job_spec().has_stderr_table_spec()) {
        const auto& stderrTableSpec = schedulerJobSpecExt.user_job_spec().stderr_table_spec();
        const auto& outputTableSpec = stderrTableSpec.output_table_spec();
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(stderrTableSpec.output_table_spec().table_writer_options()));
        options->EnableValidationOptions();

        auto stderrTableWriterConfig = ConvertTo<TBlobTableWriterConfigPtr>(
            TYsonString(stderrTableSpec.blob_table_writer_config()));

        auto debugTransactionId = FromProto<TTransactionId>(schedulerJobSpecExt.user_job_spec().debug_output_transaction_id());

        StderrTableWriter_.reset(
            new NTableClient::TBlobTableWriter(
                GetStderrBlobTableSchema(),
                {ConvertToYsonString(Host_->GetJobId())},
                Host_->GetClient(),
                stderrTableWriterConfig,
                options,
                debugTransactionId,
                FromProto<TChunkListId>(outputTableSpec.chunk_list_id()),
                Host_->GetTrafficMeter(),
                Host_->GetOutBandwidthThrottler()));
    }
}

std::vector<ISchemalessMultiChunkWriterPtr> TUserJobWriteController::GetWriters() const
{
    if (Initialized_) {
        return Writers_;
    } else {
        return {};
    }
}

IOutputStream* TUserJobWriteController::GetStderrTableWriter() const
{
    if (Initialized_) {
        return StderrTableWriter_.get();
    } else {
        return nullptr;
    }
}

void TUserJobWriteController::PopulateResult(TSchedulerJobResultExt* schedulerJobResultExt)
{
    std::vector<NChunkClient::NProto::TChunkSpec> writtenChunkSpecs;
    const auto& outputTableSpecs = Host_->GetJobSpecHelper()->GetSchedulerJobSpecExt().output_table_specs();
    for (int index = 0; index < Writers_.size(); ++index) {
        const auto& writer = Writers_[index];
        const auto& outputTableSpec = outputTableSpecs.Get(index);
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputTableSpec.table_writer_options()));
        if (options->ReturnBoundaryKeys) {
            *schedulerJobResultExt->add_output_boundary_keys() = GetWrittenChunksBoundaryKeys(writer);
        }
        auto writtenChunks = writer->GetWrittenChunksMasterMeta();
        for (auto& chunkSpec : writtenChunks) {
            writtenChunkSpecs.emplace_back(std::move(chunkSpec));
        }
    }
    ToProto(schedulerJobResultExt->mutable_output_chunk_specs(), writtenChunkSpecs);
}

void TUserJobWriteController::PopulateStderrResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt)
{
    if (StderrTableWriter_) {
        *schedulerJobResultExt->mutable_stderr_table_boundary_keys() = StderrTableWriter_->GetOutputResult();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
