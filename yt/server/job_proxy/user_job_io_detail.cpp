#include "user_job_io_detail.h"
#include "config.h"
#include "job.h"

#include <yt/server/misc/job_table_schema.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/chunk_client/schema.h>
#include <yt/ytlib/chunk_client/schema.pb.h>

#include <yt/ytlib/table_client/blob_table_writer.h>
#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/ytree/convert.h>

#include <yt/core/misc/finally.h>

namespace NYT {
namespace NJobProxy {

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

TUserJobIOBase::TUserJobIOBase(IJobHostPtr host)
    : Host_(host)
    , SchedulerJobSpec_(Host_->GetJobSpec().GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
    , JobIOConfig_(Host_->GetConfig()->JobIO)
    , Logger(host->GetLogger())
{ }

TUserJobIOBase::~TUserJobIOBase()
{ }

void TUserJobIOBase::Init()
{
    LOG_INFO("Opening writers");

    auto guard = Finally([&] () {
        Initialized_ = true;
    });

    auto transactionId = FromProto<TTransactionId>(SchedulerJobSpec_.output_transaction_id());
    for (const auto& outputSpec : SchedulerJobSpec_.output_table_specs()) {
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->ValidateDuplicateIds = true;
        options->ValidateRowWeight = true;
        options->ValidateColumnCount = true;

        auto writerConfig = JobIOConfig_->TableWriter;
        if (outputSpec.has_table_writer_config()) {
            writerConfig = UpdateYsonSerializable(
                writerConfig,
                ConvertTo<INodePtr>(TYsonString(outputSpec.table_writer_config())));
        }

        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        TTableSchema schema;
        if (outputSpec.has_table_schema()) {
            schema = FromProto<TTableSchema>(outputSpec.table_schema());
        }

        auto writer = DoCreateWriter(writerConfig, options, chunkListId, transactionId, schema);
        // ToDo(psushin): open writers in parallel.
        auto error = WaitFor(writer->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
        Writers_.push_back(writer);
    }

    if (SchedulerJobSpec_.user_job_spec().has_stderr_table_spec()) {
        const auto& stderrTableSpec = SchedulerJobSpec_.user_job_spec().stderr_table_spec();
        const auto& outputTableSpec = stderrTableSpec.output_table_spec();
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(stderrTableSpec.output_table_spec().table_writer_options()));
        options->ValidateDuplicateIds = true;
        options->ValidateRowWeight = true;
        options->ValidateColumnCount = true;

        auto stderrTableWriterConfig = ConvertTo<TBlobTableWriterConfigPtr>(
            TYsonString(stderrTableSpec.blob_table_writer_config()));

        StderrTableWriter_.reset(
            new NTableClient::TBlobTableWriter(
                GetStderrBlobTableSchema(),
                {ConvertToYsonString(Host_->GetJobId())},
                Host_->GetClient(),
                stderrTableWriterConfig,
                options,
                transactionId,
                FromProto<TChunkListId>(outputTableSpec.chunk_list_id())));
    }
}

void TUserJobIOBase::InitReader(TNameTablePtr nameTable, const TColumnFilter& columnFilter)
{
    YCHECK(!Reader_);
    Reader_ = DoCreateReader(std::move(nameTable), columnFilter);
}

void TUserJobIOBase::CreateReader()
{
    LOG_INFO("Creating reader");
    InitReader(New<TNameTable>(), TColumnFilter());
}

TSchemalessReaderFactory TUserJobIOBase::GetReaderFactory()
{
    for (const auto& inputSpec : SchedulerJobSpec_.input_table_specs()) {
        for (const auto& dataSliceDescriptor : inputSpec.data_slice_descriptors()) {
            for (const auto& chunkSpec : dataSliceDescriptor.chunks()) {
                if (chunkSpec.has_channel() && !FromProto<NChunkClient::TChannel>(chunkSpec.channel()).IsUniversal()) {
                    THROW_ERROR_EXCEPTION("Channels and QL filter cannot appear in the same operation");
                }
            }
        }
    }

    return [&] (TNameTablePtr nameTable, TColumnFilter columnFilter) {
        InitReader(std::move(nameTable), std::move(columnFilter));
        return Reader_;
    };
}

int TUserJobIOBase::GetKeySwitchColumnCount() const
{
    return 0;
}

std::vector<ISchemalessMultiChunkWriterPtr> TUserJobIOBase::GetWriters() const
{
    if (Initialized_) {
        return Writers_;
    } else {
        return std::vector<ISchemalessMultiChunkWriterPtr>();
    }
}

ISchemalessMultiChunkReaderPtr TUserJobIOBase::GetReader() const
{
    if (Initialized_) {
        return Reader_;
    } else {
        return nullptr;
    }
}

TOutputStream* TUserJobIOBase::GetStderrTableWriter() const
{
    if (Initialized_) {
        return StderrTableWriter_.get();
    } else {
        return nullptr;
    }
}

void TUserJobIOBase::PopulateResult(TSchedulerJobResultExt* schedulerJobResultExt)
{
    for (const auto& writer : Writers_) {
        *schedulerJobResultExt->add_output_boundary_keys() = GetWrittenChunksBoundaryKeys(writer);
    }
}

void TUserJobIOBase::PopulateStderrResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt)
{
    if (StderrTableWriter_) {
        *schedulerJobResultExt->mutable_stderr_table_boundary_keys() = StderrTableWriter_->GetOutputResult();
    }
}

void TUserJobIOBase::InterruptReader()
{
    THROW_ERROR_EXCEPTION("Interrupting is not supported for this user job")
        << TErrorAttribute("job_type", Host_->GetJobSpec().type());
}

std::vector<TDataSliceDescriptor> TUserJobIOBase::GetUnreadDataSliceDescriptors() const
{
    THROW_ERROR_EXCEPTION("Getting of unread data slice descriptors is not supported for this user job")
        << TErrorAttribute("job_type", Host_->GetJobSpec().type());
}

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkWriterPtr TUserJobIOBase::CreateTableWriter(
    TTableWriterConfigPtr config,
    TTableWriterOptionsPtr options,
    const TChunkListId& chunkListId,
    const TTransactionId& transactionId,
    const TTableSchema& tableSchema)
{
    auto nameTable = New<TNameTable>();
    nameTable->SetEnableColumnNameValidation();

    return CreateSchemalessMultiChunkWriter(
        std::move(config),
        std::move(options),
        std::move(nameTable),
        tableSchema,
        TOwningKey(),
        Host_->GetClient(),
        CellTagFromId(chunkListId),
        transactionId,
        chunkListId);
}

ISchemalessMultiChunkReaderPtr TUserJobIOBase::CreateRegularReader(
    bool isParallel,
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter)
{
    std::vector<TDataSliceDescriptor> dataSliceDescriptors;
    for (const auto& inputSpec : SchedulerJobSpec_.input_table_specs()) {
        for (const auto& descriptor : inputSpec.data_slice_descriptors()) {
            auto dataSliceDescriptor = FromProto<TDataSliceDescriptor>(descriptor);
            dataSliceDescriptors.push_back(std::move(dataSliceDescriptor));
        }
    }

    auto options = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
        SchedulerJobSpec_.input_table_specs(0).table_reader_options()));

    return CreateTableReader(options, std::move(dataSliceDescriptors), std::move(nameTable), columnFilter, isParallel);
}

ISchemalessMultiChunkReaderPtr TUserJobIOBase::CreateTableReader(
    NTableClient::TTableReaderOptionsPtr options,
    std::vector<TDataSliceDescriptor> dataSliceDescriptors,
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter,
    bool isParallel)
{
    if (isParallel) {
        return CreateSchemalessParallelMultiChunkReader(
            JobIOConfig_->TableReader,
            options,
            Host_->GetClient(),
            Host_->LocalDescriptor(),
            Host_->GetBlockCache(),
            Host_->GetInputNodeDirectory(),
            std::move(dataSliceDescriptors),
            std::move(nameTable),
            columnFilter);
    } else {
        return CreateSchemalessSequentialMultiChunkReader(
            JobIOConfig_->TableReader,
            options,
            Host_->GetClient(),
            Host_->LocalDescriptor(),
            Host_->GetBlockCache(),
            Host_->GetInputNodeDirectory(),
            std::move(dataSliceDescriptors),
            std::move(nameTable),
            columnFilter);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
