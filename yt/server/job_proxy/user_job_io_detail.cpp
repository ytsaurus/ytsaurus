#include "user_job_io_detail.h"
#include "config.h"
#include "helpers.h"
#include "job.h"

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/chunk_client/schema.h>
#include <yt/ytlib/chunk_client/schema.pb.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_chunk_writer.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/concurrency/scheduler.h>

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
using namespace NTableClient::NProto;
using namespace NTransactionClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TUserJobIOBase::TUserJobIOBase(IJobHostPtr host)
    : Host_(host)
    , SchedulerJobSpec_(Host_->GetJobSpec().GetExtension(
        TSchedulerJobSpecExt::scheduler_job_spec_ext))
    , JobIOConfig_(Host_->GetConfig()->JobIO)
    , Logger(host->GetLogger())
{ }

void TUserJobIOBase::Init()
{
    LOG_INFO("Opening writers");

    auto guard = Finally([&] () {
        Initialized_ = true;
    });

    auto transactionId = FromProto<TTransactionId>(SchedulerJobSpec_.output_transaction_id());
    for (const auto& outputSpec : SchedulerJobSpec_.output_specs()) {
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        options->ValidateDuplicateIds = true;
        options->ValidateRowWeight = true;
        options->ValidateColumnCount = true;

        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());

        TTableSchema schema;
        if (outputSpec.has_table_schema()) {
            schema = FromProto<TTableSchema>(outputSpec.table_schema());
        }

        auto writer = DoCreateWriter(options, chunkListId, transactionId, schema);
        // ToDo(psushin): open writers in parallel.
        auto error = WaitFor(writer->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
        Writers_.push_back(writer);
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
    for (const auto& inputSpec : SchedulerJobSpec_.input_specs()) {
        for (const auto& chunkSpec : inputSpec.chunks()) {
            if (chunkSpec.has_channel() && !FromProto<NChunkClient::TChannel>(chunkSpec.channel()).IsUniversal()) {
                THROW_ERROR_EXCEPTION("Channels and QL filter cannot appear in the same operation.");
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

void TUserJobIOBase::PopulateResult(TSchedulerJobResultExt* schedulerJobResultExt)
{
    for (const auto& writer : Writers_) {
        *schedulerJobResultExt->add_output_boundary_keys() = GetWrittenChunksBoundaryKeys(writer);
    }
}

ISchemalessMultiChunkWriterPtr TUserJobIOBase::CreateTableWriter(
    TTableWriterOptionsPtr options,
    const TChunkListId& chunkListId,
    const TTransactionId& transactionId,
    const TTableSchema& tableSchema)
{
    auto keyColumns = tableSchema.GetKeyColumns();
    auto nameTable = TNameTable::FromKeyColumns(keyColumns);
<<<<<<< HEAD
    auto writer = CreateSchemalessMultiChunkWriter(
=======
    nameTable->SetEnableColumnNameValidation();

    return CreateSchemalessMultiChunkWriter(
>>>>>>> origin/prestable/18.4
        JobIOConfig_->TableWriter,
        std::move(options),
        std::move(nameTable),
        keyColumns,
        TOwningKey(),
        Host_->GetClient(),
        CellTagFromId(chunkListId),
        transactionId,
        chunkListId,
        true);

    return CreateSchemaValidatingWriter(std::move(writer), tableSchema);
}

ISchemalessMultiChunkReaderPtr TUserJobIOBase::CreateRegularReader(
    bool isParallel,
    TNameTablePtr nameTable,
    const TColumnFilter& columnFilter)
{
    std::vector<TChunkSpec> chunkSpecs;
    for (const auto& inputSpec : SchedulerJobSpec_.input_specs()) {
        chunkSpecs.insert(
            chunkSpecs.end(),
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());
    }

    auto options = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpec_.input_specs(0).table_reader_options()));

    return CreateTableReader(options, chunkSpecs, std::move(nameTable), columnFilter, isParallel);
}

ISchemalessMultiChunkReaderPtr TUserJobIOBase::CreateTableReader(
    NTableClient::TTableReaderOptionsPtr options,
    const std::vector<TChunkSpec>& chunkSpecs,
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
            chunkSpecs,
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
            chunkSpecs,
            std::move(nameTable),
            columnFilter);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
