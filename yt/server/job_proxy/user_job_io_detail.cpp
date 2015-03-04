#include "stdafx.h"

#include "user_job_io_detail.h"

#include "config.h"
#include "job.h"

#include <ytlib/new_table_client/chunk_meta_extensions.h>
#include <ytlib/new_table_client/name_table.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>
#include <ytlib/new_table_client/schemaless_chunk_writer.h>

#include <core/ytree/convert.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NJobProxy {

using namespace NConcurrency;
using namespace NYson;
using namespace NYTree;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NVersionedTableClient;
using namespace NVersionedTableClient::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TUserJobIOBase::TUserJobIOBase(IJobHost* host)
    : Host_(host)
    , SchedulerJobSpec_(Host_->GetJobSpec().GetExtension(
        TSchedulerJobSpecExt::scheduler_job_spec_ext))
    , JobIOConfig_(Host_->GetConfig()->JobIO)
    , Logger(host->GetLogger())
{ }

void TUserJobIOBase::Init()
{
    LOG_INFO("Opening writers");

    auto transactionId = FromProto<TTransactionId>(SchedulerJobSpec_.output_transaction_id());
    for (const auto& outputSpec : SchedulerJobSpec_.output_specs()) {
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        TKeyColumns keyColumns;

        if (outputSpec.has_key_columns()) {
            keyColumns = FromProto<TKeyColumns>(outputSpec.key_columns());
        }
        
        auto writer = DoCreateWriter(options, chunkListId, transactionId, keyColumns);
        // ToDo(psushin): open writers in parallel.
        auto error = WaitFor(writer->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
        Writers_.push_back(writer);
    }

    LOG_INFO("Opening readers");

    Readers_ = DoCreateReaders();
    for (const auto& reader : Readers_) {
        auto error = WaitFor(reader->Open());
        THROW_ERROR_EXCEPTION_IF_FAILED(error);
    }
}

const std::vector<ISchemalessMultiChunkWriterPtr>& TUserJobIOBase::GetWriters() const
{
    return Writers_;
}

const std::vector<ISchemalessMultiChunkReaderPtr>& TUserJobIOBase::GetReaders() const
{
    return Readers_;
}

TBoundaryKeysExt TUserJobIOBase::GetBoundaryKeys(ISchemalessMultiChunkWriterPtr writer) const
{
    static TBoundaryKeysExt emptyBoundaryKeys = EmptyBoundaryKeys();

    const auto& chunks = writer->GetWrittenChunks();
    if (!writer->IsSorted() || chunks.empty()) {
        return emptyBoundaryKeys;
    }

    TBoundaryKeysExt boundaryKeys;
    auto frontBoundaryKeys = GetProtoExtension<TBoundaryKeysExt>(chunks.front().chunk_meta().extensions());
    boundaryKeys.set_min(frontBoundaryKeys.min());
    auto backBoundaryKeys = GetProtoExtension<TBoundaryKeysExt>(chunks.back().chunk_meta().extensions());
    boundaryKeys.set_max(backBoundaryKeys.max());

    return boundaryKeys;
}

void TUserJobIOBase::PopulateResult(TSchedulerJobResultExt* schedulerJobResultExt)
{
    auto* result = schedulerJobResultExt->mutable_user_job_result();
    for (const auto& writer : Writers_) {
        *result->add_output_boundary_keys() = GetBoundaryKeys(writer);
    }
}

ISchemalessMultiChunkWriterPtr TUserJobIOBase::CreateTableWriter(
    TTableWriterOptionsPtr options,
    const TChunkListId& chunkListId,
    const TTransactionId& transactionId,
    const TKeyColumns& keyColumns)
{
    auto nameTable = TNameTable::FromKeyColumns(keyColumns);
    return CreateSchemalessMultiChunkWriter(
        JobIOConfig_->NewTableWriter,
        options,
        nameTable,
        keyColumns,
        Host_->GetMasterChannel(),
        transactionId,
        chunkListId,
        true);
}

std::vector<ISchemalessMultiChunkReaderPtr> TUserJobIOBase::CreateRegularReaders(bool isParallel)
{
    std::vector<TChunkSpec> chunkSpecs;
    for (const auto& inputSpec : SchedulerJobSpec_.input_specs()) {
        chunkSpecs.insert(
            chunkSpecs.end(),
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());
    }

    auto options = New<TMultiChunkReaderOptions>();
    auto nameTable = New<TNameTable>();

    auto reader = CreateTableReader(options, chunkSpecs, nameTable, isParallel);
    return std::vector<ISchemalessMultiChunkReaderPtr>(1, reader);
}

ISchemalessMultiChunkReaderPtr TUserJobIOBase::CreateTableReader(
    TMultiChunkReaderOptionsPtr options,
    const std::vector<TChunkSpec>& chunkSpecs, 
    TNameTablePtr nameTable,
    bool isParallel)
{
    if (isParallel) {
        return CreateSchemalessParallelMultiChunkReader(
            JobIOConfig_->NewTableReader,
            options,
            Host_->GetMasterChannel(),
            Host_->GetCompressedBlockCache(),
            Host_->GetUncompressedBlockCache(),
            Host_->GetNodeDirectory(),
            chunkSpecs,
            nameTable);
    } else {
        return CreateSchemalessSequentialMultiChunkReader(
            JobIOConfig_->NewTableReader,
            options,
            Host_->GetMasterChannel(),
            Host_->GetCompressedBlockCache(),
            Host_->GetUncompressedBlockCache(),
            Host_->GetNodeDirectory(),
            chunkSpecs,
            nameTable);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
