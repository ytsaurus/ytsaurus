#include "stdafx.h"

#include "user_job_io_detail.h"

#include "config.h"
#include "job.h"

#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/schema.pb.h>

#include <ytlib/table_client/chunk_meta_extensions.h>
#include <ytlib/table_client/name_table.h>
#include <ytlib/table_client/schemaless_chunk_reader.h>
#include <ytlib/table_client/schemaless_chunk_writer.h>

#include <core/concurrency/scheduler.h>
#include <core/misc/finally.h>

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

    TFinallyGuard guard([&] () {
        Initialized_ = true;
    });

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
}

void TUserJobIOBase::InitReader(TNameTablePtr nameTable, const TColumnFilter& columnFilter)
{
    YCHECK(!Reader_);
    Reader_ = DoCreateReader(std::move(nameTable), columnFilter);
}

void TUserJobIOBase::CreateReader()
{
    LOG_INFO("Opening reader");

    InitReader(New<TNameTable>(), TColumnFilter());
    WaitFor(Reader_->Open())
        .ThrowOnError();
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

int TUserJobIOBase::GetReduceKeyColumnCount() const
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
        JobIOConfig_->TableWriter,
        std::move(options),
        std::move(nameTable),
        keyColumns,
        TOwningKey(),
        Host_->GetClient(),
        transactionId,
        chunkListId,
        true);
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

    auto options = New<TMultiChunkReaderOptions>();

    return CreateTableReader(options, chunkSpecs, std::move(nameTable), columnFilter, isParallel);
}

ISchemalessMultiChunkReaderPtr TUserJobIOBase::CreateTableReader(
    TMultiChunkReaderOptionsPtr options,
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
            Host_->GetBlockCache(),
            Host_->GetNodeDirectory(),
            chunkSpecs,
            std::move(nameTable),
            columnFilter);
    } else {
        return CreateSchemalessSequentialMultiChunkReader(
            JobIOConfig_->TableReader,
            options,
            Host_->GetClient(),
            Host_->GetBlockCache(),
            Host_->GetNodeDirectory(),
            chunkSpecs,
            std::move(nameTable),
            columnFilter);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
