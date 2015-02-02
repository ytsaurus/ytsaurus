#include "stdafx.h"

#include "user_job_io_detail.h"

#include "config.h"
#include "job.h"

#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>
#include <ytlib/chunk_client/old_multi_chunk_parallel_reader.h>
#include <ytlib/chunk_client/schema.h>

#include <ytlib/table_client/table_chunk_reader.h>
#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>

#include <core/ytree/convert.h>

namespace NYT {
namespace NJobProxy {

using namespace NYson;
using namespace NYTree;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTableClient;
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
        
        auto writer = DoCreateWriter(options, chunkListId, transactionId);
        Writers_.push_back(writer);
    }

    LOG_INFO("Opening readers");

    Readers_ = DoCreateReaders();
    for (const auto& reader : Readers_) {
        reader->Open();
    }
}

const std::vector<ISyncWriterUnsafePtr>& TUserJobIOBase::GetWriters() const
{
    return Writers_;
}

const std::vector<ISyncReaderPtr>& TUserJobIOBase::GetReaders() const
{
    return Readers_;
}

void TUserJobIOBase::PopulateResult(TSchedulerJobResultExt* schedulerJobResultExt)
{
    auto* result = schedulerJobResultExt->mutable_user_job_result();
    for (const auto& writer : Writers_) {
        *result->add_output_boundary_keys() = writer->GetOldBoundaryKeys();
    }
}

ISyncWriterUnsafePtr TUserJobIOBase::CreateTableWriter(
    NTableClient::TTableWriterOptionsPtr options,
    const NChunkClient::TChunkListId& chunkListId,
    const NTransactionClient::TTransactionId& transactionId)
{
    auto writerProvider = New<TTableChunkWriterProvider>(
        JobIOConfig_->TableWriter,
        options);

    auto asyncWriter = New<TTableChunkSequenceWriter>(
        JobIOConfig_->TableWriter,
        options,
        writerProvider,
        Host_->GetMasterChannel(),
        transactionId,
        chunkListId);

    return CreateSyncWriter<TTableChunkWriterProvider>(asyncWriter);
}

std::vector<ISyncReaderPtr> TUserJobIOBase::CreateRegularReaders(bool isParallel)
{
    std::vector<TChunkSpec> chunkSpecs;
    for (const auto& inputSpec : SchedulerJobSpec_.input_specs()) {
        chunkSpecs.insert(
            chunkSpecs.end(),
            inputSpec.chunks().begin(),
            inputSpec.chunks().end());
    }

    auto options = New<TChunkReaderOptions>();

    auto reader = CreateTableReader(options, chunkSpecs, isParallel);
    return std::vector<ISyncReaderPtr>(1, reader);
}

ISyncReaderPtr TUserJobIOBase::CreateTableReader(
    TChunkReaderOptionsPtr options,
    std::vector<TChunkSpec>& chunkSpecs, 
    bool isParallel)
{
    auto provider = New<TTableChunkReaderProvider>(
        chunkSpecs,
        JobIOConfig_->TableReader,
        Host_->GetUncompressedBlockCache());

    if (isParallel) {
        auto reader = New<TTableChunkParallelReader>(
            JobIOConfig_->TableReader,
            Host_->GetMasterChannel(),
            Host_->GetCompressedBlockCache(),
            Host_->GetNodeDirectory(),
            std::move(chunkSpecs),
            provider);

        return CreateSyncReader(reader);
    } else {
        auto reader = New<TTableChunkParallelReader>(
            JobIOConfig_->TableReader,
            Host_->GetMasterChannel(),
            Host_->GetCompressedBlockCache(),
            Host_->GetNodeDirectory(),
            std::move(chunkSpecs),
            provider);

        return CreateSyncReader(reader);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
