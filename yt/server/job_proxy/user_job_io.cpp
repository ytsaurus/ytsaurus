#include "stdafx.h"
#include "config.h"
#include "user_job_io.h"
#include "map_job_io.h"
#include "stderr_output.h"
#include "job.h"

#include <core/ytree/convert.h>

#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>
#include <ytlib/chunk_client/old_multi_chunk_parallel_reader.h>
#include <ytlib/chunk_client/schema.h>

#include <ytlib/table_client/table_chunk_writer.h>
#include <ytlib/table_client/sync_writer.h>

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

TUserJobIO::TUserJobIO(
    TJobIOConfigPtr ioConfig,
    IJobHost* host)
    : IOConfig(ioConfig)
    , Host(host)
    , JobSpec(host->GetJobSpec())
    , SchedulerJobSpecExt(JobSpec.GetExtension(TSchedulerJobSpecExt::scheduler_job_spec_ext))
    , Logger(JobProxyLogger)
{ }

TUserJobIO::~TUserJobIO()
{ }

int TUserJobIO::GetInputCount() const
{
    // Currently we don't support multiple inputs.
    return 1;
}

std::unique_ptr<TTableProducer> TUserJobIO::CreateTableInput(int index, IYsonConsumer* consumer)
{
    return DoCreateTableInput<TOldMultiChunkParallelReader>(index, consumer);
}

int TUserJobIO::GetOutputCount() const
{
    return SchedulerJobSpecExt.output_specs_size();
}

ISyncWriterPtr TUserJobIO::CreateTableOutput(int index)
{
    YCHECK(index >= 0 && index < GetOutputCount());

    LOG_DEBUG("Opening output %v", index);

    auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt.output_transaction_id());
    const auto& outputSpec = SchedulerJobSpecExt.output_specs(index);
    auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));
    auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
    auto writerProvider = New<TTableChunkWriterProvider>(
        IOConfig->TableWriter,
        options);

    auto asyncWriter = New<TTableChunkSequenceWriter>(
        IOConfig->TableWriter,
        options,
        writerProvider,
        Host->GetMasterChannel(),
        transactionId,
        chunkListId);

    auto writer = CreateSyncWriter<TTableChunkWriterProvider>(asyncWriter);

    {
        TGuard<TSpinLock> guard(SpinLock);
        YCHECK(Outputs.size() == index);
        Outputs.push_back(asyncWriter);
    }

    return writer;
}

double TUserJobIO::GetProgress() const
{
    TGuard<TSpinLock> guard(SpinLock);

    i64 total = 0;
    i64 current = 0;

    for (const auto& input : Inputs) {
        total += input->GetSessionRowCount();
        current += input->GetSessionRowIndex();
    }

    if (total == 0) {
        LOG_WARNING("GetProgress: empty total");
        return 0;
    } else {
        double progress = (double) current / total;
        LOG_DEBUG("GetProgress: %lf", progress);
        return progress;
    }
}

TDataStatistics TUserJobIO::GetInputDataStatistics() const
{
    TGuard<TSpinLock> guard(SpinLock);

    TDataStatistics statistics = ZeroDataStatistics();
    for (const auto& input : Inputs) {
        statistics += input->GetDataStatistics();
    }
    return statistics;
}

TDataStatistics TUserJobIO::GetOutputDataStatistics() const
{
    TGuard<TSpinLock> guard(SpinLock);

    TDataStatistics statistics = ZeroDataStatistics();
    for (const auto& output : Outputs) {
        statistics += output->GetProvider()->GetDataStatistics();
    }    
    return statistics;
}

std::unique_ptr<TErrorOutput> TUserJobIO::CreateFailContextOutput(
    const TTransactionId& transactionId) const
{
    return std::unique_ptr<TErrorOutput>(new TErrorOutput(
        IOConfig->ErrorFileWriter,
        Host->GetMasterChannel(),
        transactionId));
}

std::vector<TChunkId> TUserJobIO::GetFailedChunkIds() const
{
    std::vector<TChunkId> result;
    for (const auto& input : Inputs) {
        auto part = input->GetFailedChunkIds();
        result.insert(result.end(), part.begin(), part.end());
    }
    return result;
}

void TUserJobIO::PopulateUserJobResult(TUserJobResult* result)
{
    for (const auto& output : Outputs) {
        *result->add_output_boundary_keys() = output->GetProvider()->GetOldBoundaryKeys();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

