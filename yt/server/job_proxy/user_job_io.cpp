#include "stdafx.h"
#include "config.h"
#include "user_job_io.h"
#include "map_job_io.h"
#include "stderr_output.h"

#include <ytlib/meta_state/config.h>

#include <ytlib/table_client/multi_chunk_parallel_reader.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/schema.h>

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;
using namespace NScheduler;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TUserJobIO::TUserJobIO(
    TJobIOConfigPtr ioConfig,
    NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
    : IOConfig(ioConfig)
    , MasterChannel(CreateLeaderChannel(mastersConfig))
    , JobSpec(jobSpec)
    , Logger(JobProxyLogger)
{ }

TUserJobIO::~TUserJobIO()
{ }

int TUserJobIO::GetInputCount() const
{
    // Currently we don't support multiple inputs.
    return 1;
}

TAutoPtr<TTableProducer> TUserJobIO::CreateTableInput(int index, NYson::IYsonConsumer* consumer)
{
    return DoCreateTableInput<TMultiChunkParallelReader>(index, consumer);
}

int TUserJobIO::GetOutputCount() const
{
    return JobSpec.output_specs_size();
}

ISyncWriterPtr TUserJobIO::CreateTableOutput(int index)
{
    YCHECK(index >= 0 && index < GetOutputCount());

    LOG_DEBUG("Opening output %d", index);

    auto transactionId = TTransactionId::FromProto(JobSpec.output_transaction_id());
    const auto& outputSpec = JobSpec.output_specs(index);
    auto account = outputSpec.account();
    auto chunkListId = TChunkListId::FromProto(outputSpec.chunk_list_id());
    auto channels = ConvertTo<TChannels>(TYsonString(outputSpec.channels()));
    auto keyColumns = FromProto<Stroka>(outputSpec.key_columns());
    auto chunkSequenceWriter = New<TTableChunkSequenceWriter>(
        IOConfig->TableWriter,
        MasterChannel,
        transactionId,
        account,
        chunkListId,
        channels,
        keyColumns.empty() ? Null : MakeNullable(keyColumns));

    auto syncWriter = CreateSyncWriter(chunkSequenceWriter);

    YCHECK(Outputs.size() == index);
    // NB: Save reader before opening! Otherwise failed chunks may not be collected.
    Outputs.push_back(chunkSequenceWriter);

    syncWriter->Open();

    return syncWriter;
}

double TUserJobIO::GetProgress() const
{
    i64 total = 0;
    i64 current = 0;

    FOREACH (const auto& input, Inputs) {
        total += input->GetRowCount();
        current += input->GetRowIndex();
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

TAutoPtr<TErrorOutput> TUserJobIO::CreateErrorOutput(const TTransactionId& transactionId) const
{
    return new TErrorOutput(
        IOConfig->ErrorFileWriter,
        MasterChannel,
        transactionId);
}

void TUserJobIO::SetStderrChunkId(const TChunkId& chunkId)
{
    YCHECK(chunkId != NullChunkId);
    StderrChunkId = chunkId;
}

std::vector<NChunkClient::TChunkId> TUserJobIO::GetFailedChunks() const
{
    std::vector<NChunkClient::TChunkId> result;
    FOREACH(const auto& input, Inputs) {
        auto part = input->GetFailedChunks();
        result.insert(result.end(), part.begin(), part.end());
    }
    return result;
}

void TUserJobIO::PopulateUserJobResult(TUserJobResult* result)
{
    if (StderrChunkId != NullChunkId) {
        *result->mutable_stderr_chunk_id() = StderrChunkId.ToProto();
    }

    FOREACH (const auto& writer, Outputs) {
        *result->add_output_boundary_keys() = writer->GetBoundaryKeys();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

