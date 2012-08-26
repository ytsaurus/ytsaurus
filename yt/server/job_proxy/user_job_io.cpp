#include "stdafx.h"
#include "config.h"
#include "user_job_io.h"
#include "map_job_io.h"
#include "stderr_output.h"

#include <ytlib/meta_state/config.h>

#include <ytlib/table_client/multi_chunk_sequential_reader.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/schema.h>

#include <ytlib/meta_state/master_channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NElection;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;
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

TAutoPtr<TTableProducer> TUserJobIO::CreateTableInput(int index, IYsonConsumer* consumer) const
{
    return DoCreateTableInput<TMultiChunkSequentialReader>(index, consumer);
}

int TUserJobIO::GetOutputCount() const
{
    return JobSpec.output_specs_size();
}

ISyncWriterPtr TUserJobIO::CreateTableOutput(int index) const
{
    YCHECK(index >= 0 && index < GetOutputCount());

    LOG_DEBUG("Opening output %d", index);

    Stroka channelsString = JobSpec.output_specs(index).channels();
    YCHECK(!channelsString.empty());
    auto channels = ChannelsFromYson(TYsonString(channelsString));
    auto chunkSequenceWriter = New<TTableChunkSequenceWriter>(
        IOConfig->TableWriter,
        MasterChannel,
        TTransactionId::FromProto(JobSpec.output_transaction_id()),
        TChunkListId::FromProto(JobSpec.output_specs(index).chunk_list_id()),
        channels);

    auto syncWriter = CreateSyncWriter(chunkSequenceWriter);
    syncWriter->Open();

    return syncWriter;
}

void TUserJobIO::UpdateProgress()
{
    YUNIMPLEMENTED();
}

double TUserJobIO::GetProgress() const
{
    YUNIMPLEMENTED();
}

TAutoPtr<TErrorOutput> TUserJobIO::CreateErrorOutput() const
{
    return new TErrorOutput(
        IOConfig->ErrorFileWriter, 
        MasterChannel, 
        TTransactionId::FromProto(JobSpec.output_transaction_id()));
}

void TUserJobIO::SetStderrChunkId(const TChunkId& chunkId)
{
    YCHECK(chunkId != NullChunkId);
    StderrChunkId = chunkId;
}

void TUserJobIO::PopulateUserJobResult(TUserJobResult* result)
{
    if (StderrChunkId != NullChunkId) {
        *result->mutable_stderr_chunk_id() = StderrChunkId.ToProto();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

