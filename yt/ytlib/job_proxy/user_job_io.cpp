#include "stdafx.h"

#include "config.h"
#include "user_job_io.h"
#include "map_job_io.h"
#include "reduce_job_io.h"
#include "stderr_output.h"

#include <ytlib/meta_state/config.h>

#include <ytlib/chunk_client/client_block_cache.h>
#include <ytlib/table_client/table_chunk_sequence_writer.h>
#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/schema.h>

#include <ytlib/election/leader_channel.h>

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
    TJobIOConfigPtr config, 
    NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
    const NScheduler::NProto::TJobSpec& jobSpec)
    : Config(config)
    , MasterChannel(CreateLeaderChannel(mastersConfig))
    , JobSpec(jobSpec)
{ }

int TUserJobIO::GetInputCount() const
{
    // We don't support piped input right now.
    return 1;
}

int TUserJobIO::GetOutputCount() const
{
    return JobSpec.output_specs_size();
}

NTableClient::ISyncWriterPtr TUserJobIO::CreateTableOutput(int index) const
{
    YASSERT(index < GetOutputCount());
    Stroka channelsString = JobSpec.output_specs(index).channels();
    YASSERT(!channelsString.empty());
    const TYsonString& channels = TYsonString(channelsString);
    auto chunkSequenceWriter = New<TTableChunkSequenceWriter>(
        Config->ChunkSequenceWriter,
        MasterChannel,
        TTransactionId::FromProto(JobSpec.output_transaction_id()),
        TChunkListId::FromProto(JobSpec.output_specs(index).chunk_list_id()),
        ChannelsFromYson(channels));

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
        Config->ErrorFileWriter, 
        MasterChannel, 
        TTransactionId::FromProto(JobSpec.output_transaction_id()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

