#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TPartitionJob
    : public IJob
{
public:
    TPartitionJob(
        TJobIOConfigPtr ioConfig,
        NElection::TLeaderLookup::TConfigPtr masterConfig,
        const NScheduler::NProto::TPartitionJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    NTableClient::TChunkSequenceReaderPtr Reader;
    NTableClient::TPartitionChunkSequenceWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT