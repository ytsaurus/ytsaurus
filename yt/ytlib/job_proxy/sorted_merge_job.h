#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeJob
    : public IJob
{
public:
    TSortedMergeJob(
        TJobIOConfigPtr ioConfig,
        NElection::TLeaderLookup::TConfigPtr masterConfig,
        const NScheduler::NProto::TMergeJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    std::vector<NTableClient::TChunkReaderPtr> ChunkReaders;
    NTableClient::ISyncWriterPtr Writer;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT