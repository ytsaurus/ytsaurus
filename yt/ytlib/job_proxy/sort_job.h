#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TSortJob
    : public IJob
{
public:
    TSortJob(
        const TJobIOConfigPtr& ioConfig,
        const NElection::TLeaderLookup::TConfigPtr& masterConfig,
        const NScheduler::NProto::TSortJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    TJobIOConfigPtr IOConfig;
    NElection::TLeaderLookup::TConfigPtr MasterConfig;
    NScheduler::NProto::TSortJobSpec JobSpec;

    NTableClient::TKeyColumns KeyColumns;
    NTableClient::TChunkSequenceReaderPtr Reader;
    NTableClient::TChunkSequenceWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
