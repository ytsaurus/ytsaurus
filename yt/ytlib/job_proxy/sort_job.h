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
        const TJobIOConfigPtr& config,
        const NElection::TLeaderLookup::TConfigPtr& masterConfig,
        const NScheduler::NProto::TSortJobSpec& sortJobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    NTableClient::TChunkSequenceReaderPtr Reader;
    NTableClient::TChunkSequenceWriterPtr Writer;

    NTableClient::TKeyColumns KeyColumns;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
