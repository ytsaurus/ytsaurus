#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/sync_reader.h>
#include <ytlib/table_client/sync_writer.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeJob
    : public IJob
{
public:
    TSortedMergeJob(
        const TJobIoConfigPtr& config,
        const NElection::TLeaderLookup::TConfigPtr& masterConfig,
        const NScheduler::NProto::TMergeJobSpec& mergeJobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    std::vector<NTableClient::TSyncReaderAdapter::TPtr> ChunkReaders;
    NTableClient::TSyncValidatingAdaptor::TPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT