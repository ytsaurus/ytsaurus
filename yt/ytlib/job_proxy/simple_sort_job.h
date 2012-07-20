#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/election/leader_lookup.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TSimpleSortJob
    : public IJob
{
public:
    TSimpleSortJob(
        TJobProxyConfigPtr proxyConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    NTableClient::TKeyColumns KeyColumns;
    NTableClient::TChunkSequenceReaderPtr Reader;

    NTableClient::TTableChunkSequenceWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
