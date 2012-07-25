#pragma once 

#include "public.h"
#include "job.h"

#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TPartitionSortJob
    : public IJob
{
public:
    TPartitionSortJob(
        TJobProxyConfigPtr proxyConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    NTableClient::TKeyColumns KeyColumns;
    NTableClient::TPartitionChunkSequenceReaderPtr Reader;
    NTableClient::TTableChunkSequenceWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
