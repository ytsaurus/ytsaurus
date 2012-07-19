#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/election/master_discovery.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TPartitionJob
    : public IJob
{
public:
    TPartitionJob(
        TJobProxyConfigPtr proxyConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    NTableClient::TTableChunkSequenceReaderPtr Reader;
    NTableClient::TPartitionChunkSequenceWriterPtr Writer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT