#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TOrderedMergeJob
    : public IJob
{
public:
    TOrderedMergeJob(
        TJobProxyConfigPtr proxyConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    NScheduler::NProto::TJobResult Run();

private:
    NTableClient::ISyncReaderPtr Reader;
    NTableClient::TTableChunkSequenceWriterPtr Writer;

    TNullable<NTableClient::TKeyColumns> KeyColumns;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYTNYT