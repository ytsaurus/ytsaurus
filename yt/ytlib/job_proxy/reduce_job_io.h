#pragma once

#include "public.h"
#include "user_job_io.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TReduceJobIO
    : public TUserJobIO
{
public:
    TReduceJobIO(
        TJobIOConfigPtr config,
        NMetaState::TMasterDiscoveryConfigPtr mastersConfig,
        const NScheduler::NProto::TJobSpec& jobSpec);

    TAutoPtr<NTableClient::TTableProducer> CreateTableInput(
        int index, 
        NYTree::IYsonConsumer* consumer) const;
};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
