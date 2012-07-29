#pragma once

#include "private.h"
#include "public.h"

#include "user_job_io.h"

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TMapJobIO
    : public TUserJobIO
{
public:
    TMapJobIO(
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
