#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>
#include <ytlib/job_proxy/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TSchedulerConfig
    : public TConfigurable
{
    TDuration TransactionsRefreshPeriod;
    TDuration NodesRefreshPeriod;
    ESchedulerStrategy Strategy;

    TSchedulerConfig()
    {
        Register("transactions_refresh_period", TransactionsRefreshPeriod)
            .Default(TDuration::Seconds(15));
        Register("nodes_refresh_period", NodesRefreshPeriod)
            .Default(TDuration::Seconds(15));
        Register("strategy", Strategy)
            .Default(ESchedulerStrategy::Null);
    }
};

struct TOperationSpecBase
    : public TConfigurable
{
    TNullable<int> JobCount;
    NJobProxy::TJobIoConfigPtr JobIOConfig;

    TOperationSpecBase()
    {
        SetKeepOptions(true);
        // TODO(babenko): validate > 0
        Register("job_count", JobCount)
            .Default();
        Register("job_io_config", JobIOConfig)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
