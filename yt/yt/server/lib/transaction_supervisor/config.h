#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisorConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration ParticipantProbationPeriod;
    TDuration RpcTimeout;
    TDuration ParticipantBackoffTime;

    TTransactionSupervisorConfig()
    {
        RegisterParameter("participant_probation_period", ParticipantProbationPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(5));
        RegisterParameter("participant_backoff_time", ParticipantBackoffTime)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
