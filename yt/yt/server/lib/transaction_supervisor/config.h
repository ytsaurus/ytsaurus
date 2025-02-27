#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionSupervisorConfig
    : public NYTree::TYsonStruct
{
    TDuration ParticipantProbationPeriod;
    TDuration RpcTimeout;
    TDuration ParticipantBackoffTime;

    bool EnableWaitUntilPreparedTransactionsFinished;

    REGISTER_YSON_STRUCT(TTransactionSupervisorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTransactionLeaseTrackerConfig
    : public NYTree::TYsonStruct
{
    int ThreadCount;

    REGISTER_YSON_STRUCT(TTransactionLeaseTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionLeaseTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
