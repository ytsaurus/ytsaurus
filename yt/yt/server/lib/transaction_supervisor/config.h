#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

class TTransactionSupervisorConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration ParticipantProbationPeriod;
    TDuration RpcTimeout;
    TDuration ParticipantBackoffTime;

    bool EnableWaitUntilPreparedTransactionsFinished;

    REGISTER_YSON_STRUCT(TTransactionSupervisorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisorConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionLeaseTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    int ThreadCount;

    REGISTER_YSON_STRUCT(TTransactionLeaseTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTransactionLeaseTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
