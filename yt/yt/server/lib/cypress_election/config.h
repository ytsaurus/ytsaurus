#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NCypressElection {

////////////////////////////////////////////////////////////////////////////////

struct TCypressElectionManagerConfig
    : public NYTree::TYsonSerializable
{
    NYPath::TYPath LockPath;

    TDuration TransactionTimeout;
    TDuration TransactionPingPeriod;
    TDuration LockAcquisitionPeriod;

    TCypressElectionManagerConfig()
    {
        RegisterParameter("lock_path", LockPath)
            .Default();

        RegisterParameter("transaction_timeout", TransactionTimeout)
            .Default(TDuration::Minutes(1));
        RegisterParameter("transaction_ping_period", TransactionPingPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("lock_acquisition_period", LockAcquisitionPeriod)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TCypressElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
