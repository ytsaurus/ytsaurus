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

    TCypressElectionManagerConfig();
};

DEFINE_REFCOUNTED_TYPE(TCypressElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressElection
