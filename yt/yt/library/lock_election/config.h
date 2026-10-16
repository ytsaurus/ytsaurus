#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NLockElection {

////////////////////////////////////////////////////////////////////////////////

struct TLockElectionManagerConfig
    : public virtual NYTree::TYsonStruct
{
    TDuration LockAcquisitionPeriod;

    REGISTER_YSON_STRUCT(TLockElectionManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLockElectionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLockElection
