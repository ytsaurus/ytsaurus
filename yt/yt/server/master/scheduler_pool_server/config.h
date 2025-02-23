#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NSchedulerPoolServer {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicSchedulerPoolManagerConfig
    : public NYTree::TYsonStruct
{
    int MaxSchedulerPoolSubtreeSize;

    // Pool name validation regex for user with |Administer| permission on scheduler pool schema object.
    std::string PoolNameRegexForAdministrators;

    // Pool name validation regex for all others.
    std::string PoolNameRegexForUsers;

    REGISTER_YSON_STRUCT(TDynamicSchedulerPoolManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicSchedulerPoolManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
