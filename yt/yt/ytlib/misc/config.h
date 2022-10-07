#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TNodeMemoryReferenceTrackerConfig
    : public NYTree::TYsonStruct
{
public:
    bool EnableMemoryReferenceTracker;

    REGISTER_YSON_STRUCT(TNodeMemoryReferenceTrackerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNodeMemoryReferenceTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
