#pragma once

#include "workload.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TWorkloadConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TWorkloadDescriptor WorkloadDescriptor;

    TWorkloadConfig()
    {
        RegisterParameter("workload_descriptor", WorkloadDescriptor)
            .Default(TWorkloadDescriptor(EWorkloadCategory::UserBatch));
    }
};

DEFINE_REFCOUNTED_TYPE(TWorkloadConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
