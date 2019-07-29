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

class TDiscoveryConfig
    : public NYTree::TYsonSerializable
{
public:
    NYPath::TYPath Directory;
    TDuration UpdatePeriod;
    TDuration BanTimeout;

    TDiscoveryConfig()
    {
        RegisterParameter("directory", Directory);
        RegisterParameter("update_period", UpdatePeriod)
            .Default(TDuration::Seconds(30));
        RegisterParameter("ban_timeout", BanTimeout)
            .Default(TDuration::Seconds(31));
    }
};

DEFINE_REFCOUNTED_TYPE(TDiscoveryConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
