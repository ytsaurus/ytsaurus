#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TPodTypeHandlerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    ui64 MinVcpuGuarantee;
    ui64 DefaultVcpuGuarantee;
    ui64 DefaultMemoryGuarantee;

    TPodTypeHandlerConfig()
    {
        RegisterParameter("min_vcpu_guarantee", MinVcpuGuarantee)
                .Default(100);
        RegisterParameter("default_vcpu_guarantee", DefaultVcpuGuarantee)
                .Default(1000);
        RegisterParameter("default_memory_guarantee", DefaultMemoryGuarantee)
                .Default(100_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TPodTypeHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TDuration RemovedObjectsSweepPeriod;
    TDuration RemovedObjectsGraceTimeout;
    int RemovedObjectsDropBatchSize;
    TPodTypeHandlerConfigPtr PodTypeHandler;
    bool EnableExtensibleAttributes;

    TObjectManagerConfig()
    {
        RegisterParameter("removed_objects_sweep_period", RemovedObjectsSweepPeriod)
            .Default(TDuration::Minutes(10));
        RegisterParameter("removed_objects_grace_timeout", RemovedObjectsGraceTimeout)
            .Default(TDuration::Hours(24));
        RegisterParameter("removed_objects_drop_batch_size", RemovedObjectsDropBatchSize)
            .GreaterThanOrEqual(1)
            .Default(50000);
        RegisterParameter("pod_type_handler", PodTypeHandler)
            .DefaultNew();
        RegisterParameter("enable_extensible_attributes", EnableExtensibleAttributes)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TObjectManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    i64 InputRowLimit;
    i64 OutputRowLimit;
    int MaxKeysPerLookupRequest;

    TTransactionManagerConfig()
    {
        RegisterParameter("input_row_limit", InputRowLimit)
            .Default(10000000);
        RegisterParameter("output_row_limit", OutputRowLimit)
            .Default(10000000);
        RegisterParameter("max_keys_per_lookup_request", MaxKeysPerLookupRequest)
            .GreaterThan(0)
            .Default(100);
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
