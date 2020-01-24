#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/ypath/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

// Config shared by classes that aggregate pod spec.
class TPodSpecValidationConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    ui64 MinVcpuGuarantee;

    TPodSpecValidationConfig()
    {
        RegisterParameter("min_vcpu_guarantee", MinVcpuGuarantee)
            .Default(100);
    }
};

DEFINE_REFCOUNTED_TYPE(TPodSpecValidationConfig)

////////////////////////////////////////////////////////////////////////////////

class TPodTypeHandlerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    ui64 DefaultVcpuGuarantee;
    ui64 DefaultMemoryGuarantee;
    ui64 DefaultSlot;
    TPodSpecValidationConfigPtr SpecValidation;

    TPodTypeHandlerConfig()
    {
        RegisterParameter("default_vcpu_guarantee", DefaultVcpuGuarantee)
            .Default(1000);
        RegisterParameter("default_memory_guarantee", DefaultMemoryGuarantee)
            .Default(100_MB);
        RegisterParameter("default_slot", DefaultSlot)
            .Default(1);
        RegisterParameter("spec_validation", SpecValidation)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TPodTypeHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPodSetTypeHandlerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool AllowNodeSegmentChange;

    TPodSetTypeHandlerConfig()
    {
        RegisterParameter("allow_node_segment_change", AllowNodeSegmentChange)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TPodSetTypeHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TPodVcpuGuaranteeToLimitRatioConstraintConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    double MaxMultiplier;
    ui64 MaxAdditive;

    TPodVcpuGuaranteeToLimitRatioConstraintConfig()
    {
        RegisterParameter("max_multiplier", MaxMultiplier)
            .GreaterThanOrEqual(1.0)
            .Default(1000.0);
        RegisterParameter("max_additive", MaxAdditive)
            .Default(1000000);
    }
};

DEFINE_REFCOUNTED_TYPE(TPodVcpuGuaranteeToLimitRatioConstraintConfig)

////////////////////////////////////////////////////////////////////////////////

class TNodeSegmentTypeHandlerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    TPodVcpuGuaranteeToLimitRatioConstraintConfigPtr PodVcpuGuaranteeToLimitRatioConstraint;

    TNodeSegmentTypeHandlerConfig()
    {
        RegisterParameter("pod_vcpu_guarantee_to_limit_ratio_constraint", PodVcpuGuaranteeToLimitRatioConstraint)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TNodeSegmentTypeHandlerConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    // Sweep.
    TDuration RemovedObjectsSweepPeriod;
    TDuration RemovedObjectsGraceTimeout;
    int RemovedObjectsDropBatchSize;

    // Type handlers.
    TPodTypeHandlerConfigPtr PodTypeHandler;
    TPodSetTypeHandlerConfigPtr PodSetTypeHandler;
    TNodeSegmentTypeHandlerConfigPtr NodeSegmentTypeHandler;

    // Extensible attributes.
    bool EnableExtensibleAttributes;

    // History.
    bool EnableHistory;
    THashSet<EObjectType> HistoryDisabledTypes;

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
        RegisterParameter("pod_set_type_handler", PodSetTypeHandler)
            .DefaultNew();
        RegisterParameter("node_segment_type_handler", NodeSegmentTypeHandler)
            .DefaultNew();

        RegisterParameter("enable_extensible_attributes", EnableExtensibleAttributes)
            .Default(false);

        RegisterParameter("enable_history", EnableHistory)
            .Default(true);
        RegisterParameter("history_disabled_types", HistoryDisabledTypes)
            .Default();
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

class TWatchManagerConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    bool Enabled;
    TDuration TabletInfosPollInterval;
    TDuration MaxWaitForTabletInfosTimeLimit;

    TWatchManagerConfig()
    {
        RegisterParameter("enabled", Enabled)
            .Default(true);
        RegisterParameter("tablet_infos_poll_interval", TabletInfosPollInterval)
            .Default(TDuration::MilliSeconds(50));
        RegisterParameter("max_wait_for_tablet_infos_time_limit", MaxWaitForTabletInfosTimeLimit)
            .Default(TDuration::Seconds(30));
    }
};

DEFINE_REFCOUNTED_TYPE(TWatchManagerConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
