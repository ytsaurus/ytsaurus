#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

struct TPodSpecConfig
    : public virtual NYTree::TYsonStruct
{
    std::optional<double> CpuToVCpuFactor;

    REGISTER_YSON_STRUCT(TPodSpecConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPodSpecConfig)

////////////////////////////////////////////////////////////////////////////////

struct TCGroupConfig
    : public virtual NYTree::TYsonStruct
{
    std::vector<TString> SupportedCGroups;

    bool IsCGroupSupported(const TString& cgroupType) const;

    REGISTER_YSON_STRUCT(TCGroupConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

struct TPortoExecutorDynamicConfig
    : public NYTree::TYsonStruct
{
    TDuration RetriesTimeout;
    TDuration PollPeriod;
    TDuration ApiTimeout;
    TDuration ApiDiskTimeout;
    bool EnableNetworkIsolation;
    bool EnableTestPortoFailures;
    bool EnableTestPortoNotResponding;

    EPortoErrorCode StubErrorCode;

    REGISTER_YSON_STRUCT(TPortoExecutorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPortoExecutorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
