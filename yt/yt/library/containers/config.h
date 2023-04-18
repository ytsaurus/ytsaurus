#pragma once

#include "public.h"
#include "porto_executor.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

class TPodSpecConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::optional<double> CpuToVCpuFactor;

    REGISTER_YSON_STRUCT(TPodSpecConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPodSpecConfig)

////////////////////////////////////////////////////////////////////////////////

class TCGroupConfig
    : public virtual NYTree::TYsonStruct
{
public:
    std::vector<TString> SupportedCGroups;

    bool IsCGroupSupported(const TString& cgroupType) const;

    REGISTER_YSON_STRUCT(TCGroupConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TPortoExecutorConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration RetriesTimeout;
    TDuration PollPeriod;
    TDuration ApiTimeout;
    TDuration ApiDiskTimeout;
    bool EnableNetworkIsolation;
    bool EnableTestPortoFailures;

    EPortoErrorCode StubErrorCode;

    REGISTER_YSON_STRUCT(TPortoExecutorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPortoExecutorConfig)

////////////////////////////////////////////////////////////////////////////////

class TPortoExecutorDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<bool> EnableTestPortoFailures;
    std::optional<EPortoErrorCode> StubErrorCode;

    REGISTER_YSON_STRUCT(TPortoExecutorDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPortoExecutorDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
