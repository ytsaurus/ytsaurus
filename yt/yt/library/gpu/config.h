#pragma once

#include "public.h"

#include <yt/yt/core/ytree/polymorphic_yson_struct.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NGpu {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EGpuInfoProviderType,
    (Base)
    (NvidiaSmi)
    (NvGpuManager)
    (GpuAgent)
);

////////////////////////////////////////////////////////////////////////////////

struct TGpuInfoProviderConfigBase
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TGpuInfoProviderConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuInfoProviderConfigBase)

////////////////////////////////////////////////////////////////////////////////

struct TNvidiaSmiGpuInfoProviderConfig
    : public TGpuInfoProviderConfigBase
{
    REGISTER_YSON_STRUCT(TNvidiaSmiGpuInfoProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNvidiaSmiGpuInfoProviderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGrpcGpuInfoProviderConfigBase
    : public TGpuInfoProviderConfigBase
{
    std::string Address;
    std::string ServiceName;
    NRpc::TRetryingChannelConfigPtr Channel;

    REGISTER_YSON_STRUCT(TGrpcGpuInfoProviderConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGrpcGpuInfoProviderConfigBase)

////////////////////////////////////////////////////////////////////////////////

// TODO: Hide this config in OS version.
struct TNvManagerGpuInfoProviderConfig
    : public TGrpcGpuInfoProviderConfigBase
{
    std::optional<TString> DevicesCgroupPath;
    bool GpuIndexesFromNvidiaSmi;

    REGISTER_YSON_STRUCT(TNvManagerGpuInfoProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNvManagerGpuInfoProviderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGpuAgentGpuInfoProviderConfig
    : public TGrpcGpuInfoProviderConfigBase
{
    REGISTER_YSON_STRUCT(TGpuAgentGpuInfoProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuAgentGpuInfoProviderConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_POLYMORPHIC_YSON_STRUCT_FOR_ENUM_WITH_DEFAULT(GpuInfoProviderConfig, EGpuInfoProviderType, NvidiaSmi,
    ((Base)         (TGpuInfoProviderConfigBase))
    ((NvidiaSmi)    (TNvidiaSmiGpuInfoProviderConfig))
    ((NvGpuManager) (TNvManagerGpuInfoProviderConfig))
    ((GpuAgent)     (TGpuAgentGpuInfoProviderConfig))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGpu
