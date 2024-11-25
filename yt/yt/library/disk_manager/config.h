#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

struct TDiskManagerProxyConfig
    : public NYTree::TYsonStruct
{
    std::string DiskManagerAddress;
    std::string DiskManagerServiceName;

    TDuration RequestTimeout;

    REGISTER_YSON_STRUCT(TDiskManagerProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskInfoProviderConfig
    : public NYTree::TYsonStruct
{
    std::vector<std::string> DiskIds;
    std::string YTDiskPrefix;

    REGISTER_YSON_STRUCT(TDiskInfoProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskInfoProviderConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskManagerProxyDynamicConfig
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> RequestTimeout;

    REGISTER_YSON_STRUCT(TDiskManagerProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct THotswapManagerConfig
    : public NYTree::TYsonStruct
{
    TDiskManagerProxyConfigPtr DiskManagerProxy;
    TDiskInfoProviderConfigPtr DiskInfoProvider;

    REGISTER_YSON_STRUCT(THotswapManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THotswapManagerConfig)

////////////////////////////////////////////////////////////////////////////////

struct THotswapManagerDynamicConfig
    : public NYTree::TYsonStruct
{
    TDiskManagerProxyDynamicConfigPtr DiskManagerProxy;

    REGISTER_YSON_STRUCT(THotswapManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(THotswapManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiskManager
