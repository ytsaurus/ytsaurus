#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDiskManager {

////////////////////////////////////////////////////////////////////////////////

struct TMockedDiskConfig
    : public NYTree::TYsonStruct
{
    TString DiskId;
    TString DevicePath;
    TString DeviceName;
    TString DiskModel;
    std::vector<TString> PartitionFsLabels;
    EDiskState State;

    REGISTER_YSON_STRUCT(TMockedDiskConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMockedDiskConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskManagerProxyConfig
    : public NYTree::TYsonStruct
{
    TString DiskManagerAddress;
    TString DiskManagerServiceName;

    TDuration RequestTimeout;

    REGISTER_YSON_STRUCT(TDiskManagerProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDiskManagerProxyConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDiskInfoProviderConfig
    : public NYTree::TYsonStruct
{
    std::vector<TString> DiskIds;
    TString YtDiskPrefix;

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

} // namespace NYT::NDiskManager
