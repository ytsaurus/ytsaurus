#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

class TFileSystemBlockDeviceConfig
    : public NYTree::TYsonStruct
{
public:
    REGISTER_YSON_STRUCT(TFileSystemBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileSystemBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

class TMemoryBlockDeviceConfig
    : public NYTree::TYsonStruct
{
public:
    i64 Size;

    REGISTER_YSON_STRUCT(TMemoryBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

class TDynamicTableBlockDeviceConfig
    : public NYTree::TYsonStruct
{
public:
    i64 Size;
    i64 BlockSize;
    i64 ReadBatchSize;
    i64 WriteBatchSize;
    NYPath::TYPath TablePath;

    REGISTER_YSON_STRUCT(TDynamicTableBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TDynamicTableBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

//! Internet Domain Socket config
class TIdsConfig
    : public NYTree::TYsonStruct
{
public:
    int Port;
    int MaxBacklogSize;

    REGISTER_YSON_STRUCT(TIdsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIdsConfig)

////////////////////////////////////////////////////////////////////////////////

//! Unix Domain Socket config
class TUdsConfig
    : public NYTree::TYsonStruct
{
public:
    TString Path;
    int MaxBacklogSize;

    REGISTER_YSON_STRUCT(TUdsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUdsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TNbdTestOptions
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> BlockDeviceSleepBeforeRead;
    std::optional<TDuration> BlockDeviceSleepBeforeWrite;
    bool SetBlockDeviceErrorOnRead;
    bool SetBlockDeviceErrorOnWrite;
    bool AbortConnectionOnRead;
    bool AbortConnectionOnWrite;

    REGISTER_YSON_STRUCT(TNbdTestOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdTestOptions)

////////////////////////////////////////////////////////////////////////////////

class TNbdServerConfig
    : public NYTree::TYsonStruct
{
public:
    TIdsConfigPtr InternetDomainSocket;
    TUdsConfigPtr UnixDomainSocket;

    int ThreadCount;
    i64 BlockCacheCompressedDataCapacity;

    // For testing purposes.
    TNbdTestOptionsPtr TestOptions;

    REGISTER_YSON_STRUCT(TNbdServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
