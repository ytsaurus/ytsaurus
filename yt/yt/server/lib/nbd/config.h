#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFilesystemType,
    ((Unknown)      (1)     ("unknown"))
    ((Ext3)         (2)     ("ext3"))
    ((Ext4)         (3)     ("ext4"))
);

////////////////////////////////////////////////////////////////////////////////

struct TChunkBlockDeviceConfig
    : public NYTree::TYsonStruct
{
public:
    i64 Size;
    int MediumIndex;
    EFilesystemType FsType;
    TDuration KeepSessionAlivePeriod;
    TDuration DataNodeNbdServiceRpcTimeout;
    //! Time to create chunk and make filesystem in it.
    TDuration DataNodeNbdServiceMakeTimeout;

    REGISTER_YSON_STRUCT(TChunkBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemBlockDeviceConfig
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TFileSystemBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileSystemBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryBlockDeviceConfig
    : public NYTree::TYsonStruct
{
    i64 Size;

    REGISTER_YSON_STRUCT(TMemoryBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableBlockDeviceConfig
    : public NYTree::TYsonStruct
{
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
struct TIdsConfig
    : public NYTree::TYsonStruct
{
    int Port;
    int MaxBacklogSize;

    REGISTER_YSON_STRUCT(TIdsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIdsConfig)

////////////////////////////////////////////////////////////////////////////////

//! Unix Domain Socket config
struct TUdsConfig
    : public NYTree::TYsonStruct
{
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

struct TNbdServerConfig
    : public NYTree::TYsonStruct
{
    TIdsConfigPtr InternetDomainSocket;
    TUdsConfigPtr UnixDomainSocket;
    int ThreadCount;
    // For testing purposes.
    TNbdTestOptionsPtr TestOptions;

    REGISTER_YSON_STRUCT(TNbdServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
