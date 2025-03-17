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
    TString Medium;
    EFilesystemType FsType;
    TDuration KeepSessionAlivePeriod;
    // Address of data node.
    std::optional<TString> Address;
    TDuration DataNodeNbdServiceRpcTimeout;
    // Time to create chunk and make filesystem in it.
    TDuration DataNodeNbdServiceMakeTimeout;

    REGISTER_YSON_STRUCT(TChunkBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemBlockDeviceConfig
    : public NYTree::TYsonStruct
{
    // For testing purposes: how long to sleep before read request
    TDuration TestSleepBeforeRead;

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

struct TNbdServerConfig
    : public NYTree::TYsonStruct
{
    TIdsConfigPtr InternetDomainSocket;
    TUdsConfigPtr UnixDomainSocket;
    // For testing purposes: how long to sleep before read request
    TDuration TestBlockDeviceSleepBeforeRead;
    // For testing purposes: abort connection on read request
    bool TestAbortConnectionOnRead;

    int ThreadCount;

    REGISTER_YSON_STRUCT(TNbdServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
