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

DEFINE_ENUM_UNKNOWN_VALUE(EFilesystemType, Unknown);

////////////////////////////////////////////////////////////////////////////////

struct TBlockDeviceConfigBase
    : public NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TBlockDeviceConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlockDeviceConfigBase)

////////////////////////////////////////////////////////////////////////////////

//! Configuration for the write-back page cache of TChunkBlockDevice.
struct TPageCacheConfig
    : public NYTree::TYsonStruct
{
    //! Page size in bytes.
    //! Must be a positive multiple of 4096.
    i64 PageSize;

    //! Total size of the page cache in bytes.
    //! Must be a positive multiple of PageSize.
    i64 Size;

    //! Period for background flush of dirty pages.
    //! When not set (nullopt), periodic flush is disabled.
    std::optional<TDuration> FlushPeriod;

    //! Maximum number of concurrent Write RPCs issued during a single flush.
    //! Dirty pages are flushed in batches of this size to avoid overwhelming
    //! the data node's request queue.
    int FlushBatchSize;

    REGISTER_YSON_STRUCT(TPageCacheConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TPageCacheConfig)

////////////////////////////////////////////////////////////////////////////////

struct TChunkBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    i64 Size;
    int MediumIndex;
    EFilesystemType FsType;
    TDuration KeepSessionAlivePeriod;
    TDuration DataNodeNbdServiceRpcTimeout;
    //! Time to create chunk and make filesystem in it.
    TDuration DataNodeNbdServiceMakeTimeout;
    //! Number of TCP connections to use for NBD RPC requests.
    int MultiplexingParallelism;

    //! Write-back page cache configuration.
    //! When null, page cache is disabled.
    TPageCacheConfigPtr PageCache;

    REGISTER_YSON_STRUCT(TChunkBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TChunkBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    REGISTER_YSON_STRUCT(TFileSystemBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileSystemBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TMemoryBlockDeviceConfig
    : public TBlockDeviceConfigBase
{
    i64 Size;

    REGISTER_YSON_STRUCT(TMemoryBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMemoryBlockDeviceConfig)

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableBlockDeviceConfig
    : public TBlockDeviceConfigBase
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
    std::string Path;
    int MaxBacklogSize;

    REGISTER_YSON_STRUCT(TUdsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUdsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TNbdTestOptions
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> SleepOnRead;
    std::optional<TDuration> SleepOnWrite;
    bool SetErrorOnRead;
    bool SetErrorOnWrite;
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
