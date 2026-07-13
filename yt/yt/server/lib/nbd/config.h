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
    : public virtual NYTree::TYsonStruct
{
    REGISTER_YSON_STRUCT(TBlockDeviceConfigBase);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TBlockDeviceConfigBase)

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

    //! When set, an HTTP server is exposed on this port: a REST API for managing devices, plus a
    //! status endpoint dumping the server orchid.
    std::optional<int> HttpPort;

    //! The listen address: the unix domain socket path or ":<port>".
    std::string GetAddress() const;

    REGISTER_YSON_STRUCT(TNbdServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
