#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NNbd {

////////////////////////////////////////////////////////////////////////////////

class TCypressFileBlockDeviceConfig
    : public NYTree::TYsonStruct
{
public:
    TString Path;

    REGISTER_YSON_STRUCT(TCypressFileBlockDeviceConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressFileBlockDeviceConfig)

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

    REGISTER_YSON_STRUCT(TUdsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TUdsConfig)

////////////////////////////////////////////////////////////////////////////////

class TNbdServerConfig
    : public NYTree::TYsonStruct
{
public:
    TIdsConfigPtr InternetDomainSocket;
    TUdsConfigPtr UnixDomainSocket;

    REGISTER_YSON_STRUCT(TNbdServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNbdServerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
