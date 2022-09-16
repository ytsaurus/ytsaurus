#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticationManagerConfig
    : public NYTree::TYsonStruct
{
public:
    //! Native TVM service config. If it's null, then no native TVM service is used.
    TTvmServiceConfigPtr TvmService;

    //! If true, then service ticket verification is mandatory for native authenticator.
    bool EnableValidation;

    REGISTER_YSON_STRUCT(TNativeAuthenticationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeAuthenticationManagerConfig);

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticationManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<bool> EnableValidation;

    REGISTER_YSON_STRUCT(TNativeAuthenticationManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeAuthenticationManagerDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
