#pragma once

#include "public.h"

#include <yt/yt/library/auth_server/config.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/backoff_strategy.h>

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

    //! If true, then service tickets are allowed to be sent.
    bool EnableSubmission;

    REGISTER_YSON_STRUCT(TNativeAuthenticationManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeAuthenticationManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TNativeAuthenticationManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<bool> EnableValidation;
    std::optional<bool> EnableSubmission;

    REGISTER_YSON_STRUCT(TNativeAuthenticationManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TNativeAuthenticationManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TTvmBridgeConfig
    : public NYTree::TYsonStruct
{
public:
    //! Source TVM id.
    TTvmId SelfTvmId;

    //! Period to refresh the TVM tokens.
    TDuration RefreshPeriod;

    //! Backoff for EnsureDestinationServiceIds().
    TExponentialBackoffOptions EnsureTicketsBackoff;

    //! Timeout for RPC calls.
    TDuration RpcTimeout;

    REGISTER_YSON_STRUCT(TTvmBridgeConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTvmBridgeConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
