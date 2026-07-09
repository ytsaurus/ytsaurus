#pragma once

#include "public.h"

#include <yt/yt/core/http/public.h>

#include <yt/yt/client/api/options.h>
#include <yt/yt/client/api/public.h>
#include <yt/yt/client/cache/public.h>
#include <yt/yt/client/object_client/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/flow/library/cpp/misc/node_info.h>

#include <yt/yt/library/tvm/service/public.h>

#include <library/cpp/yt/misc/static_initializer.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TAuthenticatorConfig
    : public virtual NYTree::TYsonStruct
{
    //! If true, requests without a valid proxy signature are rejected.
    //! If false, signatures are still verified and metrics are emitted,
    //! but invalid/missing signatures do not block the request.
    //! Used during rollout: deploy with false, switch to true after both
    //! sides are updated and the failure metric is at zero.
    bool RequireProxySignature{};

    REGISTER_YSON_STRUCT(TAuthenticatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TAuthenticatorConfig);

////////////////////////////////////////////////////////////////////////////////

//! This class provides authentication facilities for Flow pipeline.
//! Supports TVM (https://nda.ya.ru/t/EDKtwJBv7a3itW) and OAuth authentication methods.
//! Provides internal (flow controller <-> flow workers)
//! and external (YT -> flow, flow -> YT, flow -> other systems) authentication.
//! See yt/docs/ru/yandex-specific/flow/contributor/internal-authentication.md for the authentication algorithm.
struct IPipelineAuthenticator
    : public TRefCounted
{
    //! Returns tvm service if tvm is configured, returns nullptr otherwise.
    virtual NAuth::IDynamicTvmServicePtr GetTvmService() = 0;

    //! Returns options with OAUTH or TVM authentication parameters,
    virtual NApi::TClientOptions GetClientOptions() = 0;

    //! Wraps channel factory to inject authentication for interacting inside Flow pipeline.
    virtual NRpc::IChannelFactoryPtr CreateSelfCredentialsInjectingChannelFactory(NRpc::IChannelFactoryPtr underlying) = 0;

    //! Returns authenticator for services processing internal Flow pipeline requests.
    virtual NRpc::IAuthenticatorPtr CreateSelfRpcAuthenticator() = 0;

    //! Returns authenticator for controller service to authenticate requests from YT.
    virtual NRpc::IAuthenticatorPtr CreateYTControllerRpcAuthenticator() = 0;

    //! Returns human-readable text with description of used auth method.
    virtual std::string GetAuthDescription() = 0;
};

IPipelineAuthenticatorPtr CreatePipelineAuthenticator(
    NYPath::TRichYPath pipelinePath,
    std::optional<std::string> proxyRole,
    NAuth::TTvmServiceConfigPtr tvmConfig,
    NHttp::IClientPtr httpClient,
    TAuthenticatorConfigPtr config,
    TNodeInfoPtr nodeInfo,
    NClient::NCache::TClientsCacheConfigPtr clientsCacheConfig);


////////////////////////////////////////////////////////////////////////////////

//! Add built-in TVM ID to default tvm aliases map.
//! This function cannot be called after first call of GetDefaultTvmAliases,
//! so it is usually called from global constructors.
//! Prefer using this function via YT_FLOW_REGISTER_DEFAULT_TVM_ALIAS macro.
void RegisterDefaultTvmAlias(const std::string& tvmAlias, NAuth::TTvmId tvmId);

//! Returns default tvm aliases map. It contains build-in TVM IDs.
const THashMap<std::string, NAuth::TTvmId>& GetDefaultTvmAliases();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

//! Register built-in TVM ID. It will be added to default value in destination TVM IDs in TVM config.
//! Mark .cpp file containing such registration as GLOBAL to avoid linker optimization of this registration.
#define YT_FLOW_REGISTER_DEFAULT_TVM_ALIAS(tvmAlias, tvmId)                \
    YT_STATIC_INITIALIZER({                                                \
        NYT::NFlow::RegisterDefaultTvmAlias(std::string{tvmAlias}, tvmId); \
    })

////////////////////////////////////////////////////////////////////////////////
