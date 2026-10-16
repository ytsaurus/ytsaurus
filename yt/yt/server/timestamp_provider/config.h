#pragma once

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/transaction_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

struct TTimestampProviderBootstrapConfig
    : public NServer::TServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    NBus::NTcp::TBusConfigPtr BusClient;

    //! Clock server cell tag
    NObjectClient::TCellTag ClockClusterTag;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    std::vector<NTransactionClient::TAlienTimestampProviderConfigPtr> AlienProviders;

    //! Timestamp provider is not necessarily bound to a native cluster.
    NApi::NNative::TConnectionCompoundConfigPtr ClusterConnection;
    TCypressRegistrarConfigPtr CypressRegistrar;

    REGISTER_YSON_STRUCT(TTimestampProviderBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTimestampProviderBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TTimestampProviderProgramConfig
    : public TTimestampProviderBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TTimestampProviderProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTimestampProviderProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
