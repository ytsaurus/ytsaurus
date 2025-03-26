#pragma once

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/client/transaction_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

struct TTimestampProviderBootstrapConfig
    : public NServer::TServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    NBus::TBusConfigPtr BusClient;

    //! Clock server cell tag
    NObjectClient::TCellTag ClockClusterTag;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    std::vector<NTransactionClient::TAlienTimestampProviderConfigPtr> AlienProviders;

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
