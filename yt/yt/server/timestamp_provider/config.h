#pragma once

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/client/transaction_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProviderBootstrapConfig
    : public NServer::TServerBootstrapConfig
{
public:
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

class TTimestampProviderProgramConfig
    : public TTimestampProviderBootstrapConfig
    , public TServerProgramConfig
{
public:
    REGISTER_YSON_STRUCT(TTimestampProviderProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTimestampProviderProgramConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
