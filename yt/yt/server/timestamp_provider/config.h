#pragma once

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/client/transaction_client/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProviderConfig
    : public TServerConfig
    , public TServerProgramConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    NBus::TBusConfigPtr BusClient;

    //! Clock server cell tag
    NObjectClient::TCellTag ClockClusterTag;
    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;
    std::vector<NTransactionClient::TAlienTimestampProviderConfigPtr> AlienProviders;

    REGISTER_YSON_STRUCT(TTimestampProviderConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
