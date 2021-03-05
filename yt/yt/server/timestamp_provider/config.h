#pragma once

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NTimestampProvider {

////////////////////////////////////////////////////////////////////////////////

class TTimestampProviderConfig
    : public TServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    NBus::TTcpBusConfigPtr BusClient;

    NTransactionClient::TRemoteTimestampProviderConfigPtr TimestampProvider;

    TTimestampProviderConfig();
};

DEFINE_REFCOUNTED_TYPE(TTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTimestampProvider
