#include "config.h"

namespace NYT::NServiceDiscovery::NYP {

////////////////////////////////////////////////////////////////////////////////

TServiceDiscoveryConfig::TServiceDiscoveryConfig()
{
    RegisterParameter("enable", Enable)
        .Default(true);

    RegisterParameter("fqdn", Fqdn)
        .Default("sd.yandex.net");
    RegisterParameter("grpc_port", GrpcPort)
        .Default(8081);

    RegisterParameter("client", Client)
        .Default("yt")
        .NonEmpty();

    RegisterPreprocessor([this] {
        RetryBackoffTime = TDuration::Seconds(1);
        RetryAttempts = 5;
        RetryTimeout = TDuration::Seconds(10);

        ExpireAfterAccessTime = TDuration::Days(1);
        ExpireAfterSuccessfulUpdateTime = TDuration::Days(1);
        ExpireAfterFailedUpdateTime = TDuration::Seconds(5);
        RefreshTime = TDuration::Seconds(5);
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery::NYP
