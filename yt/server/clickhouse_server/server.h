#pragma once

#include <yt/server/clickhouse_server/public.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TServer
{
public:
    TServer(
        ILoggerPtr logger,
        IStoragePtr storage,
        ICoordinationServicePtr coordinationService,
        ICliqueAuthorizationManagerPtr cliqueAuthorizationManager,
        TClickHouseServerBootstrapConfigPtr nativeConfig,
        std::string cliqueId,
        std::string instanceId,
        ui16 tcpPort,
        ui16 httpPort);

    ~TServer();

    void Start();
    void Shutdown();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
