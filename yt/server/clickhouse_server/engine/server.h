#pragma once

#include "clickhouse.h"

#include <yt/server/clickhouse_server/native/public.h>

#include <string>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

class TServer
{
public:
    TServer(
        NNative::ILoggerPtr logger,
        NNative::IStoragePtr storage,
        NNative::ICoordinationServicePtr coordinationService,
        NNative::ICliqueAuthorizationManagerPtr cliqueAuthorizationManager,
        std::string configFile,
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

} // namespace NYT::NClickHouseServer::NEngine
