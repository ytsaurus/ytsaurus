#pragma once

#include "cluster_nodes.h"
#include "query_context.h"
#include "private.h"

#include <yt/client/misc/discovery.h>

#include <yt/core/actions/public.h>

#include <string>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TClickHouseHost
    : public TRefCounted
{
public:
    TClickHouseHost(
        TBootstrap* bootstrap,
        TClickHouseServerBootstrapConfigPtr nativeConfig,
        std::string cliqueId,
        std::string instanceId,
        ui16 rpcPort,
        ui16 monitoringPort,
        ui16 tcpPort,
        ui16 httpPort);

    ~TClickHouseHost();

    void Start();

    void HandleIncomingGossip(const TString& instanceId, EInstanceState state);

    TFuture<void> StopDiscovery();
    void StopTcpServers();

    const IInvokerPtr& GetControlInvoker() const;

    DB::Context& GetContext() const;

    TClusterNodes GetNodes() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TClickHouseHost)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
