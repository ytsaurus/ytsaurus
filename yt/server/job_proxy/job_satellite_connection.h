#pragma once

#include "public.h"

#include <yt/core/bus/config.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/proc.h>

#include <yt/core/yson/public.h>

#include <yt/ytlib/job_tracker_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobSatelliteConnectionConfig
    : public NYTree::TYsonSerializable
{
public:
    //JobProxy -> JobSatellite connection.
    NBus::TTcpBusServerConfigPtr SatelliteRpcServerConfig;
    //Job -> JobSatellite -> JobProxy synchronization.
    NBus::TTcpBusClientConfigPtr JobProxyRpcClientConfig;
    bool UseContainer;

    TJobSatelliteConnectionConfig()
    {
        RegisterParameter("satellite_rpc_server", SatelliteRpcServerConfig)
            .DefaultNew();
        RegisterParameter("job_proxy_rpc_client", JobProxyRpcClientConfig)
            .DefaultNew();
        RegisterParameter("use_container_managment", UseContainer)
            .Default(false);
    }
};

DEFINE_REFCOUNTED_TYPE(TJobSatelliteConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TJobSatelliteConnection
{
public:
    TJobSatelliteConnection(
        const NJobTrackerClient::TJobId& jobId,
        NBus::TTcpBusServerConfigPtr jobProxyRpcServerConfig,
        bool useContainer);
    TString GetConfigPath() const;
    NBus::TTcpBusClientConfigPtr GetRpcClientConfig() const;
    const NJobTrackerClient::TJobId& GetJobId() const;

    void MakeConfig();

private:
    const NJobTrackerClient::TJobId JobId_;

    TString ConfigFile_;
    TJobSatelliteConnectionConfigPtr ConnectionConfig_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
