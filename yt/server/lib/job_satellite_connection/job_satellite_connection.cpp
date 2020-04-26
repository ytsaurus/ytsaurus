#include "job_satellite_connection.h"

#include <yt/core/bus/tcp/config.h>

#include <yt/core/misc/fs.h>

#include <util/system/fs.h>

namespace NYT::NJobSatelliteConnection {

using NJobTrackerClient::TJobId;
using NYson::EYsonFormat;
using NExecAgent::EJobEnvironmentType;

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

const TString SatelliteConfigFileName("satellite_config.yson");

////////////////////////////////////////////////////////////////////////////////

TString GetJobSatelliteUnixDomainSocketPath(const TString& slotPath, TJobId jobId)
{
    return NFS::GetRealPath(NFS::CombinePaths({
        slotPath,
        "pipes",
        Format("%v-job-satellite", jobId)}));
}

////////////////////////////////////////////////////////////////////////////////

TJobSatelliteConnection::TJobSatelliteConnection(
    const TString& slotPath,
    TJobId jobId,
    TTcpBusServerConfigPtr jobProxyRpcServerConfig,
    EJobEnvironmentType environmentType)
    : JobId_(jobId)
{
    ConnectionConfig_ = New<TJobSatelliteConnectionConfig>();
    auto jobSatelliteUnixDomainSocketPath = GetJobSatelliteUnixDomainSocketPath(slotPath, jobId);
    ConnectionConfig_->SatelliteRpcServerConfig->UnixDomainSocketPath = jobSatelliteUnixDomainSocketPath;
    if (jobProxyRpcServerConfig->UnixDomainSocketPath) {
        ConnectionConfig_->JobProxyRpcClientConfig->UnixDomainSocketPath = *jobProxyRpcServerConfig->UnixDomainSocketPath;
    }
    ConnectionConfig_->EnvironmentType = environmentType;
}

TString TJobSatelliteConnection::GetConfigPath() const
{
    return ConfigFile_;
}

TTcpBusClientConfigPtr TJobSatelliteConnection::GetRpcClientConfig() const
{
    return TTcpBusClientConfig::CreateUnixDomain(GetJobSatelliteUnixDomainSocketPath(".", JobId_));
}

NJobTrackerClient::TJobId TJobSatelliteConnection::GetJobId() const
{
    return JobId_;
}

void TJobSatelliteConnection::MakeConfig()
{
    ConfigFile_ = NFS::CombinePaths(NFs::CurrentWorkingDirectory().data(), SatelliteConfigFileName);
    try {
        TFile file(ConfigFile_, CreateAlways | WrOnly | Seq | CloseOnExec);
        TUnbufferedFileOutput output(file);
        NYson::TYsonWriter writer(&output, EYsonFormat::Pretty);
        Serialize(ConnectionConfig_, &writer);
        writer.Flush();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to write satellite config into %v", ConfigFile_) << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobSatelliteConnection
