#pragma once

#include "public.h"

#include <ytlib/table_client/config.h>
#include <ytlib/file_client/config.h>
#include <ytlib/meta_state/config.h>
#include <ytlib/ytree/ytree.h>
#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/bus/config.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobIOConfig
    : public TYsonSerializable
{
    NTableClient::TTableReaderConfigPtr TableReader;
    NTableClient::TTableWriterConfigPtr TableWriter;
    NFileClient::TFileWriterConfigPtr ErrorFileWriter;

    TJobIOConfig()
    {
        Register("table_reader", TableReader)
            .DefaultNew();
        Register("table_writer", TableWriter)
            .DefaultNew();
        Register("error_file_writer", ErrorFileWriter)
            .DefaultNew();

        // We do not provide much fault tolerance for stderr by default.
        ErrorFileWriter->ReplicationFactor = 1;
        ErrorFileWriter->UploadReplicationFactor = 1;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TJobProxyConfig
    : public TYsonSerializable
{
    // Filled by exec agent.
    NBus::TTcpBusClientConfigPtr SupervisorConnection;
    Stroka SandboxName;
    NMetaState::TMasterDiscoveryConfigPtr Masters;
    TDuration SupervisorRpcTimeout;
    TDuration HeartbeatPeriod;

    TJobIOConfigPtr JobIO;
    NYTree::INodePtr Logging;

    TJobProxyConfig()
    {
        Register("supervisor_connection", SupervisorConnection);
        Register("sandbox_name", SandboxName)
            .NonEmpty();
        Register("masters", Masters);
        Register("supervisor_rpc_timeout", SupervisorRpcTimeout)
            .Default(TDuration::Seconds(30));
        Register("heartbeat_period", HeartbeatPeriod)
            .Default(TDuration::Seconds(5));
        Register("job_io", JobIO)
            .DefaultNew();
        Register("logging", Logging)
            .Default();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
