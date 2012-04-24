#pragma once

#include "public.h"

// ToDo: use public.h everywhere.
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/file_client/file_writer_base.h>
#include <ytlib/election/leader_lookup.h>
#include <ytlib/ytree/ytree.h>

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct TJobIOConfig
    : public TConfigurable
{
    NYTree::EYsonFormat OutputFormat;
    NTableClient::TChunkSequenceReader::TConfig::TPtr ChunkSequenceReader;
    NTableClient::TChunkSequenceWriter::TConfig::TPtr ChunkSequenceWriter;
    NFileClient::TFileWriterBase::TConfig::TPtr ErrorFileWriter;

    TJobIOConfig()
    {
        Register("output_format", OutputFormat)
            .Default(NYTree::EYsonFormat::Text);
        Register("chunk_sequence_reader", ChunkSequenceReader)
            .DefaultNew();
        Register("chunk_sequence_writer", ChunkSequenceWriter)
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
    : public TConfigurable
{
    // Filled by exec agent.
    Stroka ExecAgentAddress;
    Stroka SandboxName;
    NElection::TLeaderLookup::TConfigPtr Masters;
    TDuration RpcTimeout;
    TDuration HeartbeatPeriod;

    TJobIOConfigPtr JobIO;
    NYTree::INodePtr Logging;

    TJobProxyConfig()
    {
        Register("exec_agent_address", ExecAgentAddress).NonEmpty();
        Register("sandbox_name", SandboxName).NonEmpty();
        Register("masters", Masters);
        Register("rpc_timeout", RpcTimeout).Default(TDuration::Seconds(5));
        Register("heartbeat_period", HeartbeatPeriod).Default(TDuration::Seconds(5));
        Register("job_io", JobIO).DefaultNew();
        Register("logging", Logging).Default(NULL);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT