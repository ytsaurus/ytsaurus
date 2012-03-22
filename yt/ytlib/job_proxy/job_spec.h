#pragma once

#include "common.h"

#include "../exec/operations.pb.h"
#include <ytlib/election/leader_lookup.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/table_client/chunk_sequence_reader.h>
#include <ytlib/table_client/chunk_sequence_writer.h>
#include <ytlib/file_client/file_writer_base.h>
#include <ytlib/ytree/yson_writer.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

class TJobSpec
{
public:
    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        NYTree::EYsonFormat OutputFormat;
        NTableClient::TChunkSequenceReader::TConfig::TPtr ChunkSequenceReader;
        NTableClient::TChunkSequenceWriter::TConfig::TPtr ChunkSequenceWriter;
        NElection::TLeaderLookup::TConfig::TPtr Masters;
        NFileClient::TFileWriterBase::TConfig::TPtr StdErr;

        TConfig()
        {
            Register("output_format", OutputFormat).Default(NYTree::EYsonFormat::Text);
            Register("chunk_sequence_reader", ChunkSequenceReader).DefaultNew();
            Register("chunk_sequence_writer", ChunkSequenceWriter).DefaultNew();
            Register("masters", Masters);
            Register("std_err", StdErr).Default(New<NFileClient::TFileWriterBase::TConfig>(1,1));
        }
    };

    TJobSpec(
        TConfig* config,
        const NScheduler::NProto::TJobSpec& jobSpec,
        const NTransactionClient::TTransactionId& transactionId);

    int GetInputCount() const;
    int GetOutputCount() const;

    TOutputStream* GetErrorOutput();
    TInputStream* GetTableInput(int index);
    NTableClient::ISyncWriter::TPtr GetTableOutput(int index);

    Stroka GetShellCommand() const;

private:
    TConfig::TPtr Config;

    // ToDo: make factory, depending on job type and interface.
    NScheduler::NProto::TJobSpec ProtoSpec;
    NScheduler::NProto::TMapJobSpec MapSpec;

    NTransactionClient::TTransactionId TransactionId;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::TTransactionId StdErrTransactionId;
    NChunkServer::TChunkListId StdErrChunkListId;
    NFileClient::TFileWriterBase* StdErr;

};

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
