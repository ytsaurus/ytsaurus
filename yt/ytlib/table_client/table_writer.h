#pragma once

#include "common.h"
#include "chunk_sequence_writer.h"

#include "../rpc/channel.h"
#include "../transaction_client/transaction.h"
#include "../cypress/cypress_service_proxy.h"
#include "../table_server/table_ypath_proxy.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Provides a synchronous API for writing tables.
class TTableWriter
    : public ISyncWriter
{   
public:
    typedef TIntrusivePtr<TTableWriter> TPtr;

    struct TConfig 
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration MasterRpcTimeout;
        TChunkSequenceWriter::TConfig::TPtr ChunkSequenceWriter;

        TConfig()
        {
            Register("master_rpc_timeout", MasterRpcTimeout).Default(TDuration::Seconds(5));
            Register("chunk_sequence_writer", ChunkSequenceWriter).DefaultNew();
        }
    };

    TTableWriter(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        const TSchema& schema,
        const NYTree::TYPath& path);

    void Open();
    void Write(const TColumn& column, TValue value);
    void EndRow();
    void Close();

private:
    bool NodeExists(const Stroka& nodePath);
    void CreateTableNode(const Stroka& nodePath);

    void Finish();

    void OnAborted();

    TConfig::TPtr Config;
    const Stroka Path;
    NCypress::TNodeId NodeId;
    NTransactionClient::ITransaction::TPtr Transaction;
    NRpc::IChannel::TPtr MasterChannel;
    TChunkSequenceWriter::TPtr Writer;
    NCypress::TCypressServiceProxy Proxy;
    IAction::TPtr OnAborted_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
