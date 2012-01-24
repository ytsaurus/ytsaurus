#pragma once

#include "common.h"
#include "chunk_sequence_writer.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/table_server/table_ypath_proxy.h>

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
        NTransactionClient::TTransactionManager* transactionManager,
        const TSchema& schema,
        const NYTree::TYPath& path);

    void Open();
    void Write(const TColumn& column, TValue value);
    void EndRow();
    void Close();

private:
    void Finish();
    void OnAborted();

    TConfig::TPtr Config;
    NChunkServer::TChunkListId ChunkListId;
    NTransactionClient::ITransaction::TPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NTransactionClient::ITransaction::TPtr UploadTransaction;
    NTransactionClient::TTransactionManager::TPtr TransactionManager;
    NCypress::TCypressServiceProxy Proxy;
    TChunkSequenceWriter::TPtr Writer;
    NLog::TTaggedLogger Logger;
    IAction::TPtr OnAborted_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
