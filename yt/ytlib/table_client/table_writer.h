#pragma once

#include "common.h"
#include "chunk_sequence_writer.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/table_server/table_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing tables.
/*!
 *  The client must first call #Open.
 *  
 *  For each row to be written, the client must add its entries by calling #Write.
 *  To finish the current row, the client must call #EndRow.
 *  
 *  Finally the client must call #Close.
 *  After this call the writer is no longer usable.
 */
class TTableWriter
    : public NTransactionClient::TTransactionListener
    , public ISyncWriter
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
            Register("master_rpc_timeout", MasterRpcTimeout)
                .Default(TDuration::Seconds(5));
            Register("chunk_sequence_writer", ChunkSequenceWriter)
                .DefaultNew();
        }
    };

    //! Initializes an instance.
    TTableWriter(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NTransactionClient::TTransactionManager* transactionManager,
        const NYTree::TYPath& path);

    //! Opens the writer.
    void Open();

    //! Appends a new entry to the current row.
    void Write(const TColumn& column, TValue value);

    //! Flushes the current row and switches the writer to a new one.
    void EndRow();

    //! Closes the writer.
    void Close();

private:
    TConfig::TPtr Config;

    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NTransactionClient::TTransactionManager::TPtr TransactionManager;
    NYTree::TYPath Path;
    bool IsOpen;
    bool IsClosed;
    NCypress::TCypressServiceProxy Proxy;
    NLog::TTaggedLogger Logger;

    NTransactionClient::ITransaction::TPtr UploadTransaction;
    NChunkServer::TChunkListId ChunkListId;
    TChunkSequenceWriter::TPtr Writer;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
