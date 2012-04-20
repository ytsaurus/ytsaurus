#pragma once

#include "public.h"
#include "sync_writer.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/cypress/cypress_service_proxy.h>

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
    //! Initializes an instance.
    TTableWriter(
        TChunkSequenceWriterConfigPtr config,
        NRpc::IChannel::TPtr masterChannel,
        NTransactionClient::ITransaction::TPtr transaction,
        NTransactionClient::TTransactionManager::TPtr transactionManager,
        const NYTree::TYPath& path,
        const TNullable<TKeyColumns>& keyColumns);

    //! Opens the writer.
    void Open();

    void WriteRow(TRow& column, TKey& value);
    void Close();

    const TNullable<TKeyColumns>& GetKeyColumns() const;

    //! Current row count.
    i64 GetRowCount() const;

    TKey& GetLastKey();

private:
    TChunkSequenceWriterConfigPtr Config;
    NRpc::IChannel::TPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NTransactionClient::TTransactionManager::TPtr TransactionManager;
    NYTree::TYPath Path;
    TNullable<TKeyColumns> KeyColumns;

    bool IsOpen;
    bool IsClosed;
    NCypress::TCypressServiceProxy CypressProxy;
    NLog::TTaggedLogger Logger;

    NTransactionClient::ITransaction::TPtr UploadTransaction;
    NChunkServer::TChunkListId ChunkListId;

    TChunkSequenceWriterPtr Writer;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
