#pragma once

#include "public.h"
#include "sync_writer.h"

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/public.h>

#include <ytlib/table_client/table_ypath_proxy.h>

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
        TTableWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NTransactionClient::TTransactionManagerPtr transactionManager,
        const NYTree::TYPath& path,
        const TNullable<TKeyColumns>& keyColumns);

    //! Opens the writer.
    void Open();

    void WriteRow(const TRow& column);
    void Close();

    const TNullable<TKeyColumns>& GetKeyColumns() const;

    //! Current row count.
    i64 GetRowCount() const;

private:
    TTableWriterConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionClient::ITransactionPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NTransactionClient::TTransactionManagerPtr TransactionManager;
    NYTree::TYPath Path;
    TNullable<TKeyColumns> KeyColumns;

    bool IsOpen;
    bool IsClosed;
    NObjectClient::TObjectServiceProxy ObjectProxy;
    NLog::TTaggedLogger Logger;

    NTransactionClient::ITransactionPtr UploadTransaction;
    NChunkClient::TChunkListId ChunkListId;

    TTableChunkSequenceWriterPtr Writer;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
