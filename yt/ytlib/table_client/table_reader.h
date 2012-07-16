#pragma once

#include "public.h"
#include "sync_reader.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/cypress_client/id.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/table_server/table_ypath_proxy.h>
#include <ytlib/chunk_client/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading tables.
/*!
 *  The client must first call #Open. This positions the reader before the first row.
 *  
 *  Then the client it must iteratively fetch rows by calling #NextRow.
 *  When no more rows can be fetched, the latter returns False.
 *  
 *  For each row, the client must fetch its entries in a similar manner by calling #NextColumn.
 *  
 *  When a table entry is fetched, its content becomes accessible via #GetColumn and #GetValue.
 */
class TTableReader
    : public NTransactionClient::TTransactionListener
    , public ISyncReader
{
public:
    //! Initializes an instance.
    TTableReader(
        TChunkSequenceReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYTree::TYPath& path);

    //! Opens the reader and positions it on the first row
    /*!
     *  Check if row is valid before getting it.
     */
    void Open();

    void NextRow();

    bool IsValid() const;

    TRow& GetRow();
    const NYTree::TYsonString& GetRowAttributes() const;

private:
    TChunkSequenceReaderConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionClient::ITransactionPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NChunkClient::IBlockCachePtr BlockCache;
    NYTree::TYPath Path;
    bool IsOpen;
    NObjectServer::TObjectServiceProxy Proxy;
    NLog::TTaggedLogger Logger;

    TChunkSequenceReaderPtr Reader;
    NCypressClient::TNodeId NodeId;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
