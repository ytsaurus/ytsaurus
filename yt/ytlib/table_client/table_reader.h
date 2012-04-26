#pragma once

#include "common.h"
#include "sync_reader.h"
#include "chunk_sequence_reader.h"

#include <ytlib/logging/tagged_logger.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/rpc/channel.h>
#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/cypress/id.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/table_server/table_ypath_proxy.h>
#include <ytlib/chunk_client/block_cache.h>

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
    , public ISyncTableReader
{
public:
    typedef TIntrusivePtr<TTableReader> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TChunkSequenceReader::TConfig::TPtr ChunkSequenceReader;

        TConfig()
        {
            Register("chunk_sequence_reader", ChunkSequenceReader)
                .DefaultNew();
        }
    };

    //! Initializes an instance.
    TTableReader(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NChunkClient::IBlockCache* blockCache,
        const NYTree::TYPath& path);

    //! Opens the reader and positions it on the first row
    /*!
     *  Check if row is valid before getting it.
     */
    void Open();

    void NextRow();

    bool IsValid() const;

    const TRow& GetRow() const;
    const TKey& GetKey() const;

private:
    TConfig::TPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionClient::ITransaction::TPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NChunkClient::IBlockCache::TPtr BlockCache;
    NYTree::TYPath Path;
    bool IsOpen;
    NCypress::TCypressServiceProxy Proxy;
    NLog::TTaggedLogger Logger;

    TChunkSequenceReader::TPtr Reader;
    NCypress::TNodeId NodeId;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
