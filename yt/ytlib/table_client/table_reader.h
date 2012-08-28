#pragma once

#include "public.h"
#include "sync_reader.h"

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/ytree/ypath.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/table_client/table_ypath_proxy.h>

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
 */
class TTableReader
    : public NTransactionClient::TTransactionListener
    , public ISyncReader
{
public:
    //! Initializes an instance.
    TTableReader(
        TTableReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYTree::TRichYPath& richPath);

    virtual void Open() override;

    virtual void NextRow() override;

    virtual bool IsValid() const override;

    virtual const TRow& GetRow() const override;
    virtual const TNonOwningKey& GetKey() const override;

private:
    TTableReaderConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NTransactionClient::ITransactionPtr Transaction;
    NTransactionClient::TTransactionId TransactionId;
    NChunkClient::IBlockCachePtr BlockCache;
    NYTree::TRichYPath RichPath;
    bool IsOpen;
    NObjectClient::TObjectServiceProxy Proxy;
    NLog::TTaggedLogger Logger;

    TTableChunkSequenceReaderPtr Reader;
    NCypressClient::TNodeId NodeId;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
