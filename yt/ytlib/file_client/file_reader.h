#pragma once

#include "public.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading files.
/*!
 *  The client must call #Open and then read the file block-by-block
 *  calling #Read.
 */
class TFileReader
    : public NTransactionClient::TTransactionListener
{
public:
    //! Initializes an instance.
    TFileReader();

    //! Opens the reader.
    void Open(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYPath::TRichYPath& richPath,
        const TNullable<i64>& offset,
        const TNullable<i64>& length);

    TSharedRef Read();

    //! Returns the size of the file.
    i64 GetSize() const;

private:
    TAutoPtr<TFileReaderBase> BaseReader;
    NLog::TTaggedLogger Logger;

    DECLARE_THREAD_AFFINITY_SLOT(ClientThread);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
