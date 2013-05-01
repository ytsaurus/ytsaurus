#pragma once

#include "public.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

typedef NChunkClient::TMultiChunkSequentialReader<TFileChunkReader> TFileChunkSequenceReader;

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

    ~TFileReader();

    //! Opens the reader.
    void Open(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYPath::TRichYPath& richPath,
        const TNullable<i64>& offset = Null,
        const TNullable<i64>& length = Null);

    TSharedRef Read();

    i64 GetSize() const;

private:
    bool IsFirstBlock;
    TIntrusivePtr<TFileChunkSequenceReader> Reader;

    i64 Size;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
