#pragma once

#include "public.h"
#include "file_ypath_proxy.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/multi_chunk_sequential_reader.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading files.
/*!
 *  The client must call #AsyncOpen and then read the file block-by-block
 *  by calling #AsyncRead.
 */
class TAsyncReader
    : public NTransactionClient::TTransactionListener
{
public:
    typedef TErrorOr<TSharedRef> TReadResult;

    TAsyncReader(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NChunkClient::IBlockCachePtr blockCache,
        NTransactionClient::TTransactionPtr transaction,
        const NYPath::TRichYPath& richPath,
        const TNullable<i64>& offset = Null,
        const TNullable<i64>& length = Null);

    ~TAsyncReader();

    TAsyncError Open();
    TFuture<TReadResult> Read();

    //! Can only be called after the reader is successfully opened.
    i64 GetSize() const;

private:
    typedef TAsyncReader TThis;
    typedef NChunkClient::TMultiChunkSequentialReader<TFileChunkReader> TReader;

    TFileReaderConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;
    NChunkClient::IBlockCachePtr BlockCache;
    NTransactionClient::TTransactionPtr Transaction;
    NYPath::TRichYPath RichPath;
    TNullable<i64> Offset;
    TNullable<i64> Length;

    bool IsFirstBlock;
    bool IsFinished;
    TIntrusivePtr<TReader> Reader;

    i64 Size;

    NLog::TTaggedLogger Logger;


    void DoOpen();
    TSharedRef DoRead();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
