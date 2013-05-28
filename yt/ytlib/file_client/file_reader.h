#pragma once

#include "public.h"
#include "file_ypath_proxy.h"

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
class TAsyncReader
    : public NTransactionClient::TTransactionListener
{
public:
    typedef TValueOrError<TSharedRef> TResult;
    typedef TAsyncReader TThis;

    TAsyncReader();

    TAsyncError AsyncOpen(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYPath::TRichYPath& richPath,
        const TNullable<i64>& offset = Null,
        const TNullable<i64>& length = Null);
    
    TFuture<TResult> AsyncRead();

    i64 GetSize() const;

private:
    TAsyncError OnInfoFetched(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        TFileYPathProxy::TRspFetchPtr fetchRsp);

    bool IsFirstBlock;
    TIntrusivePtr<TFileChunkSequenceReader> Reader;

    i64 Size;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

class TSyncReader
    : public TRefCounted
{
public:
    TSyncReader();

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
    TAsyncReaderPtr AsyncReader_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
