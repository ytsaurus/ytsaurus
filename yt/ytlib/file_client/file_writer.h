#pragma once

#include "public.h"

#include <ytlib/misc/ref.h>

#include <ytlib/ytree/public.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/rpc/public.h>

#include <ytlib/ypath/rich.h>

#include <ytlib/logging/tagged_logger.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Close.
 */
class TAsyncWriter
    : public NTransactionClient::TTransactionListener
{
public:
    TAsyncWriter(
        TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NTransactionClient::TTransactionManagerPtr transactionManager,
        const NYPath::TRichYPath& richPath);

    TAsyncError AsyncOpen();

    TAsyncError AsyncWrite(const TRef& data);

    TAsyncError AsyncClose();

private:
    typedef NChunkClient::TMultiChunkSequentialWriter<TFileChunkWriter> TWriter;
    typedef TAsyncWriter TThis;

    TAsyncError OnUploadTransactionStarted(
        TErrorOr<NTransactionClient::ITransactionPtr> transactionOrError);
    TAsyncError OnFileInfoReceived(NObjectClient::TObjectServiceProxy::TRspExecuteBatchPtr batchRsp);

    TFileWriterConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;

    NTransactionClient::ITransactionPtr Transaction;
    NTransactionClient::TTransactionManagerPtr TransactionManager;
    NTransactionClient::ITransactionPtr UploadTransaction;
    NYPath::TRichYPath RichPath;

    TIntrusivePtr<TWriter> Writer;

    NLog::TTaggedLogger Logger;

    NCypressClient::TNodeId NodeId;
};

////////////////////////////////////////////////////////////////////////////////

class TSyncWriter
    : public TRefCounted
{
public:
    TSyncWriter(
        TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NTransactionClient::TTransactionManagerPtr transactionManager,
        const NYPath::TRichYPath& richPath);

    void Open();

    void Write(const TRef& data);

    void Close();

private:
    TAsyncWriterPtr AsyncWriter_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
