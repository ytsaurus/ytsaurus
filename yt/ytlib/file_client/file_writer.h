#pragma once

#include "public.h"

#include <core/misc/ref.h>

#include <core/ytree/public.h>

#include <core/rpc/public.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/cypress_client/public.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/multi_chunk_sequential_writer.h>

#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

#include <ytlib/ypath/rich.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #AsyncOpen and then feed the data in by calling #AsyncWrite.
 *  Finally it must call #Close.
 */
class TAsyncWriter
    : public NTransactionClient::TTransactionListener
{
public:
    TAsyncWriter(
        TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::TTransactionPtr transaction,
        NTransactionClient::TTransactionManagerPtr transactionManager,
        const NYPath::TRichYPath& richPath);

    ~TAsyncWriter();

    TAsyncError Open();
    TAsyncError Write(const TRef& data);
    TAsyncError Close();

private:
    typedef NChunkClient::TMultiChunkSequentialWriter<TFileChunkWriterProvider> TWriter;

    TFileWriterConfigPtr Config;
    NRpc::IChannelPtr MasterChannel;

    NTransactionClient::TTransactionPtr Transaction;
    NTransactionClient::TTransactionManagerPtr TransactionManager;
    NTransactionClient::TTransactionPtr UploadTransaction;
    NYPath::TRichYPath RichPath;

    TIntrusivePtr<TWriter> Writer;

    NLog::TTaggedLogger Logger;

    NCypressClient::TNodeId NodeId;


    TError DoOpen();
    TError DoWrite(const TRef& data);
    TError DoClose();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
