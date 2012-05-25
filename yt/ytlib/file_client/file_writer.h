#pragma once

#include "public.h"
#include "file_writer_base.h"

#include <ytlib/ytree/public.h>
#include <ytlib/cypress/id.h>
#include <ytlib/transaction_client/public.h>
#include <ytlib/transaction_client/transaction_listener.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Close.
 */
class TFileWriter
    : public NTransactionClient::TTransactionListener
    , public TFileWriterBase
{
public:
    //! Initializes an instance.
    TFileWriter(
        TFileWriterConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NTransactionClient::TTransactionManagerPtr transactionManager,
        const NYTree::TYPath& path);

    ~TFileWriter();

    //! Opens the writer.
    virtual void Open();

    virtual void Write(TRef data);

    //! Closes the writer.
    virtual void Close();

    NCypress::TNodeId GetNodeId() const;

private:
    NTransactionClient::ITransactionPtr Transaction;
    NTransactionClient::TTransactionManagerPtr TransactionManager;
    NTransactionClient::ITransactionPtr UploadTransaction;
    NYTree::TYPath Path;

    NCypress::TNodeId NodeId;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
