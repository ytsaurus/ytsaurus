#pragma once

#include "common.h"

#include <ytlib/file_client/file_writer_base.h>
#include <ytlib/cypress/cypress_service_proxy.h>
#include <ytlib/cypress/id.h>

namespace NYT {
namespace NFileClient {
    
////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for writing files.
/*!
 *  The client must call #Open and then feed the data in by calling #Write.
 *  Finally it must call #Close.
 */
class TFileWriter
    : public TFileWriterBase
{
public:
    typedef TIntrusivePtr<TFileWriter> TPtr;

    //! Initializes an instance.
    TFileWriter(
        TConfig* config,
        NRpc::IChannel* masterChannel,
        NTransactionClient::ITransaction* transaction,
        NTransactionClient::TTransactionManager* transactionManager,
        const NYTree::TYPath& path);

    //! Opens the writer.
    void Open();

protected:
    virtual void SpecificClose(const NChunkServer::TChunkId&, const NObjectServer::TTransactionId&);

private:
    NTransactionClient::ITransaction::TPtr Transaction;
    NYTree::TYPath Path;
    NCypress::TNodeId NodeId;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
