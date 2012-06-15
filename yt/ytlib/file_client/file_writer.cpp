#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_output.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/ytree/serialize.h>
#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NCypress;
using namespace NObjectServer;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NFileServer;
using namespace NChunkHolder::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    TFileWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    TTransactionManagerPtr transactionManager,
    const TYPath& path)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionManager(transactionManager)
    , Path(path)
    , Logger(FileWriterLogger)
{
    YASSERT(transactionManager);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~Path,
        transaction ? ~transaction->GetId().ToString() : "None"));

    if (Transaction) {
        ListenTransaction(Transaction);
    }
}

void TFileWriter::Open()
{
    CheckAborted();

    LOG_INFO("Creating upload transaction");
    try {
        UploadTransaction = TransactionManager->Start(
            NULL,
            Transaction ? Transaction->GetId() : NullTransactionId);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating upload transaction\n%s",
            ex.what());
    }

    ListenTransaction(UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)",
        ~UploadTransaction->GetId().ToString());

    Writer = new TFileChunkOutput(Config, MasterChannel, UploadTransaction->GetId());
    Writer->Open();
}

void TFileWriter::Write(TRef data)
{
    CheckAborted();
    Writer->Write(data.Begin(), data.Size());
}


void TFileWriter::Close()
{
    CheckAborted();

    Writer->Finish();

    LOG_INFO("Creating file node");
    {
        TObjectServiceProxy objectProxy(MasterChannel);

        auto req = TCypressYPathProxy::Create(WithTransaction(
            Path,
            Transaction ? Transaction->GetId() : NullTransactionId));
        req->set_type(EObjectType::File);
        auto* reqExt = req->MutableExtension(NFileServer::NProto::TReqCreateFileExt::create_file);
        *reqExt->mutable_chunk_id() = Writer->GetChunkId().ToProto();
        auto rsp = objectProxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            LOG_ERROR_AND_THROW(yexception(), "Error creating file node\n%s",
                ~rsp->GetError().ToString());
        }
        NodeId = TNodeId::FromProto(rsp->object_id());
    }
    LOG_INFO("File node created (NodeId: %s)", ~NodeId.ToString());

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error committing upload transaction\n%s",
            ex.what());
    }
    LOG_INFO("Upload transaction committed");
}

NCypress::TNodeId TFileWriter::GetNodeId() const
{
    return NodeId;
}

TFileWriter::~TFileWriter()
{
    VERIFY_THREAD_AFFINITY_ANY();

    if (UploadTransaction) {
        UploadTransaction->Abort(true);
        UploadTransaction.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
