#include "stdafx.h"
#include "stderr_output.h"

#include <ytlib/file_client/config.h>
#include <ytlib/file_client/file_writer_base.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/transaction_server/transaction_ypath_proxy.h>
#include <ytlib/rpc/channel.h>

namespace NYT {
namespace NJobProxy {

using namespace NFileClient;
using namespace NRpc;
using namespace NTransactionServer;
using namespace NChunkServer;
using namespace NObjectServer;
using namespace NCypress;

////////////////////////////////////////////////////////////////////

TErrorOutput::TErrorOutput(
    TFileWriterConfigPtr config, 
    IChannelPtr masterChannel,
    const TTransactionId& transactionId,
    const TChunkListId& chunkListId)
    : FileWriter(New<TFileWriterBase>(config, masterChannel))
    , MasterChannel(masterChannel)
    , TransactionId(transactionId)
    , ChunkListId(chunkListId)
{
    FileWriter->Open(TransactionId);
}

TErrorOutput::~TErrorOutput() throw()
{ }

void TErrorOutput::DoWrite(const void* buf, size_t len)
{
    //ToDo(psushin): may be fix cast by fixing TRef ctor?
    FileWriter->Write(TRef(const_cast<void *>(buf), len));
}

void TErrorOutput::DoFinish() 
{
    FileWriter->Close();

    TObjectServiceProxy proxy(~MasterChannel);
    auto batchReq = proxy.ExecuteBatch();
    {
        auto req = TChunkListYPathProxy::Attach(FromObjectId(ChunkListId));
        req->add_children_ids(FileWriter->GetChunkId().ToProto());
        batchReq->AddRequest(~req);
    }
    {
        auto req = TTransactionYPathProxy::ReleaseObject(FromObjectId(TransactionId));
        req->set_object_id(FileWriter->GetChunkId().ToProto());
        batchReq->AddRequest(~req);
    }

    auto batchRsp = batchReq->Invoke()->Get();

    if (!batchRsp->IsOK()) {
        ythrow yexception() << Sprintf(
            "Request to attach chunk with stderr failed (error: %s)", 
            ~batchRsp->GetError().GetMessage());
    }

    for (int i = 0; i < batchRsp->GetSize(); ++i) {
        auto rsp = batchRsp->GetResponse(i);
        if (!rsp->IsOK()) {
            ythrow yexception() << Sprintf(
                "Failed to attach chunk with stderr (error: %s)", 
                ~rsp->GetError().GetMessage());
        }
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
