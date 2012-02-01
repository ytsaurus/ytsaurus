#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_meta.pb.h"

//#include <ytlib/misc/string.h>
//#include <ytlib/misc/sync.h>
//#include <ytlib/misc/serialize.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/file_server/file_ypath_proxy.h>
#include <ytlib/ytree/serialize.h>
//#include <ytlib/chunk_server/chunk_ypath_proxy.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NCypress;
using namespace NChunkServer;
using namespace NChunkClient;
using namespace NFileServer;
using namespace NProto;
using namespace NChunkHolder::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): use totalReplicaCount

TFileWriter::TFileWriter(
    TConfig* config,
    NRpc::IChannel* masterChannel,
    ITransaction* transaction,
    TTransactionManager* transactionManager,
    const TYPath& path)
    : TFileWriterBase(config, masterChannel, transaction ? transaction->GetId() : NullTransactionId, transactionManager)
    , Transaction(transaction)
    , Path(path)
{
    Logger.AddTag(Sprintf("Path: %s",
        ~Path));
}

void TFileWriter::Open()
{
    TFileWriterBase::Open();
    if (Transaction) {
        ListenTransaction(~Transaction);
    }

    LOG_INFO("File writer opened");
}

void TFileWriter::SpecificClose(const NChunkServer::TChunkId& ChunkId, const NObjectServer::TTransactionId& TransactionId)
{
    LOG_INFO("Creating file node");
    auto createNodeReq = TCypressYPathProxy::Create(WithTransaction(Path, TransactionId));
    createNodeReq->set_type(EObjectType::File);
    auto manifest = New<TFileManifest>();
    manifest->ChunkId = ChunkId;
    createNodeReq->set_manifest(SerializeToYson(~manifest));
    auto createNodeRsp = CypressProxy.Execute(~createNodeReq)->Get();
    if (!createNodeRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating file node\n%s",
            ~createNodeRsp->GetError().ToString());
    }
    NodeId = TNodeId::FromProto(createNodeRsp->object_id());
    LOG_INFO("File node created (NodeId: %s)", ~NodeId.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
