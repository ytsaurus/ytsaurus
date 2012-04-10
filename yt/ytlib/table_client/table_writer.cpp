#include "stdafx.h"
#include "table_writer.h"

#include <ytlib/misc/sync.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>
#include <ytlib/chunk_server/chunk_list_ypath_proxy.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypress;
using namespace NTransactionClient;
using namespace NTableServer;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): use totalReplicaCount

TTableWriter::TTableWriter(
    TConfig* config,
    const TOptions& options,
    NRpc::IChannel* masterChannel,
    ITransaction* transaction,
    TTransactionManager* transactionManager,
    const TYPath& path)
    : Config(config)
    , Options(options)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , Path(path)
    , IsOpen(false)
    , IsClosed(false)
    , Proxy(masterChannel)
    , Logger(TableClientLogger)
{
    YASSERT(config);
    YASSERT(masterChannel);
    YASSERT(transactionManager);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~path,
        ~TransactionId.ToString()));
}

void TTableWriter::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YVERIFY(!IsOpen);
    YVERIFY(!IsClosed);

    LOG_INFO("Opening table writer");

    LOG_INFO("Creating upload transaction");
    try {
        UploadTransaction = TransactionManager->Start(NULL, TransactionId);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error creating upload transaction\n%s",
            ex.what());
    }
    ListenTransaction(~UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)", ~UploadTransaction->GetId().ToString());

    LOG_INFO("Requesting table info");
    auto getInfoReq = Proxy.ExecuteBatch();

    auto getChunkListIdReq = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(Path, TransactionId));
    getInfoReq->AddRequest(~getChunkListIdReq);

    auto getSchemaReq = TCypressYPathProxy::Get(CombineYPaths(
        WithTransaction(Path, TransactionId),
        "@schema"));
    getInfoReq->AddRequest(~getSchemaReq);

    auto getInfoRsp = getInfoReq->Invoke().Get();
    if (!getInfoRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error requesting table info\n%s",
            ~getInfoRsp->GetError().ToString());
    }

    auto getChunkListIdRsp = getInfoRsp->GetResponse<TTableYPathProxy::TRspGetChunkListForUpdate>(0);
    if (!getChunkListIdRsp->IsOK()) {
        LOG_ERROR_AND_THROW(yexception(), "Error requesting chunk list id\n%s",
            ~getChunkListIdRsp->GetError().ToString());
    }
    auto chunkListId = TChunkListId::FromProto(getChunkListIdRsp->chunk_list_id());

    auto getSchemaRsp = getInfoRsp->GetResponse<TCypressYPathProxy::TRspGet>(1);
    auto schema = TSchema::Default();
    if (getSchemaRsp->IsOK()) {
        try {
            schema = TSchema::FromYson(getSchemaRsp->value());
        }
        catch (const std::exception& ex) {
            ythrow yexception() << Sprintf("Error parsing table schema (Path: %s)\n%s",
                ~Path,
                ex.what());
        }
    }

    LOG_INFO("Table info received (ChunkListId: %s, ChannelCount: %d)",
        ~chunkListId.ToString(),
        static_cast<int>(schema.GetChannels().size()));

    auto asyncWriter = New<TChunkSequenceWriter>(
        ~Config->ChunkSequenceWriter, 
        ~MasterChannel,
        UploadTransaction->GetId(),
        chunkListId);

    Writer.Reset(Options.Sorted
        ? new TSortedValidatingWriter(schema, ~asyncWriter)
        : new TValidatingWriter(schema, ~asyncWriter));

    Sync(~Writer, &TValidatingWriter::AsyncOpen);

    if (Transaction) {
        ListenTransaction(~Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table writer opened");
}

void TTableWriter::Write(const TColumn& column, TValue value)
{
    VERIFY_THREAD_AFFINITY(Client);
    YVERIFY(IsOpen);

    CheckAborted();
    Writer->Write(column, value);
}

void TTableWriter::EndRow()
{
    VERIFY_THREAD_AFFINITY(Client);
    YVERIFY(IsOpen);

    CheckAborted();
    Sync(~Writer, &TValidatingWriter::AsyncEndRow);
}

void TTableWriter::Close()
{
    VERIFY_THREAD_AFFINITY(Client);

    if (!IsOpen)
        return;

    IsOpen = false;
    IsClosed = true;

    CheckAborted();

    LOG_INFO("Closing table writer");

    LOG_INFO("Closing chunk writer");
    Sync(~Writer, &TValidatingWriter::AsyncClose);
    LOG_INFO("Chunk writer closed");

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(yexception(), "Error committing upload transaction\n%s",
            ex.what());
    }
    LOG_INFO("Upload transaction committed");

    LOG_INFO("Table writer closed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
