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
    TConfig::TPtr config,
    const TOptions& options,
    NRpc::IChannel::TPtr masterChannel,
    ITransaction::TPtr transaction,
    TTransactionManager::TPtr transactionManager,
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
    , CypressProxy(masterChannel)
    , Logger(TableClientLogger)
    , PathWithTransaction(WithTransaction(Path, TransactionId))
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
    TChunkListId chunkListId;
    auto schema = TSchema::CreateDefault();
    {
        auto batchReq = CypressProxy.ExecuteBatch();
        if (Options.Sorted) {
            {
                auto req = TCypressYPathProxy::Lock(WithTransaction(Path, UploadTransaction->GetId()));
                req->set_mode(ELockMode::Exclusive);
                batchReq->AddRequest(req, "lock");
            }
            {
                auto req = TYPathProxy::Get(PathWithTransaction + "/@row_count");
                batchReq->AddRequest(req, "get_row_count");
            }
        }

        {
            auto req = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(Path, UploadTransaction->GetId()));
            batchReq->AddRequest(req, "get_chunk_list_for_update");
        }
        {
            auto req = TCypressYPathProxy::Get(PathWithTransaction + "/@schema");
            batchReq->AddRequest(req, "get_schema");
        }

        auto batchRsp = batchReq->Invoke()->Get();
        if (!batchRsp->IsOK()) {
            LOG_ERROR_AND_THROW(yexception(), "Error requesting table info\n%s",
                ~batchRsp->GetError().ToString());
        }

        if (Options.Sorted) {
            {
                auto rsp = batchRsp->GetResponse("lock");
                if (!rsp->IsOK()) {
                    LOG_ERROR_AND_THROW(yexception(), "Error locking table for sorted write\n%s",
                        ~rsp->GetError().ToString());
                }
            }
            {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_row_count");
                if (!rsp->IsOK()) {
                    LOG_ERROR_AND_THROW(yexception(), "Error getting table row count\n%s",
                        ~rsp->GetError().ToString());
                }
                auto rowCount = DeserializeFromYson<i64>(rsp->value());
                if (rowCount > 0) {
                    LOG_ERROR_AND_THROW(yexception(), "Cannot perform sorted write to a nonempty table");
                }
            }
        }

        {
            auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspGetChunkListForUpdate>("get_chunk_list_for_update");
            if (!rsp->IsOK()) {
                LOG_ERROR_AND_THROW(yexception(), "Error requesting chunk list id\n%s",
                    ~rsp->GetError().ToString());
            }
            chunkListId = TChunkListId::FromProto(rsp->chunk_list_id());
        }

        {
            auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspGet>("get_schema");
            schema = TSchema::CreateDefault();
            if (rsp->IsOK()) {
                try {
                    schema = TSchema::FromYson(rsp->value());
                }
                catch (const std::exception& ex) {
                    ythrow yexception() << Sprintf("Error parsing table schema\n%s", ex.what());
                }
            }
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

    if (Options.Sorted) {
        LOG_INFO("Marking table as sorted");
        auto req = TTableYPathProxy::SetSorted(WithTransaction(Path, UploadTransaction->GetId()));
        auto rsp = CypressProxy.Execute(req)->Get();
        if (!rsp->IsOK()) {
            LOG_ERROR_AND_THROW(yexception(), "Error marking table as sorted\n%s",
                ~rsp->GetError().ToString());
        }
        LOG_INFO("Table is marked as sorted");
    }

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
