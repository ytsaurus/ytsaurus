#include "stdafx.h"
#include "table_writer.h"
#include "config.h"
#include "private.h"
#include "schema.h"
#include "table_chunk_sequence_writer.h"

#include <ytlib/misc/sync.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/meta_state/rpc_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TTableWriter::TTableWriter(
    TTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    NTransactionClient::ITransactionPtr transaction,
    NTransactionClient::TTransactionManagerPtr transactionManager,
    const NYTree::TRichYPath& richPath,
    const TNullable<TKeyColumns>& keyColumns)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , RichPath(richPath)
    , IsOpen(false)
    , IsClosed(false)
    , ObjectProxy(masterChannel)
    , Logger(TableWriterLogger)
    , KeyColumns(keyColumns)
{
    YASSERT(config);
    YASSERT(masterChannel);
    YASSERT(transactionManager);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~ToString(richPath),
        ~TransactionId.ToString()));
}

void TTableWriter::Open()
{
    VERIFY_THREAD_AFFINITY(Client);
    YCHECK(!IsOpen);
    YCHECK(!IsClosed);

    LOG_INFO("Opening table writer");

    LOG_INFO("Creating upload transaction");
    try {
        UploadTransaction = TransactionManager->Start(NULL, TransactionId);
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(TError("Error creating upload transaction")
            << ex);
    }
    auto uploadTransactionId = UploadTransaction->GetId();
    ListenTransaction(UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)", ~uploadTransactionId.ToString());

    auto path = RichPath.GetPath();

    LOG_INFO("Requesting table info");
    TChunkListId chunkListId;
    std::vector<TChannel> channels;
    {
        auto batchReq = ObjectProxy.ExecuteBatch();

        if (KeyColumns.IsInitialized()) {
            auto req = TCypressYPathProxy::Get(WithTransaction(path, TransactionId) + "/@row_count");
            batchReq->AddRequest(req, "get_row_count");
        }

        if (KeyColumns.IsInitialized() || RichPath.Attributes().Get<bool>("overwrite", false)) {
            auto req = TTableYPathProxy::Clear(WithTransaction(path, uploadTransactionId));
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "clear");
        }

        {
            auto req = TTableYPathProxy::GetChunkListForUpdate(WithTransaction(path, uploadTransactionId));
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "get_chunk_list_for_update");
        }
        {
            auto req = TCypressYPathProxy::Get(WithTransaction(path, TransactionId) + "/@channels");
            batchReq->AddRequest(req, "get_channels");
        }

        auto batchRsp = batchReq->Invoke().Get();
        if (!batchRsp->IsOK()) {
            LOG_ERROR_AND_THROW(TError("Error requesting table info")
                << batchRsp->GetError());
        }

        if (KeyColumns.IsInitialized()) {
            {
                auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_row_count");
                if (!rsp->IsOK()) {
                    LOG_ERROR_AND_THROW(TError("Error requesting table row count")
                        << rsp->GetError());
                }
                i64 rowCount = ConvertTo<i64>(TYsonString(rsp->value()));
                if (rowCount > 0) {
                    LOG_ERROR_AND_THROW(TError("Cannot write sorted data into a non-empty table"));
                }
            }
            {
                auto rsp = batchRsp->FindResponse("clear");
                if (rsp && !rsp->IsOK()) {
                    LOG_ERROR_AND_THROW(TError("Error clearing table")
                        << rsp->GetError());
                }
            }
        }

        {
            auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspGetChunkListForUpdate>("get_chunk_list_for_update");
            if (!rsp->IsOK()) {
                LOG_ERROR_AND_THROW(TError("Error requesting chunk list id")
                    << rsp->GetError());
            }
            chunkListId = TChunkListId::FromProto(rsp->chunk_list_id());
        }

        {
            auto rsp = batchRsp->GetResponse<TCypressYPathProxy::TRspGet>("get_channels");
            if (rsp->IsOK()) {
                try {
                    channels = ChannelsFromYson(TYsonString(rsp->value()));
                }
                catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Error parsing table channels")
                        << ex;
                }
            }
        }
    }
    LOG_INFO("Table info received (ChunkListId: %s, ChannelCount: %d)",
        ~chunkListId.ToString(),
        static_cast<int>(channels.size()));

    Writer = New<TTableChunkSequenceWriter>(
        Config, 
        MasterChannel,
        uploadTransactionId,
        chunkListId,
        channels,
        KeyColumns);

    Sync(~Writer, &TTableChunkSequenceWriter::AsyncOpen);

    if (Transaction) {
        ListenTransaction(Transaction);
    }

    IsOpen = true;

    LOG_INFO("Table writer opened");
}

void TTableWriter::WriteRow(const TRow& row)
{
    VERIFY_THREAD_AFFINITY(Client);
    YCHECK(IsOpen);

    CheckAborted();
    while (!Writer->TryWriteRow(row)) {
        Sync(~Writer, &TTableChunkSequenceWriter::GetReadyEvent);
    }
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
    Sync(~Writer, &TTableChunkSequenceWriter::AsyncClose);
    LOG_INFO("Chunk writer closed");

    auto path = RichPath.GetPath();

    if (KeyColumns) {
        auto keyColumns = KeyColumns.Get();
        LOG_INFO("Marking table as sorted by %s", ~ConvertToYsonString(keyColumns, EYsonFormat::Text).Data());
        
        auto req = TTableYPathProxy::SetSorted(WithTransaction(path, UploadTransaction->GetId()));
        NMetaState::GenerateRpcMutationId(req);
        ToProto(req->mutable_key_columns(), keyColumns);

        auto rsp = ObjectProxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            LOG_ERROR_AND_THROW(TError("Error marking table as sorted")
                << rsp->GetError());
        }
        LOG_INFO("Table is marked as sorted");
    }

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
    } catch (const std::exception& ex) {
        LOG_ERROR_AND_THROW(TError("Error committing upload transaction")
            << ex);
    }
    LOG_INFO("Upload transaction committed");

    LOG_INFO("Table writer closed");
}

const TNullable<TKeyColumns>& TTableWriter::GetKeyColumns() const
{
    return Writer->GetKeyColumns();
}

i64 TTableWriter::GetRowCount() const
{
    return Writer->GetRowCount();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
