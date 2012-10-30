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
    const NYPath::TRichYPath& richPath,
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
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(transactionManager);

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
        THROW_ERROR_EXCEPTION("Error creating upload transaction")
            << ex;
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

        if (KeyColumns.HasValue() || RichPath.Attributes().Get<bool>("overwrite", false)) {
            auto req = TTableYPathProxy::Clear(path);
            SetTransactionId(req, uploadTransactionId);
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "clear");
        }

        {
            auto req = TCypressYPathProxy::Get(path);
            SetTransactionId(req, TransactionId);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("replication_factor");
            attributeFilter.Keys.push_back("channels");
            if (KeyColumns.HasValue()) {
                attributeFilter.Keys.push_back("row_count");
            }
            *req->mutable_attribute_filter() = ToProto(attributeFilter);
            batchReq->AddRequest(req, "get_attributes");
        }

        {
            auto req = TTableYPathProxy::GetChunkListForUpdate(path);
            SetTransactionId(req, uploadTransactionId);
            NMetaState::GenerateRpcMutationId(req);
            batchReq->AddRequest(req, "get_chunk_list_for_update");
        }

        auto batchRsp = batchReq->Invoke().Get();
        if (!batchRsp->IsOK()) {
            THROW_ERROR_EXCEPTION("Error requesting table info")
                << batchRsp->GetError();
        }

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            if (!rsp->IsOK()) {
                THROW_ERROR_EXCEPTION("Error requesting table attributes")
                    << rsp->GetError();
            }

            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            if (KeyColumns.HasValue()) {
                i64 rowCount = attributes.Get<i64>("row_count");
                if (rowCount > 0) {
                    THROW_ERROR_EXCEPTION("Cannot write sorted data into a non-empty table");
                }
            }
            
            auto channelsYson = attributes.FindYson("channels");
            if (channelsYson) {
                channels = ChannelsFromYson(channelsYson.Get());
            }

            // COMPAT(babenko): eliminate default value
            Config->ReplicationFactor = attributes.Get<int>("replication_factor", 3);
        }
        
        if (KeyColumns.HasValue()) {
            {
                auto rsp = batchRsp->FindResponse("clear");
                if (rsp && !rsp->IsOK()) {
                    THROW_ERROR_EXCEPTION("Error clearing table")
                        << rsp->GetError();
                }
            }
        }

        {
            auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspGetChunkListForUpdate>("get_chunk_list_for_update");
            if (!rsp->IsOK()) {
                THROW_ERROR_EXCEPTION("Error requesting chunk list id")
                    << rsp->GetError();
            }
            chunkListId = TChunkListId::FromProto(rsp->chunk_list_id());
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
        
        auto req = TTableYPathProxy::SetSorted(path);
        SetTransactionId(req, UploadTransaction);
        NMetaState::GenerateRpcMutationId(req);
        ToProto(req->mutable_key_columns(), keyColumns);

        auto rsp = ObjectProxy.Execute(req).Get();
        if (!rsp->IsOK()) {
            THROW_ERROR_EXCEPTION("Error marking table as sorted")
                << rsp->GetError();
        }
        LOG_INFO("Table is marked as sorted");
    }

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error committing upload transaction")
            << ex;
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
