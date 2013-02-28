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
    ITransactionPtr transaction,
    TTransactionManagerPtr transactionManager,
    const NYPath::TRichYPath& richPath,
    const TNullable<TKeyColumns>& keyColumns)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , RichPath(richPath)
    , KeyColumns(keyColumns)
    , IsOpen(false)
    , IsClosed(false)
    , ObjectProxy(masterChannel)
    , Logger(TableWriterLogger)
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
        TTransactionStartOptions options;
        options.ParentId = TransactionId;
        options.EnableUncommittedAccounting = false;
        options.Attributes->Set("title", Sprintf("Table upload to %s", ~RichPath.GetPath()));
        UploadTransaction = TransactionManager->Start(options);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error creating upload transaction")
            << ex;
    }
    auto uploadTransactionId = UploadTransaction->GetId();
    ListenTransaction(UploadTransaction);
    LOG_INFO("Upload transaction created (TransactionId: %s)", ~uploadTransactionId.ToString());

    auto path = RichPath.GetPath();
    bool overwrite = RichPath.Attributes().Get<bool>("overwrite", false);
    bool clear = KeyColumns.HasValue() || overwrite;

    LOG_INFO("Requesting table info");
    TChunkListId chunkListId;
    TChannels channels;
    Stroka account;
    {
        auto batchReq = ObjectProxy.ExecuteBatch();

        {
            auto req = TCypressYPathProxy::Get(path);
            SetTransactionId(req, TransactionId);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("replication_factor");
            attributeFilter.Keys.push_back("channels");
            if (KeyColumns.HasValue()) {
                attributeFilter.Keys.push_back("row_count");
            }
            attributeFilter.Keys.push_back("account");
            *req->mutable_attribute_filter() = ToProto(attributeFilter);
            batchReq->AddRequest(req, "get_attributes");
        }

        {
            auto req = TTableYPathProxy::PrepareForUpdate(path);
            SetTransactionId(req, uploadTransactionId);
            NMetaState::GenerateRpcMutationId(req);
            req->set_mode(clear ? ETableUpdateMode::Overwrite : ETableUpdateMode::Append);
            batchReq->AddRequest(req, "prepare_for_update");
        }

        auto batchRsp = batchReq->Invoke().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting table info");

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting table attributes");

            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            if (KeyColumns.HasValue() && !overwrite) {
                if (attributes.Get<i64>("row_count") > 0) {
                    THROW_ERROR_EXCEPTION("Cannot write sorted data into a non-empty table");
                }
            }

            auto channelsYson = attributes.FindYson("channels");
            if (channelsYson) {
                channels = ConvertTo<TChannels>(channelsYson.Get());
            }

            Config->ReplicationFactor = attributes.Get<int>("replication_factor");

            account = attributes.Get<Stroka>("account");
        }

        {
            auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error preparing table for update");
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
        account,
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
        LOG_INFO("Marking table as sorted by %s", ~ConvertToYsonString(keyColumns, NYson::EYsonFormat::Text).Data());

        auto req = TTableYPathProxy::SetSorted(path);
        SetTransactionId(req, UploadTransaction);
        NMetaState::GenerateRpcMutationId(req);
        ToProto(req->mutable_key_columns(), keyColumns);

        auto rsp = ObjectProxy.Execute(req).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error marking table as sorted");

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
