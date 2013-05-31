#include "stdafx.h"
#include "table_writer.h"
#include "config.h"
#include "private.h"
#include "table_chunk_writer.h"

#include <ytlib/misc/sync.h>
#include <ytlib/misc/nullable.h>

#include <ytlib/transaction_client/transaction.h>
#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/chunk_client/schema.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/meta_state/rpc_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NYTree;
using namespace NCypressClient;
using namespace NTransactionClient;
using namespace NChunkClient;

typedef TMultiChunkSequentialWriter<TTableChunkWriter> TTableMultiChunkWriter;

////////////////////////////////////////////////////////////////////////////////

TTableWriter::TTableWriter(
    TTableWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    TTransactionManagerPtr transactionManager,
    const NYPath::TRichYPath& richPath,
    const TNullable<TKeyColumns>& keyColumns)
    : Config(config)
    , Options(New<TTableWriterOptions>())
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionId(transaction ? transaction->GetId() : NullTransactionId)
    , TransactionManager(transactionManager)
    , RichPath(richPath)
    , IsOpen(false)
    , IsClosed(false)
    , ObjectProxy(masterChannel)
    , Logger(TableWriterLogger)
{
    YCHECK(config);
    YCHECK(masterChannel);
    YCHECK(transactionManager);

    Options->KeyColumns = keyColumns;

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~richPath.GetPath(),
        ~ToString(TransactionId)));
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
    LOG_INFO("Upload transaction created (TransactionId: %s)", ~ToString(uploadTransactionId));

    auto path = RichPath.GetPath();

    bool overwrite = NChunkClient::ExtractOverwriteFlag(RichPath.Attributes());
    bool clear = Options->KeyColumns.HasValue() || overwrite;

    LOG_INFO("Requesting table info");
    TChunkListId chunkListId;
    {
        auto batchReq = ObjectProxy.ExecuteBatch();

        {
            auto req = TCypressYPathProxy::Get(path);
            SetTransactionId(req, uploadTransactionId);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("replication_factor");
            attributeFilter.Keys.push_back("channels");
            attributeFilter.Keys.push_back("compression_codec");
            attributeFilter.Keys.push_back("erasure_codec");
            if (Options->KeyColumns.HasValue()) {
                attributeFilter.Keys.push_back("row_count");
            }
            attributeFilter.Keys.push_back("account");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            batchReq->AddRequest(req, "get_attributes");
        }

        {
            auto req = TTableYPathProxy::PrepareForUpdate(path);
            SetTransactionId(req, uploadTransactionId);
            NMetaState::GenerateMutationId(req);
            req->set_mode(clear ? NChunkClient::EUpdateMode::Overwrite : NChunkClient::EUpdateMode::Append);
            batchReq->AddRequest(req, "prepare_for_update");
        }

        auto batchRsp = batchReq->Invoke().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error requesting table info");

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting table attributes");

            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            // TODO(psushin): Keep in sync with OnInputsReceived (operation_controller_detail.cpp).
            if (Options->KeyColumns.HasValue() && !overwrite) {
                if (attributes.Get<i64>("row_count") > 0) {
                    THROW_ERROR_EXCEPTION("Cannot write sorted data into a non-empty table");
                }
            }

            Options->Channels = attributes.Get<TChannels>("channels");
            Options->ReplicationFactor = attributes.Get<int>("replication_factor");
            Options->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
            // COMPAT(babenko)
            Options->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
            Options->Account = attributes.Get<Stroka>("account");
        }

        {
            auto rsp = batchRsp->GetResponse<TTableYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error preparing table for update");
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        }
    }
    LOG_INFO("Table info received (ChunkListId: %s)",
        ~ToString(chunkListId));

    auto provider = New<TTableChunkWriterProvider>(
        Config,
        Options);

    Writer = CreateSyncWriter<TTableChunkWriter>(New<TTableMultiChunkWriter>(
        Config,
        Options,
        provider,
        MasterChannel,
        uploadTransactionId,
        chunkListId));
    Writer->Open();

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
    Writer->WriteRow(row);
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
    Writer->Close();
    LOG_INFO("Chunk writer closed");

    auto path = RichPath.GetPath();

    if (Options->KeyColumns) {
        auto keyColumns = Options->KeyColumns.Get();
        LOG_INFO("Marking table as sorted by %s", ~ConvertToYsonString(keyColumns, NYson::EYsonFormat::Text).Data());

        auto req = TTableYPathProxy::SetSorted(path);
        SetTransactionId(req, UploadTransaction);
        NMetaState::GenerateMutationId(req);
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
