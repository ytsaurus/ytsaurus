#include "stdafx.h"
#include "file_writer.h"
#include "file_chunk_writer.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction.h>

#include <ytlib/meta_state/rpc_helpers.h>

#include <ytlib/misc/sync.h>

namespace NYT {
namespace NFileClient {

using namespace NYTree;
using namespace NYPath;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TFileWriter::TFileWriter(
    TFileWriterConfigPtr config,
    NRpc::IChannelPtr masterChannel,
    ITransactionPtr transaction,
    TTransactionManagerPtr transactionManager,
    const TRichYPath& richPath)
    : Config(config)
    , MasterChannel(masterChannel)
    , Transaction(transaction)
    , TransactionManager(transactionManager)
    , RichPath(richPath.Simplify())
    , Logger(FileWriterLogger)
{
    YCHECK(transactionManager);

    Logger.AddTag(Sprintf("Path: %s, TransactionId: %s",
        ~RichPath.GetPath(),
        transaction ? ~ToString(transaction->GetId()) : ~ToString(NullTransactionId)));

    if (Transaction) {
        ListenTransaction(Transaction);
    }
}

TFileWriter::~TFileWriter()
{ }

void TFileWriter::Open()
{
    CheckAborted();

    LOG_INFO("Creating upload transaction");
    try {
        TTransactionStartOptions options;
        options.ParentId = Transaction ? Transaction->GetId() : NullTransactionId;
        options.EnableUncommittedAccounting = false;
        options.Attributes->Set("title", Sprintf("File upload to %s", ~RichPath.GetPath()));
        UploadTransaction = TransactionManager->Start(options);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error creating upload transaction") << ex;
    }

    ListenTransaction(UploadTransaction);

    LOG_INFO("Upload transaction created (TransactionId: %s)",
        ~ToString(UploadTransaction->GetId()));

    TObjectServiceProxy proxy(MasterChannel);

    LOG_INFO("Requesting file info");
    TChunkListId chunkListId;
    auto options = New<TMultiChunkWriterOptions>();
    {
        auto batchReq = proxy.ExecuteBatch();

        auto path = RichPath.GetPath();
        bool overwrite = NChunkClient::ExtractOverwriteFlag(RichPath.Attributes());

        {
            auto req = TCypressYPathProxy::Get(path);
            SetTransactionId(req, UploadTransaction);
            TAttributeFilter attributeFilter(EAttributeFilterMode::MatchingOnly);
            attributeFilter.Keys.push_back("replication_factor");
            attributeFilter.Keys.push_back("account");
            attributeFilter.Keys.push_back("compression_codec");
            attributeFilter.Keys.push_back("erasure_codec");
            ToProto(req->mutable_attribute_filter(), attributeFilter);
            batchReq->AddRequest(req, "get_attributes");
        }

        {
            auto req = TFileYPathProxy::PrepareForUpdate(path);
            req->set_mode(overwrite ? EUpdateMode::Overwrite : EUpdateMode::Append);
            NMetaState::GenerateMutationId(req);
            SetTransactionId(req, UploadTransaction);
            batchReq->AddRequest(req, "prepare_for_update");
        }

        auto batchRsp = batchReq->Invoke().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error preparing file for update");

        {
            auto rsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_attributes");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting file attributes");

            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            options->ReplicationFactor = attributes.Get<int>("replication_factor");
            options->Account = attributes.Get<Stroka>("account");
            options->CompressionCodec = attributes.Get<NCompression::ECodec>("compression_codec");
            // COMPAT(babenko)
            options->ErasureCodec = attributes.Get<NErasure::ECodec>("erasure_codec", NErasure::ECodec::None);
        }

        {
            auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspPrepareForUpdate>("prepare_for_update");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error preparing file for update");
            chunkListId = FromProto<TChunkListId>(rsp->chunk_list_id());
        }
    }

    LOG_INFO("File info received (Account: %s, ChunkListId: %s)",
        ~options->Account,
        ~ToString(chunkListId));

    auto provider = New<TFileChunkWriterProvider>(
        Config,
        options);

    Writer = New<TWriter>(
        Config,
        options,
        provider,
        MasterChannel,
        UploadTransaction->GetId(),
        chunkListId);
    Sync(~Writer, &TWriter::AsyncOpen);

}

void TFileWriter::Write(const TRef& data)
{
    CheckAborted();
    while (true) {
        auto* facade = Writer->GetCurrentWriter();
        if (!facade) {
            Sync(~Writer, &TWriter::GetReadyEvent);
        } else {
            facade->Write(data);
            break;
        }
    }
}

void TFileWriter::Close()
{
    CheckAborted();

    Sync(~Writer, &TWriter::AsyncClose);

    LOG_INFO("Committing upload transaction");
    try {
        UploadTransaction->Commit();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error committing upload transaction")
            << ex;
    }
    LOG_INFO("Upload transaction committed");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
