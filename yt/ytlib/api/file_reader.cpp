#include "file_reader.h"
#include "private.h"
#include "config.h"
#include "connection.h"

#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/dispatcher.h>
#include <yt/ytlib/chunk_client/read_limit.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/ytlib/file_client/file_chunk_reader.h>
#include <yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/transaction_client/helpers.h>
#include <yt/ytlib/transaction_client/transaction_listener.h>
#include <yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NApi {

using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NYPath;
using namespace NChunkClient;
using namespace NFileClient;
using namespace NTransactionClient;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

class TFileReader
    : public TTransactionListener
    , public IFileReader
{
public:
    TFileReader(
        IClientPtr client,
        const TYPath& path,
        const TFileReaderOptions& options)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(options.Config ? options.Config : New<TFileReaderConfig>())
        , Logger(ApiLogger)
    {
        if (Options_.TransactionId) {
            auto transactionManager = Client_->GetTransactionManager();
            Transaction_ = transactionManager->Attach(Options_.TransactionId);
        }


        Logger.AddTag("Path: %v, TransactionId: %v",
            Path_,
            Options_.TransactionId);
    }

    virtual TFuture<void> Open() override
    {
        return BIND(&TFileReader::DoOpen, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TFuture<TSharedRef> Read() override
    {
        ValidateAborted();

        TSharedRef block;
        if (!Reader_ || !Reader_->ReadBlock(&block)) {
            return MakeFuture(TSharedRef());
        }

        if (block) {
            return MakeFuture(block);
        }

        return Reader_->GetReadyEvent().Apply(
            BIND(&TFileReader::Read, MakeStrong(this)));
    }

private:
    const IClientPtr Client_;
    const TYPath Path_;
    const TFileReaderOptions Options_;
    const TFileReaderConfigPtr Config_;

    TTransactionPtr Transaction_;

    IFileMultiChunkReaderPtr Reader_;

    NLogging::TLogger Logger;


    void DoOpen()
    {
        LOG_INFO("Opening file reader");

        LOG_INFO("Fetching file info");

        bool isEmptyRead = Options_.Length && *Options_.Length == 0;

        auto masterChannel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
        TObjectServiceProxy proxy(masterChannel);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TFileYPathProxy::GetBasicAttributes(Path_);
            SetTransactionId(req, Transaction_);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            batchReq->AddRequest(req, "get_basic_attrs");
        }

        if (!isEmptyRead) {
            auto req = TFileYPathProxy::Fetch(Path_);

            TReadLimit lowerLimit, upperLimit;
            i64 offset = Options_.Offset.Get(0);
            if (Options_.Offset) {
                lowerLimit.SetOffset(offset);
            }
            if (Options_.Length) {
                if (*Options_.Length < 0) {
                    THROW_ERROR_EXCEPTION("Invalid length to read from file: %v < 0", *Options_.Length);
                }
                upperLimit.SetOffset(offset + *Options_.Length);
            }

            ToProto(req->mutable_ranges(), std::vector<TReadRange>({TReadRange(lowerLimit, upperLimit)}));

            SetTransactionId(req, Transaction_);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            batchReq->AddRequest(req, "fetch");
        }

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(batchRspOrError, "Error fetching file info");
        const auto& batchRsp = batchRspOrError.Value();

        {
            auto rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspGetBasicAttributes>("get_basic_attrs");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error getting object attributes");
            const auto& rsp = rspOrError.Value();

            auto type = EObjectType(rsp->type());
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType::File,
                    type);
            }
        }

        if (!isEmptyRead) {
            auto nodeDirectory = New<TNodeDirectory>();
            auto rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>("fetch");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching file chunks");
            const auto& rsp = rspOrError.Value();

            nodeDirectory->MergeFrom(rsp->node_directory());

            auto chunks = FromProto<NChunkClient::NProto::TChunkSpec>(std::move(*rsp->mutable_chunks()));

            Reader_ = CreateFileMultiChunkReader(
                Config_,
                New<TMultiChunkReaderOptions>(),
                Client_,
                Client_->GetConnection()->GetBlockCache(),
                nodeDirectory,
                std::move(chunks));
            WaitFor(Reader_->Open())
                .ThrowOnError();
        }

        if (Transaction_) {
            ListenTransaction(Transaction_);
        }

        LOG_INFO("File reader opened");
    }

};

IFileReaderPtr CreateFileReader(
    IClientPtr client,
    const TYPath& path,
    const TFileReaderOptions& options)
{
    return New<TFileReader>(client, path, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
