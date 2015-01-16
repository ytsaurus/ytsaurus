#include "stdafx.h"
#include "file_reader.h"
#include "connection.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <core/ytree/ypath_proxy.h>

#include <core/logging/log.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/helpers.h>

#include <ytlib/chunk_client/chunk_replica.h>
#include <ytlib/chunk_client/chunk_spec.h>
#include <ytlib/chunk_client/read_limit.h>
#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/old_multi_chunk_sequential_reader.h>
#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/file_client/file_chunk_reader.h>
#include <ytlib/file_client/file_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

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
        if (Options_.TransactionId != NullTransactionId) {
            auto transactionManager = Client_->GetTransactionManager();
            TTransactionAttachOptions attachOptions(Options_.TransactionId);
            attachOptions.AutoAbort = false;
            Transaction_ = transactionManager->Attach(attachOptions);
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
        return BIND(&TFileReader::DoRead, MakeStrong(this))
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

private:
    IClientPtr Client_;
    TYPath Path_;
    TFileReaderOptions Options_;
    TFileReaderConfigPtr Config_;

    bool IsFirstBlock_ = true;
    bool IsFinished_ = false;

    TTransactionPtr Transaction_;

    IFileMultiChunkReaderPtr Reader_;

    NLog::TLogger Logger;


    void DoOpen()
    {
        LOG_INFO("Opening file reader");

        LOG_INFO("Fetching file info");

        auto masterChannel = Client_->GetMasterChannel(EMasterChannelKind::LeaderOrFollower);
        TObjectServiceProxy proxy(masterChannel);
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TFileYPathProxy::GetBasicAttributes(Path_);
            SetTransactionId(req, Transaction_);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            batchReq->AddRequest(req, "get_basic_attrs");
        }

        {
            auto req = TFileYPathProxy::Fetch(Path_);

            TReadLimit lowerLimit, upperLimit;
            i64 offset = Options_.Offset.Get(0);
            if (Options_.Offset) {
                lowerLimit.SetOffset(offset);
            }
            if (Options_.Length) {
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

        auto nodeDirectory = New<TNodeDirectory>();
        {
            auto rspOrError = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>("fetch");
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error fetching file chunks");
            const auto& rsp = rspOrError.Value();

            nodeDirectory->MergeFrom(rsp->node_directory());

            auto chunks = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());
            Reader_ = CreateFileMultiChunkReader(
                Config_,
                New<TMultiChunkReaderOptions>(),
                masterChannel,
                Client_->GetConnection()->GetCompressedBlockCache(),
                Client_->GetConnection()->GetUncompressedBlockCache(),
                nodeDirectory,
                std::move(chunks));
        }

        {
            WaitFor(Reader_->Open())
                .ThrowOnError();
        }

        if (Transaction_) {
            ListenTransaction(Transaction_);
        }

        LOG_INFO("File reader opened");
    }

    TSharedRef DoRead()
    {
        CheckAborted();

        TSharedRef block;
        auto endOfRead = !Reader_->ReadBlock(&block);

        if (block.Empty()) {
            if (endOfRead)
                return TSharedRef();

            WaitFor(Reader_->GetReadyEvent()).
                ThrowOnError();
            YCHECK(Reader_->ReadBlock(&block));
            YCHECK(!block.Empty());
        }

        return block;
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
