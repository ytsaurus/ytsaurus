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
        const TFileReaderOptions& options,
        TFileReaderConfigPtr config)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(config ? config : New<TFileReaderConfig>())
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

    virtual TAsyncError Open() override
    {
        return BIND(&TFileReader::DoOpen, MakeStrong(this))
            .Guarded()
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TFuture<TErrorOr<TSharedRef>> Read() override
    {
        return BIND(&TFileReader::DoRead, MakeStrong(this))
            .Guarded()
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

    typedef TOldMultiChunkSequentialReader<TFileChunkReader> TReader;
    TIntrusivePtr<TReader> Reader_;

    NLog::TLogger Logger;


    void DoOpen()
    {
        LOG_INFO("Opening file reader");

        LOG_INFO("Fetching file info");

        TObjectServiceProxy proxy(Client_->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TFileYPathProxy::GetBasicAttributes(Path_);
            SetTransactionId(req, Transaction_);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            batchReq->AddRequest(req, "get_basic_attrs");
        }

        {
            auto req = TFileYPathProxy::Fetch(Path_);
            i64 offset = Options_.Offset.Get(0);
            if (Options_.Offset) {
                req->mutable_lower_limit()->set_offset(offset);
            }
            if (Options_.Length) {
                req->mutable_upper_limit()->set_offset(offset + *Options_.Length);
            }
            SetTransactionId(req, Transaction_);
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            batchReq->AddRequest(req, "fetch");
        }

        auto batchRsp = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error fetching file info");

        {
            auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspGetBasicAttributes>("get_basic_attrs");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting object attributes");

            auto type = EObjectType(rsp->type());
            if (type != EObjectType::File) {
                THROW_ERROR_EXCEPTION("Invalid type of %v: expected %Qlv, actual %Qlv",
                    Path_,
                    EObjectType(EObjectType::File),
                    type);
            }
        }

        auto nodeDirectory = New<TNodeDirectory>();
        {
            auto rsp = batchRsp->GetResponse<TFileYPathProxy::TRspFetch>("fetch");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching file chunks");

            nodeDirectory->MergeFrom(rsp->node_directory());

            auto chunks = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());
            auto provider = New<TFileChunkReaderProvider>(
                Config_,
                Client_->GetConnection()->GetUncompressedBlockCache());
            Reader_ = New<TReader>(
                Config_,
                Client_->GetMasterChannel(),
                Client_->GetConnection()->GetCompressedBlockCache(),
                nodeDirectory,
                std::move(chunks),
                provider);
        }

        {
            auto result = WaitFor(Reader_->AsyncOpen());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        if (Transaction_) {
            ListenTransaction(Transaction_);
        }

        LOG_INFO("File reader opened");
    }

    TSharedRef DoRead()
    {
        CheckAborted();

        if (IsFinished_) {
            return TSharedRef();
        }
        
        if (!IsFirstBlock_ && !Reader_->FetchNext()) {
            auto result = WaitFor(Reader_->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        IsFirstBlock_ = false;

        auto* facade = Reader_->GetFacade();
        if (facade) {
            return facade->GetBlock();
        } else {
            IsFinished_ = true;
            return TSharedRef();
        }
    }

};

IFileReaderPtr CreateFileReader(
    IClientPtr client,
    const TYPath& path,
    const TFileReaderOptions& options,
    TFileReaderConfigPtr config)
{
    YCHECK(client);

    return New<TFileReader>(
        client,
        path,
        options,
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
