#include "stdafx.h"
#include "journal_reader.h"
#include "config.h"
#include "private.h"

#include <core/logging/tagged_logger.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/journal_client/journal_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NApi {
    
using namespace NConcurrency;
using namespace NYPath;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NJournalClient;
using namespace NCypressClient;

////////////////////////////////////////////////////////////////////////////////

class TJournalReader
    : public IJournalReader
{
public:
    TJournalReader(
        IClientPtr client,
        const TYPath& path,
        const TJournalReaderOptions& options,
        TJournalReaderConfigPtr config)
        : Client_(client)
        , Path_(path)
        , Options_(options)
        , Config_(config ? config : New<TJournalReaderConfig>())
        //, IsFirstBlock_(true)
        //, IsFinished_(false)
        //, Size_(0)
        , Logger(ApiLogger)
    {
        Logger.AddTag(Sprintf("Path: %s",
            ~Path_));
    }

    virtual TAsyncError Open() override
    {
        return BIND(&TJournalReader::DoOpen, MakeStrong(this))
            .Guarded()
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TFuture<TErrorOr<std::vector<TSharedRef>>> Read() override
    {
        return BIND(&TJournalReader::DoRead, MakeStrong(this))
            .Guarded()
            .AsyncVia(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

private:
    IClientPtr Client_;
    TYPath Path_;
    TJournalReaderOptions Options_;
    TJournalReaderConfigPtr Config_;

    //bool IsFirstBlock_;
    //bool IsFinished_;

    //TTransactionPtr Transaction_;

    //typedef TMultiChunkSequentialReader<TFileChunkReader> TReader;
    //TIntrusivePtr<TReader> Reader_;

    //i64 Size_;

    NLog::TTaggedLogger Logger;


    void DoOpen()
    {
        LOG_INFO("Opening journal reader");

        LOG_INFO("Fetching journal info");

        TObjectServiceProxy proxy(Client_->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TJournalYPathProxy::GetBasicAttributes(Path_);
            batchReq->AddRequest(req, "get_attrs");
        }

        {
            auto req = TJournalYPathProxy::Fetch(Path_);
            i64 firstRecordIndex = Options_.FirstRecordIndex.Get(0);
            if (Options_.FirstRecordIndex) {
                req->mutable_lower_limit()->set_record_index(firstRecordIndex);
            }
            if (Options_.RecordCount) {
                req->mutable_upper_limit()->set_offset(firstRecordIndex + *Options_.RecordCount);
            }
            SetSuppressAccessTracking(req, Options_.SuppressAccessTracking);
            req->add_extension_tags(TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value);
            batchReq->AddRequest(req, "fetch");
        }

        auto batchRsp = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(*batchRsp, "Error fetching journal info");

        {
            auto rsp = batchRsp->GetResponse<TJournalYPathProxy::TRspGetBasicAttributes>("get_attrs");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error getting object attributes");

            auto type = EObjectType(rsp->type());
            if (type != EObjectType::Journal) {
                THROW_ERROR_EXCEPTION("Invalid type of %s: expected %s, actual %s",
                    ~Path_,
                    ~FormatEnum(EObjectType(EObjectType::File)).Quote(),
                    ~FormatEnum(type).Quote());
            }
        }

        auto nodeDirectory = New<TNodeDirectory>();
        {
            auto rsp = batchRsp->GetResponse<TJournalYPathProxy::TRspFetch>("fetch");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching journal chunks");

            nodeDirectory->MergeFrom(rsp->node_directory());

            auto chunks = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());
            //for (const auto& chunk : chunks) {
            //    i64 dataSize;
            //    GetStatistics(chunk, &dataSize);
            //    Size_ += dataSize;
            //}

            //auto provider = New<TFileChunkReaderProvider>(Config_);
            //Reader_ = New<TReader>(
            //    Config_,
            //    Client_->GetMasterChannel(),
            //    Client_->GetConnection()->GetBlockCache(),
            //    nodeDirectory,
            //    std::move(chunks),
            //    provider);
        }

        //{
        //    auto result = WaitFor(Reader_->AsyncOpen());
        //    THROW_ERROR_EXCEPTION_IF_FAILED(result);
        //}

        LOG_INFO("Journal reader opened");
    }

    std::vector<TSharedRef> DoRead()
    {
        YUNIMPLEMENTED();
        //CheckAborted();

        //if (IsFinished_) {
        //    return TSharedRef();
        //}
        //
        //if (!IsFirstBlock_ && !Reader_->FetchNext()) {
        //    auto result = WaitFor(Reader_->GetReadyEvent());
        //    THROW_ERROR_EXCEPTION_IF_FAILED(result);
        //}

        //IsFirstBlock_ = false;

        //auto* facade = Reader_->GetFacade();
        //if (facade) {
        //    return facade->GetBlock();
        //} else {
        //    IsFinished_ = true;
        //    return TSharedRef();
        //}
    }

};

IJournalReaderPtr CreateJournalReader(
    IClientPtr client,
    const TYPath& path,
    const TJournalReaderOptions& options,
    TJournalReaderConfigPtr config)
{
    return New<TJournalReader>(
        client,
        path,
        options,
        config);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
