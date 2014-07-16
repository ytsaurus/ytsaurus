#include "stdafx.h"
#include "journal_reader.h"
#include "config.h"
#include "connection.h"
#include "private.h"

#include <core/logging/log.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/rpc_helpers.h>

#include <ytlib/transaction_client/transaction_manager.h>
#include <ytlib/transaction_client/transaction_listener.h>
#include <ytlib/transaction_client/helpers.h>

#include <ytlib/chunk_client/chunk_meta_extensions.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/chunk_client/reader.h>
#include <ytlib/chunk_client/replication_reader.h>
#include <ytlib/chunk_client/read_limit.h>

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
using namespace NTransactionClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TJournalReader
    : public TTransactionListener
    , public IJournalReader
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

    TTransactionPtr Transaction_;

    TNodeDirectoryPtr NodeDirectory_ = New<TNodeDirectory>();
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs_;

    int CurrentChunkIndex_ = -1;
    bool Finished_ = false;
    IReaderPtr CurrentChunkReader_;

    // Inside current chunk.
    int BeginRecordIndex_ = -1;
    int CurrentRecordIndex_ = -1;
    int EndRecordIndex_ = -1; // exclusive

    NLog::TLogger Logger;


    void DoOpen()
    {
        LOG_INFO("Opening journal reader");

        LOG_INFO("Fetching journal info");

        TObjectServiceProxy proxy(Client_->GetMasterChannel());
        auto batchReq = proxy.ExecuteBatch();

        {
            auto req = TJournalYPathProxy::GetBasicAttributes(Path_);
            SetTransactionId(req, Transaction_);
            batchReq->AddRequest(req, "get_attrs");
        }

        {
            auto req = TJournalYPathProxy::Fetch(Path_);
            i64 firstRecordIndex = Options_.FirstRecordIndex.Get(0);
            if (Options_.FirstRecordIndex) {
                req->mutable_lower_limit()->set_record_index(firstRecordIndex);
            }
            if (Options_.RecordCount) {
                req->mutable_upper_limit()->set_record_index(firstRecordIndex + *Options_.RecordCount);
            }
            SetTransactionId(req, Transaction_);
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

        {
            auto rsp = batchRsp->GetResponse<TJournalYPathProxy::TRspFetch>("fetch");
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp, "Error fetching journal chunks");

            NodeDirectory_->MergeFrom(rsp->node_directory());

            ChunkSpecs_ = FromProto<NChunkClient::NProto::TChunkSpec>(rsp->chunks());
        }

        if (Transaction_) {
            ListenTransaction(Transaction_);
        }

        LOG_INFO("Journal reader opened");
    }

    std::vector<TSharedRef> DoRead()
    {
        while (true) {
            CheckAborted();

            if (Finished_) {
                return std::vector<TSharedRef>();
            }

            if (!CurrentChunkReader_) {
                if (++CurrentChunkIndex_ >= ChunkSpecs_.size()) {
                    Finished_ = true;
                    return std::vector<TSharedRef>();
                }

                const auto& chunkSpec = ChunkSpecs_[CurrentChunkIndex_];

                auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
                auto replicas = FromProto<TChunkReplica, TChunkReplicaList>(chunkSpec.replicas());
                CurrentChunkReader_ = CreateReplicationReader(
                    Config_,
                    Client_->GetConnection()->GetBlockCache(),
                    Client_->GetMasterChannel(),
                    NodeDirectory_,
                    Null,
                    chunkId,
                    replicas);

                auto lowerLimit = FromProto<TReadLimit>(chunkSpec.lower_limit());
                BeginRecordIndex_ = lowerLimit.HasRecordIndex() ? lowerLimit.GetRecordIndex() : 0;

                auto upperLimit = FromProto<TReadLimit>(chunkSpec.upper_limit());
                EndRecordIndex_ = upperLimit.HasRecordIndex() ? upperLimit.GetRecordIndex() : std::numeric_limits<int>::max();

                CurrentRecordIndex_ = BeginRecordIndex_;
            }

            auto recordsOrError = WaitFor(CurrentChunkReader_->ReadBlocks(
                CurrentRecordIndex_,
                EndRecordIndex_ - CurrentRecordIndex_));
            THROW_ERROR_EXCEPTION_IF_FAILED(recordsOrError);

            const auto& records = recordsOrError.Value();
            if (!records.empty()) {
                CurrentRecordIndex_ += records.size();
                return records;
            }

            CurrentChunkReader_.Reset();
        }
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
