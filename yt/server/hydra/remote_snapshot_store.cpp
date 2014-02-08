#include "stdafx.h"
#include "remote_snapshot_store.h"
#include "file_snapshot_store.h"
#include "snapshot.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/concurrency/fiber.h>

#include <core/rpc/channel.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/convert.h>
#include <core/ytree/attribute_helpers.h>

#include <core/ypath/token.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/chunk_client/block_cache.h>
#include <ytlib/chunk_client/client_block_cache.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/transaction_client/transaction_manager.h>

#include <ytlib/file_client/file_reader.h>
#include <ytlib/file_client/file_writer.h>

namespace NYT {
namespace NHydra {

using namespace NFS;
using namespace NConcurrency;
using namespace NYPath;
using namespace NRpc;
using namespace NElection;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NYTree;
using namespace NChunkClient;
using namespace NFileClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TRemoteSnapshotStore;
typedef TIntrusivePtr<TRemoteSnapshotStore> TRemoteSnapshotStorePtr;

class TRemoteSnapshotStore
    : public ISnapshotStore
{
public:
    TRemoteSnapshotStore(
        TRemoteSnapshotStoreConfigPtr config,
        const TCellGuid& cellGuid,
        const TYPath& remotePath,
        IChannelPtr masterChannel,
        TTransactionManagerPtr transactionManager)
        : Config_(config)
        , CellGuid_(cellGuid)
        , RemotePath_(remotePath)
        , MasterChannel_(masterChannel)
        , TransactionManager_(transactionManager)
        , Proxy_(MasterChannel_)
        , Logger(HydraLogger)
    {
        Logger.AddTag(Sprintf("CellGuid: %s", ~ToString(CellGuid_)));
    }

    virtual const TCellGuid& GetCellGuid() const override
    {
        return CellGuid_;
    }

    virtual TFuture<TErrorOr<ISnapshotReaderPtr>> CreateReader(int snapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoCreateReader, MakeStrong(this))
            .AsyncVia(HydraIOQueue->GetInvoker())
            .Run(snapshotId);
    }

    virtual ISnapshotWriterPtr CreateWriter(
        int snapshotId,
        const TSnapshotCreateParams& params) override
    {
        return CreateFileSnapshotWriter(
            GetLocalPath(snapshotId),
            NCompression::ECodec::None,
            snapshotId,
            params,
            false);
    }

    virtual TFuture<TErrorOr<int>> GetLatestSnapshotId(int maxSnapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoGetLatestSnapshotId, MakeStrong(this))
            .AsyncVia(HydraIOQueue->GetInvoker())
            .Run(maxSnapshotId);
    }

    virtual TFuture<TErrorOr<TSnapshotParams>> ConfirmSnapshot(int snapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoConfirmSnapshot, MakeStrong(this))
            .AsyncVia(HydraIOQueue->GetInvoker())
            .Run(snapshotId);
    }

    virtual TFuture<TErrorOr<TSnapshotParams>> GetSnapshotParams(int snapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoGetSnapshotParams, MakeStrong(this))
            .AsyncVia(HydraIOQueue->GetInvoker())
            .Run(snapshotId);
    }

private:
    TRemoteSnapshotStoreConfigPtr Config_;
    TCellGuid CellGuid_;
    TYPath RemotePath_;
    IChannelPtr MasterChannel_;
    TTransactionManagerPtr TransactionManager_;

    TObjectServiceProxy Proxy_;

    NLog::TTaggedLogger Logger;


    class TReaderStream
        : public TInputStream
    {
    public:
        TReaderStream(TRemoteSnapshotStorePtr store, int snapshotId)
            : Store_(store)
            , SnapshotId_(snapshotId)
            , Reader_(New<NFileClient::TAsyncReader>(
                Store_->Config_->Reader,
                Store_->MasterChannel_,
                CreateClientBlockCache(New<TClientBlockCacheConfig>()),
                nullptr,
                Store_->GetRemotePath(snapshotId)))
            , CurrentOffset_(-1)
        { }

        void Open()
        {
            auto result = WaitFor(Reader_->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        virtual size_t DoRead(void* buf, size_t len) override
        {
            if (!CurrentBlock_ || CurrentOffset_ >= CurrentBlock_.Size()) {
                auto result = WaitFor(Reader_->Read());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                CurrentBlock_ = result.GetValue();
                if (!CurrentBlock_) {
                    return 0;
                }
                CurrentOffset_ = 0;
            }

            size_t bytesToCopy = std::min(len, CurrentBlock_.Size() - CurrentOffset_);
            std::copy(
                CurrentBlock_.Begin() + CurrentOffset_,
                CurrentBlock_.Begin() + CurrentOffset_ + bytesToCopy,
                static_cast<char*>(buf));
            CurrentOffset_ += bytesToCopy;

            return bytesToCopy;
        }

    private:
        TRemoteSnapshotStorePtr Store_;
        int SnapshotId_;

        NFileClient::TAsyncReaderPtr Reader_;

        TSharedRef CurrentBlock_;
        size_t CurrentOffset_;

    };

    class TReader
        : public ISnapshotReader
    {
    public:
        TReader(TRemoteSnapshotStorePtr store, int snapshotId, const TSnapshotParams& params)
            : Stream_(store, snapshotId)
            , Params_(params)
        { }

        void Open()
        {
            Stream_.Open();
        }

        virtual TInputStream* GetStream() override
        {
            return &Stream_;
        }

        virtual TSnapshotParams GetParams() const override
        {
            return Params_;
        }

    private:
        TReaderStream Stream_;
        TSnapshotParams Params_;

    };


    TErrorOr<ISnapshotReaderPtr> DoCreateReader(int snapshotId)
    {
        try {
            auto params = DoGetSnapshotParams(snapshotId).GetValueOrThrow();
            auto reader = New<TReader>(this, snapshotId, params);
            reader->Open();
            return reader;
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<int> DoGetLatestSnapshotId(int maxSnapshotId)
    {
        try {
            LOG_DEBUG("Requesting snapshot list from the remote store");
            auto req = TYPathProxy::List(RemotePath_);

            auto rsp = WaitFor(Proxy_.Execute(req));
            if (rsp->GetError().GetCode() == NYTree::EErrorCode::ResolveError) {
                return NonexistingSegmentId;
            }
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
            LOG_DEBUG("Snapshot list received");

            auto keys = ConvertTo<std::vector<Stroka>>(TYsonString(rsp->keys()));
            int lastestSnapshotId = NonexistingSegmentId;
            for (const auto& key : keys) {
                try {
                    int id = FromString<int>(key);
                    if (id <= maxSnapshotId && id > lastestSnapshotId) {
                        lastestSnapshotId = id;
                    }
                } catch (const std::exception& ex) {
                    LOG_WARNING("Unrecognized item %s in remote snapshot store",
                        ~key.Quote());
                }
            }

            return lastestSnapshotId;
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TSnapshotParams> DoConfirmSnapshot(int snapshotId)
    {
        try {
            LOG_DEBUG("Uploading snapshot %d to the remote store", snapshotId);

            auto reader = CreateFileSnapshotReader(
                GetLocalPath(snapshotId),
                snapshotId,
                false);
            auto params = reader->GetParams();

            LOG_DEBUG("Starting transaction");
            TTransactionPtr transaction;
            {
                TTransactionStartOptions options;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Sprintf("Snapshot upload for cell %s, snapshot %d",
                    CellGuid_,
                    snapshotId));
                options.Attributes = attributes.get();
                auto transactionOrError = WaitFor(TransactionManager_->Start(options));
                transaction = transactionOrError.GetValueOrThrow();
            }

            LOG_DEBUG("Creating snapshot node");
            {
                auto req = TCypressYPathProxy::Create(GetRemotePath(snapshotId));
                req->set_type(EObjectType::File);
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("prev_record_count", params.PrevRecordCount);
                ToProto(req->mutable_node_attributes(), *attributes);
                auto rsp = WaitFor(Proxy_.Execute(req));
                THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
            }

            LOG_DEBUG("Writing snapshot data");

            auto writer = New<NFileClient::TAsyncWriter>(
                Config_->Writer,
                MasterChannel_,
                transaction,
                TransactionManager_,
                GetRemotePath(snapshotId));

            {
                auto result = WaitFor(writer->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            const size_t BufferSize = 1;
            struct TUploadBufferTag { };
            auto buffer = TSharedRef::Allocate<TUploadBufferTag>(Config_->Writer->BlockSize);

            while (true) {
                size_t bytesRead = reader->GetStream()->Read(buffer.Begin(), buffer.Size());
                if (bytesRead == 0)
                    break;
                auto result = WaitFor(writer->Write(TRef(buffer.Begin(), bytesRead)));
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            {
                auto result = WaitFor(writer->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            LOG_DEBUG("Committing transaction");
            {
                auto result = WaitFor(transaction->Commit());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            LOG_DEBUG("Snapshot uploaded successfully");

            return params;
        } catch (const std::exception& ex) {
            return ex;
        }
    }

    TErrorOr<TSnapshotParams> DoGetSnapshotParams(int snapshotId)
    {
        try {
            LOG_DEBUG("Requesting parameters for snapshot %d from the remote store",
                snapshotId);
            auto req = TYPathProxy::Get(GetRemotePath(snapshotId));
            TAttributeFilter filter(EAttributeFilterMode::MatchingOnly);
            filter.Keys.push_back("prev_record_count");
            ToProto(req->mutable_attribute_filter(), filter);

            auto rsp = WaitFor(Proxy_.Execute(req));
            THROW_ERROR_EXCEPTION_IF_FAILED(*rsp);
            LOG_DEBUG("Snapshot parameters received");

            auto node = ConvertToNode(TYsonString(rsp->value()));
            const auto& attributes = node->Attributes();

            TSnapshotParams params;
            params.PrevRecordCount = attributes.Get<i64>("prev_record_count");
            params.Checksum = 0;
            params.CompressedLength = params.UncompressedLength = -1;
            return params;
        } catch (const std::exception& ex) {
            return ex;
        }
    }


    Stroka GetLocalPath(int snapshotId)
    {
        return CombinePaths(
            Config_->TempPath,
            Sprintf("%s.%d%s", ~ToString(CellGuid_), snapshotId, TempFileSuffix));
    }

    TYPath GetRemotePath(int snapshotId)
    {
        return RemotePath_ + "/" + ToYPathLiteral(snapshotId);
    }

};

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    const TCellGuid& cellGuid,
    const TYPath& remotePath,
    IChannelPtr masterChannel,
    TTransactionManagerPtr transactionManager)
{
    return New<TRemoteSnapshotStore>(
        config,
        cellGuid,
        remotePath,
        masterChannel,
        transactionManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
