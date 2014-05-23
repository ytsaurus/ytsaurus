#include "stdafx.h"
#include "remote_snapshot_store.h"
#include "file_snapshot_store.h"
#include "snapshot.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>

#include <core/concurrency/scheduler.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/convert.h>
#include <core/ytree/attribute_helpers.h>

#include <core/ypath/token.h>

#include <core/logging/tagged_logger.h>

#include <ytlib/api/config.h>
#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>
#include <ytlib/api/transaction.h>
#include <ytlib/api/file_reader.h>
#include <ytlib/api/file_writer.h>

namespace NYT {
namespace NHydra {

using namespace NFS;
using namespace NConcurrency;
using namespace NYPath;
using namespace NElection;
using namespace NObjectClient;
using namespace NYTree;
using namespace NApi;

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
        IClientPtr masterClient)
        : Config_(config)
        , CellGuid_(cellGuid)
        , RemotePath_(remotePath)
        , MasterClient_(masterClient)
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
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
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
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
            .Run(maxSnapshotId);
    }

    virtual TFuture<TErrorOr<TSnapshotParams>> ConfirmSnapshot(int snapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoConfirmSnapshot, MakeStrong(this))
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

    virtual TFuture<TErrorOr<TSnapshotParams>> GetSnapshotParams(int snapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoGetSnapshotParams, MakeStrong(this))
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

private:
    TRemoteSnapshotStoreConfigPtr Config_;
    TCellGuid CellGuid_;
    TYPath RemotePath_;
    IClientPtr MasterClient_;

    NLog::TTaggedLogger Logger;


    class TReaderStream
        : public TInputStream
    {
    public:
        TReaderStream(TRemoteSnapshotStorePtr store, int snapshotId)
            : Store_(store)
            , Reader_(Store_->MasterClient_->CreateFileReader(
                Store_->GetRemotePath(snapshotId),
                TFileReaderOptions(),
                Store_->Config_->Reader))
            , CurrentOffset_(-1)
        { }

        ~TReaderStream() throw()
        { }

        void Open()
        {
            auto result = Reader_->Open().Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        virtual size_t DoRead(void* buf, size_t len) override
        {
            if (!CurrentBlock_ || CurrentOffset_ >= CurrentBlock_.Size()) {
                auto result = Reader_->Read().Get();
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                CurrentBlock_ = result.Value();
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

        IFileReaderPtr Reader_;

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


    ISnapshotReaderPtr DoCreateReader(int snapshotId)
    {
        auto params = DoGetSnapshotParams(snapshotId);
        auto reader = New<TReader>(this, snapshotId, params);
        reader->Open();
        return reader;
    }

    int DoGetLatestSnapshotId(int maxSnapshotId)
    {
        LOG_DEBUG("Requesting snapshot list from the remote store");

        auto resultOrError = WaitFor(MasterClient_->ListNodes(RemotePath_));
        if (resultOrError.GetCode() == NYTree::EErrorCode::ResolveError) {
            return NonexistingSegmentId;
        }
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        LOG_DEBUG("Snapshot list received");

        auto keys = ConvertTo<std::vector<Stroka>>(resultOrError.Value());
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
    }

    TSnapshotParams DoConfirmSnapshot(int snapshotId)
    {
        try {
            LOG_DEBUG("Uploading snapshot %d to the remote store", snapshotId);

            auto localPath = GetLocalPath(snapshotId);
            auto remotePath = GetRemotePath(snapshotId);

            auto reader = CreateFileSnapshotReader(
                localPath,
                snapshotId,
                false);
            auto params = reader->GetParams();

            LOG_DEBUG("Starting snapshot upload transaction");
            ITransactionPtr transaction;
            {
                TTransactionStartOptions options;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Sprintf("Snapshot upload for cell %s, snapshot %d",
                    ~ToString(CellGuid_),
                    snapshotId));
                options.Attributes = attributes.get();
                auto transactionOrError = WaitFor(MasterClient_->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options));
                transaction = transactionOrError.ValueOrThrow();
            }

            LOG_DEBUG("Creating snapshot node");
            {
                TCreateNodeOptions options;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("prev_record_count", params.PrevRecordCount);
                options.Attributes = attributes.get();
                auto result = WaitFor(transaction->CreateNode(
                    remotePath,
                    EObjectType::File,
                    options));
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            LOG_DEBUG("Writing snapshot data");

            auto writer = transaction->CreateFileWriter(
                remotePath,
                TFileWriterOptions(),
                Config_->Writer);

            {
                auto result = WaitFor(writer->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

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

            LOG_DEBUG("Committing snapshot upload transaction");
            {
                auto result = WaitFor(transaction->Commit());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }

            LOG_DEBUG("Snapshot uploaded successfully");
    
            DoCleanupSnapshot(snapshotId);

            return params;
        } catch (...) {
            DoCleanupSnapshot(snapshotId);
            throw;
        }
    }

    void DoCleanupSnapshot(int snapshotId)
    {
        auto localPath = GetLocalPath(snapshotId);
        if (!NFS::Remove(localPath)) {
            LOG_ERROR("Error removing temporary snapshot file");
        }
    }

    TSnapshotParams DoGetSnapshotParams(int snapshotId)
    {
        LOG_DEBUG("Requesting parameters for snapshot %d from the remote store",
            snapshotId);

        auto remotePath = GetRemotePath(snapshotId);

        TGetNodeOptions options;
        options.AttributeFilter.Mode = EAttributeFilterMode::MatchingOnly;
        options.AttributeFilter.Keys.push_back("prev_record_count");
        auto resultOrError = WaitFor(MasterClient_->GetNode(remotePath, options));
        THROW_ERROR_EXCEPTION_IF_FAILED(resultOrError);
        LOG_DEBUG("Snapshot parameters received");

        auto node = ConvertToNode(resultOrError.Value());
        const auto& attributes = node->Attributes();

        TSnapshotParams params;
        params.PrevRecordCount = attributes.Get<i64>("prev_record_count");
        params.Checksum = 0;
        params.CompressedLength = params.UncompressedLength = -1;
        return params;
    }


    Stroka GetLocalPath(int snapshotId)
    {
        return CombinePaths(
            Config_->TempPath,
            Sprintf("%s.%09d%s", ~ToString(CellGuid_), snapshotId, TempFileSuffix));
    }

    TYPath GetRemotePath(int snapshotId)
    {
        return Sprintf("%s/%09d",
            ~RemotePath_,
            snapshotId);
    }

};

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    const TCellGuid& cellGuid,
    const TYPath& remotePath,
    IClientPtr masterClient)
{
    return New<TRemoteSnapshotStore>(
        config,
        cellGuid,
        remotePath,
        masterClient);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
