#include "stdafx.h"
#include "remote_snapshot_store.h"
#include "file_snapshot_store.h"
#include "snapshot.h"
#include "config.h"
#include "private.h"

#include <core/misc/fs.h>
#include <core/misc/checkpointable_stream.h>

#include <core/concurrency/scheduler.h>

#include <core/ytree/ypath_proxy.h>
#include <core/ytree/convert.h>
#include <core/ytree/attribute_helpers.h>

#include <core/ypath/token.h>

#include <core/logging/log.h>

#include <ytlib/api/config.h>
#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>
#include <ytlib/api/transaction.h>
#include <ytlib/api/file_reader.h>
#include <ytlib/api/file_writer.h>

#include <ytlib/hydra/hydra_manager.pb.h>

namespace NYT {
namespace NHydra {

using namespace NFS;
using namespace NConcurrency;
using namespace NYPath;
using namespace NElection;
using namespace NObjectClient;
using namespace NYTree;
using namespace NApi;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TRemoteSnapshotStore;
typedef TIntrusivePtr<TRemoteSnapshotStore> TRemoteSnapshotStorePtr;

class TRemoteSnapshotStore
    : public ISnapshotStore
{
public:
    TRemoteSnapshotStore(
        TRemoteSnapshotStoreConfigPtr config,
        TRemoteSnapshotStoreOptionsPtr options,
        const TYPath& remotePath,
        IClientPtr masterClient)
        : Config_(config)
        , Options_(options)
        , RemotePath_(remotePath)
        , MasterClient_(masterClient)
        , Logger(HydraLogger)
    {
        Logger.AddTag("Path: %v", RemotePath_);
    }

    virtual TFuture<TErrorOr<ISnapshotReaderPtr>> CreateReader(int snapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoCreateReader, MakeStrong(this))
            .Guarded()
            .AsyncVia(GetHydraIOInvoker())
            .Run(snapshotId);
    }

    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const TSharedRef& meta) override
    {
        return CreateFileSnapshotWriter(
            GetLocalPath(snapshotId),
            NCompression::ECodec::None,
            snapshotId,
            meta,
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
    TRemoteSnapshotStoreOptionsPtr Options_;
    TYPath RemotePath_;
    IClientPtr MasterClient_;

    NLog::TLogger Logger;


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
            : UnderlyingInput_(store, snapshotId)
            , FacadeInput_(CreateCheckpointableInputStream(&UnderlyingInput_))
            , Params_(params)
        { }

        void Open()
        {
            UnderlyingInput_.Open();
        }

        virtual ICheckpointableInputStream* GetStream() override
        {
            return FacadeInput_.get();
        }

        virtual TSnapshotParams GetParams() const override
        {
            return Params_;
        }

    private:
        TReaderStream UnderlyingInput_;
        std::unique_ptr<ICheckpointableInputStream> FacadeInput_;
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
        LOG_DEBUG("Requesting snapshot list from remote store");
        auto result = WaitFor(MasterClient_->ListNodes(RemotePath_));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        LOG_DEBUG("Snapshot list received");

        auto keys = ConvertTo<std::vector<Stroka>>(result.Value());
        int lastestSnapshotId = NonexistingSegmentId;
        for (const auto& key : keys) {
            try {
                int id = FromString<int>(key);
                if (id <= maxSnapshotId && id > lastestSnapshotId) {
                    lastestSnapshotId = id;
                }
            } catch (const std::exception& ex) {
                LOG_WARNING("Unrecognized item %s in remote store %s",
                    ~key.Quote(),
                    ~RemotePath_);
            }
        }

        return lastestSnapshotId;
    }

    TSnapshotParams DoConfirmSnapshot(int snapshotId)
    {
        try {
            LOG_DEBUG("Uploading snapshot %d to remote store", snapshotId);

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
                attributes->Set("title", Format("Snapshot upload to %v",
                    remotePath));
                options.Attributes = attributes.get();
                auto transactionOrError = WaitFor(MasterClient_->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options));
                transaction = transactionOrError.ValueOrThrow();
            }

            LOG_DEBUG("Creating snapshot");
            {
                TSnapshotMeta meta;
                YCHECK(DeserializeFromProto(&meta, params.Meta));

                TCreateNodeOptions options;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("replication_factor", Options_->SnapshotReplicationFactor);
                attributes->Set("prev_record_count", meta.prev_record_count());
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
        NFS::Remove(GetLocalPath(snapshotId));
    }

    TSnapshotParams DoGetSnapshotParams(int snapshotId)
    {
        LOG_DEBUG("Requesting parameters for snapshot %d from remote store",
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

        TSnapshotMeta meta;
        meta.set_prev_record_count(attributes.Get<i64>("prev_record_count"));

        TSnapshotParams params;
        params.Checksum = 0;
        params.CompressedLength = params.UncompressedLength = -1;
        YCHECK(SerializeToProto(meta, &params.Meta));
        return params;
    }


    Stroka GetLocalPath(int snapshotId)
    {
        Stroka mangledRemotePath;
        auto remotePath = GetRemotePath(snapshotId);
        for (int index = 0; index < remotePath.length(); ++index) {
            char ch = remotePath[index];
            mangledRemotePath.append(ch == '/' ? '-' : ch);
        }
        return CombinePaths(Config_->TempPath, mangledRemotePath);
    }

    TYPath GetRemotePath(int snapshotId)
    {
        return Format("%v/%09v", RemotePath_, snapshotId);
    }

};

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    TRemoteSnapshotStoreOptionsPtr options,
    const TYPath& remotePath,
    IClientPtr masterClient)
{
    return New<TRemoteSnapshotStore>(
        config,
        options,
        remotePath,
        masterClient);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
