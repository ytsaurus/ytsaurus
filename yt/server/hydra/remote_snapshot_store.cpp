#include "remote_snapshot_store.h"
#include "private.h"
#include "config.h"
#include "file_snapshot_store.h"
#include "snapshot.h"

#include <yt/client/api/client.h>
#include <yt/client/api/config.h>
#include <yt/client/api/connection.h>
#include <yt/client/api/file_reader.h>
#include <yt/client/api/file_writer.h>
#include <yt/client/api/transaction.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>
#include <yt/ytlib/hydra/config.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/scheduler.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/fs.h>

#include <yt/core/ypath/token.h>

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/ypath_proxy.h>

namespace NYT::NHydra {

using namespace NFS;
using namespace NConcurrency;
using namespace NYPath;
using namespace NElection;
using namespace NObjectClient;
using namespace NYTree;
using namespace NApi;
using namespace NHydra::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRemoteSnapshotStore)

class TRemoteSnapshotStore
    : public ISnapshotStore
{
public:
    TRemoteSnapshotStore(
        TRemoteSnapshotStoreConfigPtr config,
        TRemoteSnapshotStoreOptionsPtr options,
        const TYPath& path,
        IClientPtr client,
        TTransactionId prerequisiteTransactionId)
        : Config_(config)
        , Options_(options)
        , Path_(path)
        , Client_(client)
        , PrerequisiteTransactionId_(prerequisiteTransactionId)
    {
        Logger.AddTag("Path: %v", Path_);
    }

    virtual ISnapshotReaderPtr CreateReader(int snapshotId) override
    {
        return New<TReader>(this, snapshotId);
    }

    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const TSnapshotMeta& meta) override
    {
        if (!PrerequisiteTransactionId_) {
            THROW_ERROR_EXCEPTION("Snapshot store is read-only");
        }
        return New<TWriter>(this, snapshotId, meta);
    }

    virtual TFuture<int> GetLatestSnapshotId(int maxSnapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoGetLatestSnapshotId, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(maxSnapshotId);
    }

private:
    const TRemoteSnapshotStoreConfigPtr Config_;
    const TRemoteSnapshotStoreOptionsPtr Options_;
    const TYPath Path_;
    const IClientPtr Client_;
    const TTransactionId PrerequisiteTransactionId_;

    NLogging::TLogger Logger = HydraLogger;


    class TReader
        : public ISnapshotReader
    {
    public:
        TReader(TRemoteSnapshotStorePtr store, int snapshotId)
            : Store_(store)
            , SnapshotId_(snapshotId)
            , Path_(Store_->GetSnapshotPath(SnapshotId_))
        {
            Logger.AddTag("Path: %v", Path_);
        }

        virtual TFuture<void> Open() override
        {
            return BIND(&TReader::DoOpen, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker())
                .Run();
        }

        virtual TFuture<TSharedRef> Read() override
        {
            return BIND(&TReader::DoRead, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker())
                .Run();
        }

        virtual TSnapshotParams GetParams() const override
        {
            return Params_;
        }

    private:
        const TRemoteSnapshotStorePtr Store_;
        const int SnapshotId_;

        TYPath Path_;

        TSnapshotParams Params_;

        IAsyncZeroCopyInputStreamPtr UnderlyingReader_;

        NLogging::TLogger Logger = HydraLogger;


        void DoOpen()
        {
            try {
                YT_LOG_DEBUG("Requesting remote snapshot parameters");
                INodePtr node;
                {
                    TGetNodeOptions options;
                    options.Attributes = {"prev_record_count"};
                    auto asyncResult = Store_->Client_->GetNode(Path_, options);
                    auto result = WaitFor(asyncResult)
                        .ValueOrThrow();
                    node = ConvertToNode(result);
                }
                YT_LOG_DEBUG("Remote snapshot parameters received");

                {
                    const auto& attributes = node->Attributes();
                    Params_.Meta.set_prev_record_count(attributes.Get<i64>("prev_record_count"));
                    Params_.Checksum = 0;
                    Params_.CompressedLength = Params_.UncompressedLength = -1;
                }

                YT_LOG_DEBUG("Opening remote snapshot reader");
                {
                    TFileReaderOptions options;
                    options.Config = Store_->Config_->Reader;
                    UnderlyingReader_ = WaitFor(Store_->Client_->CreateFileReader(Store_->GetSnapshotPath(SnapshotId_), options))
                        .ValueOrThrow();
                }
                YT_LOG_DEBUG("Remote snapshot reader opened");
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error opening remote snapshot for reading")
                    << TErrorAttribute("snapshot_path", Path_)
                    << ex;
            }
        }

        TSharedRef DoRead()
        {
            try {
                return WaitFor(UnderlyingReader_->Read())
                    .ValueOrThrow();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error reading remote snapshot")
                    << TErrorAttribute("snapshot_path", Path_)
                    << ex;
            }
        }
    };

    class TWriter
        : public ISnapshotWriter
    {
    public:
        TWriter(TRemoteSnapshotStorePtr store, int snapshotId, const TSnapshotMeta& meta)
            : Store_(store)
            , SnapshotId_(snapshotId)
            , Meta_(meta)
            , Path_(Store_->GetSnapshotPath(SnapshotId_))
        {
            Logger.AddTag("Path: %v", Path_);
        }

        virtual TFuture<void> Open() override
        {
            return BIND(&TWriter::DoOpen, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker())
                .Run();
        }

        virtual TFuture<void> Write(const TSharedRef& buffer) override
        {
            YCHECK(Opened_ && !Closed_);
            Length_ += buffer.Size();
            return Writer_->Write(buffer);
        }

        virtual TFuture<void> Close() override
        {
            return BIND(&TWriter::DoClose, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker())
                .Run();
        }

        virtual TSnapshotParams GetParams() const override
        {
            YCHECK(Closed_);
            return Params_;
        }

    private:
        const TRemoteSnapshotStorePtr Store_;
        const int SnapshotId_;
        const TSnapshotMeta Meta_;

        TYPath Path_;

        ITransactionPtr Transaction_;

        IFileWriterPtr Writer_;
        i64 Length_ = 0;
        TSnapshotParams Params_;
        bool Opened_ = false;
        bool Closed_ = false;

        NLogging::TLogger Logger = HydraLogger;


        void DoOpen()
        {
            try {
                YCHECK(!Opened_);

                YT_LOG_DEBUG("Starting remote snapshot upload transaction");
                {
                    TTransactionStartOptions options;
                    auto attributes = CreateEphemeralAttributes();
                    attributes->Set("title", Format("Snapshot upload to %v",
                        Path_));
                    options.Attributes = std::move(attributes);

                    auto asyncResult = Store_->Client_->StartTransaction(
                        NTransactionClient::ETransactionType::Master,
                        options);
                    Transaction_ = WaitFor(asyncResult)
                        .ValueOrThrow();
                }
                YT_LOG_DEBUG("Remote snapshot upload transaction started (TransactionId: %v)",
                    Transaction_->GetId());

                YT_LOG_DEBUG("Creating remote snapshot");
                {
                    TCreateNodeOptions options;
                    auto attributes = CreateEphemeralAttributes();
                    attributes->Set("replication_factor", Store_->Options_->SnapshotReplicationFactor);
                    attributes->Set("compression_codec", Store_->Options_->SnapshotCompressionCodec);
                    attributes->Set("account", Store_->Options_->SnapshotAccount);
                    attributes->Set("primary_medium", Store_->Options_->SnapshotPrimaryMedium);
                    attributes->Set("prev_record_count", Meta_.prev_record_count());
                    options.Attributes = std::move(attributes);
                    if (Store_->PrerequisiteTransactionId_) {
                        options.PrerequisiteTransactionIds.push_back(Store_->PrerequisiteTransactionId_);
                    }

                    auto asyncResult = Transaction_->CreateNode(
                        Path_,
                        EObjectType::File,
                        options);
                    WaitFor(asyncResult)
                        .ThrowOnError();
                }
                YT_LOG_DEBUG("Remote snapshot created");

                YT_LOG_DEBUG("Opening remote snapshot writer");
                {
                    TFileWriterOptions options;
                    options.TransactionId = Transaction_->GetId();
                    if (Store_->PrerequisiteTransactionId_) {
                        options.PrerequisiteTransactionIds.push_back(Store_->PrerequisiteTransactionId_);
                    }

                    // Aim for safely: always upload snapshots with maximum RF.
                    options.Config = CloneYsonSerializable(Store_->Config_->Writer);
                    options.Config->UploadReplicationFactor = Store_->Options_->SnapshotReplicationFactor;
                    options.Config->MinUploadReplicationFactor = Store_->Options_->SnapshotReplicationFactor;

                    Writer_ = Store_->Client_->CreateFileWriter(Store_->GetSnapshotPath(SnapshotId_), options);

                    WaitFor(Writer_->Open())
                        .ThrowOnError();
                }
                YT_LOG_DEBUG("Remote snapshot writer opened");

                Opened_ = true;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error opening remote snapshot for writing")
                    << TErrorAttribute("snapshot_path", Path_)
                    << ex;
            }
        }

        void DoClose()
        {
            try {
                YCHECK(Opened_ && !Closed_);

                YT_LOG_DEBUG("Closing remote snapshot writer");
                WaitFor(Writer_->Close())
                    .ThrowOnError();
                YT_LOG_DEBUG("Remote snapshot writer closed");

                YT_LOG_DEBUG("Committing snapshot upload transaction");
                WaitFor(Transaction_->Commit())
                    .ThrowOnError();
                YT_LOG_DEBUG("Snapshot upload transaction committed");

                Params_.Meta = Meta_;
                Params_.CompressedLength = Length_;
                Params_.UncompressedLength = Length_;

                Closed_ = true;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error closing remote snapshot")
                    << TErrorAttribute("snapshot_path", Path_)
                    << ex;
            }
        }

    };


    int DoGetLatestSnapshotId(int maxSnapshotId)
    {
        try {
            YT_LOG_DEBUG("Requesting snapshot list from remote store");
            auto asyncResult = Client_->ListNode(Path_);
            auto result = WaitFor(asyncResult)
                .ValueOrThrow();
            YT_LOG_DEBUG("Snapshot list received");

            auto keys = ConvertTo<std::vector<TString>>(result);
            int lastestSnapshotId = InvalidSegmentId;
            for (const auto& key : keys) {
                int id;
                try {
                    id = FromString<int>(key);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING("Unrecognized item %Qv in remote store %v",
                        key,
                        Path_);
                    continue;
                }
                if (id <= maxSnapshotId && id > lastestSnapshotId) {
                    lastestSnapshotId = id;
                }
            }

            return lastestSnapshotId;
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error computing the latest snapshot id in remote store")
                << TErrorAttribute("snapshot_path", Path_)
                << ex;
        }
    }

    TYPath GetSnapshotPath(int snapshotId)
    {
        return Format("%v/%09v", Path_, snapshotId);
    }

};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStore)

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr config,
    TRemoteSnapshotStoreOptionsPtr options,
    const TYPath& path,
    IClientPtr client,
    TTransactionId prerequisiteTransactionId)
{
    return New<TRemoteSnapshotStore>(
        config,
        options,
        path,
        client,
        prerequisiteTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
