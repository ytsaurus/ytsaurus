#include "private.h"
#include "remote_snapshot_store.h"
#include "config.h"
#include "snapshot.h"

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/file_writer.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

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

// COMPAT(danilalexeev) Purge `SecondaryPath_`.
class TRemoteSnapshotStore
    : public ISnapshotStore
{
public:
    TRemoteSnapshotStore(
        TRemoteSnapshotStoreConfigPtr config,
        TRemoteSnapshotStoreOptionsPtr options,
        TYPath primaryPath,
        TYPath secondaryPath,
        IClientPtr client,
        TTransactionId prerequisiteTransactionId)
        : Config_(config)
        , Options_(options)
        , PrimaryPath_(std::move(primaryPath))
        , SecondaryPath_(std::move(secondaryPath))
        , Client_(client)
        , PrerequisiteTransactionId_(prerequisiteTransactionId)
        , Logger(HydraLogger.WithTag(
            "PrimaryPath: %v, SecondaryPath: %v",
            PrimaryPath_,
            SecondaryPath_))
    {
    }

    ISnapshotReaderPtr CreateReader(int snapshotId) override
    {
        return New<TReader>(this, snapshotId);
    }

    ISnapshotWriterPtr CreateWriter(int snapshotId, const TSnapshotMeta& meta) override
    {
        if (!PrerequisiteTransactionId_) {
            THROW_ERROR_EXCEPTION("Snapshot store is read-only");
        }
        return New<TWriter>(this, snapshotId, meta);
    }

    TFuture<int> GetLatestSnapshotId(int maxSnapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoGetLatestSnapshotId, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(maxSnapshotId);
    }

private:
    const TRemoteSnapshotStoreConfigPtr Config_;
    const TRemoteSnapshotStoreOptionsPtr Options_;
    const TYPath PrimaryPath_;
    const TYPath SecondaryPath_;
    const IClientPtr Client_;
    const TTransactionId PrerequisiteTransactionId_;
    const NLogging::TLogger Logger;


    class TReader
        : public ISnapshotReader
    {
    public:
        TReader(TRemoteSnapshotStorePtr store, int snapshotId)
            : Store_(store)
            , SnapshotId_(snapshotId)
            , Logger(HydraLogger.WithTag("PrimaryPath: %v, SecondaryPath: %v",
                Store_->PrimaryPath_,
                Store_->SecondaryPath_))
        { }

        TFuture<void> Open() override
        {
            return BIND(&TReader::DoOpen, MakeStrong(this))
                .AsyncVia(GetInvoker())
                .Run();
        }

        TFuture<TSharedRef> Read() override
        {
            return BIND(&TReader::DoRead, MakeStrong(this))
                .AsyncVia(GetInvoker())
                .Run();
        }

        TSnapshotParams GetParams() const override
        {
            return Params_;
        }

    private:
        const TRemoteSnapshotStorePtr Store_;
        const int SnapshotId_;
        const NLogging::TLogger Logger;

        TYPath Path_;
        TSnapshotParams Params_;

        IAsyncZeroCopyInputStreamPtr UnderlyingReader_;


        IInvokerPtr GetInvoker()
        {
            return Store_->GetInvoker();
        }

        void DoOpen()
        {
            Path_ = Store_->GetSnapshotPath(Store_->PrimaryPath_, SnapshotId_);
            try {
                YT_LOG_DEBUG("Requesting remote snapshot parameters");
                INodePtr node;
                {
                    TGetNodeOptions options;
                    options.Attributes = {
                        "sequence_number",
                        "random_seed",
                        "state_hash",
                        "timestamp",
                        "last_segment_id",
                        "last_record_id",
                        "last_mutation_term"
                    };

                    auto requestNode = [&] (const TYPath& path) {
                        return WaitFor(Store_->Client_->GetNode(path, options));
                    };

                    auto rspOrError = requestNode(Path_);
                    if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                        Path_ = Store_->GetSnapshotPath(Store_->SecondaryPath_, SnapshotId_);
                        rspOrError = requestNode(Path_);
                    }
                    node = ConvertToNode(rspOrError.ValueOrThrow());
                }
                YT_LOG_DEBUG("Remote snapshot parameters received");

                {
                    const auto& attributes = node->Attributes();
                    Params_.Meta.set_random_seed(attributes.Get<ui64>("random_seed"));
                    Params_.Meta.set_sequence_number(attributes.Get<i64>("sequence_number"));
                    Params_.Meta.set_state_hash(attributes.Get<ui64>("state_hash"));
                    Params_.Meta.set_timestamp(ToProto<ui64>(attributes.Get<TInstant>("timestamp")));
                    Params_.Meta.set_last_segment_id(attributes.Get<i64>("last_segment_id"));
                    Params_.Meta.set_last_record_id(attributes.Get<i64>("last_record_id"));
                    Params_.Meta.set_last_mutation_term(attributes.Get<int>("last_mutation_term"));

                    Params_.Checksum = 0;
                    Params_.CompressedLength = Params_.UncompressedLength = -1;
                }

                YT_LOG_DEBUG("Opening remote snapshot reader");
                {
                    TFileReaderOptions options;
                    options.Config = Store_->Config_->Reader;
                    UnderlyingReader_ = WaitFor(Store_->Client_->CreateFileReader(Path_, options))
                        .ValueOrThrow();
                }
                YT_LOG_DEBUG("Remote snapshot reader opened");
            } catch (const TErrorException& ex) {
                if (ex.Error().FindMatching(NYTree::EErrorCode::ResolveError)) {
                    THROW_ERROR_EXCEPTION(EErrorCode::NoSuchSnapshot, "Error opening remote snapshot for reading")
                        << TErrorAttribute("snapshot_path", Path_)
                        << ex;
                } else {
                    THROW_ERROR_EXCEPTION("Error opening remote snapshot for reading")
                        << TErrorAttribute("snapshot_path", Path_)
                        << ex;
                }
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
            , Logger(HydraLogger.WithTag("PrimaryPath: %v, SecondaryPath: %v",
                Store_->PrimaryPath_,
                Store_->SecondaryPath_))
        { }

        TFuture<void> Open() override
        {
            return BIND(&TWriter::DoOpen, MakeStrong(this))
                .AsyncVia(GetInvoker())
                .Run();
        }

        TFuture<void> Write(const TSharedRef& buffer) override
        {
            YT_VERIFY(IsOpened_ && !IsClosed_);
            Length_ += buffer.Size();
            return Writer_->Write(buffer);
        }

        TFuture<void> Close() override
        {
            return BIND(&TWriter::DoClose, MakeStrong(this))
                .AsyncVia(GetInvoker())
                .Run();
        }

        TSnapshotParams GetParams() const override
        {
            YT_VERIFY(IsClosed_);
            return Params_;
        }

        DEFINE_SIGNAL_OVERRIDE(void(), Closed);

    private:
        const TRemoteSnapshotStorePtr Store_;
        const int SnapshotId_;
        const TSnapshotMeta Meta_;
        const NLogging::TLogger Logger;

        TYPath Path_;
        ITransactionPtr Transaction_;

        IFileWriterPtr Writer_;
        i64 Length_ = 0;
        TSnapshotParams Params_;
        bool IsOpened_ = false;
        bool IsClosed_ = false;


        IInvokerPtr GetInvoker()
        {
            return Store_->GetInvoker();
        }

        void DoOpen()
        {
            try {
                Path_ = Store_->GetSnapshotPath(Store_->GetCurrentStoragePath(), SnapshotId_);

                YT_VERIFY(!IsOpened_);

                YT_LOG_DEBUG("Starting remote snapshot upload transaction");
                {
                    TTransactionStartOptions options;
                    auto attributes = CreateEphemeralAttributes();
                    attributes->Set("title", Format("Snapshot upload to %v",
                        Path_));
                    options.Attributes = std::move(attributes);
                    if (Store_->PrerequisiteTransactionId_) {
                        options.PrerequisiteTransactionIds.push_back(Store_->PrerequisiteTransactionId_);
                    }

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
                    attributes->Set("erasure_codec", Store_->Options_->SnapshotErasureCodec);
                    attributes->Set("enable_striped_erasure", Store_->Options_->SnapshotEnableStripedErasure);
                    attributes->Set("sequence_number", Meta_.sequence_number());
                    attributes->Set("random_seed", Meta_.random_seed());
                    attributes->Set("state_hash", Meta_.state_hash());
                    attributes->Set("timestamp", Meta_.timestamp());
                    attributes->Set("last_segment_id", Meta_.last_segment_id());
                    attributes->Set("last_record_id", Meta_.last_record_id());
                    attributes->Set("last_mutation_term", Meta_.last_mutation_term());
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
                    options.Config = CloneYsonStruct(Store_->Config_->Writer);
                    options.Config->UploadReplicationFactor = Store_->Options_->SnapshotReplicationFactor;
                    options.Config->MinUploadReplicationFactor = Store_->Options_->SnapshotReplicationFactor;

                    Writer_ = Store_->Client_->CreateFileWriter(Path_, options);

                    WaitFor(Writer_->Open())
                        .ThrowOnError();
                }
                YT_LOG_DEBUG("Remote snapshot writer opened");

                IsOpened_ = true;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error opening remote snapshot for writing")
                    << TErrorAttribute("snapshot_path", Path_)
                    << ex;
            }
        }

        void DoClose()
        {
            try {
                YT_VERIFY(IsOpened_ && !IsClosed_);

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

                IsClosed_ = true;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error closing remote snapshot")
                    << TErrorAttribute("snapshot_path", Path_)
                    << ex;
            }
        }

    };


    IInvokerPtr GetInvoker()
    {
        return Client_->GetConnection()->GetInvoker();
    }

    int DoGetLatestSnapshotId(int maxSnapshotId)
    {
        int latestSnapshotId = InvalidSegmentId;

        auto processStore = [&] (const TYPath& path) {
            YT_LOG_DEBUG("Requesting snapshot list from remote store (Path: %v)", path);
            auto rspOrError = WaitFor(Client_->ListNode(path));
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_WARNING("Couldn`t resolve list request (Path: %v)", path);
                return;
            }
            YT_LOG_DEBUG("Snapshot list received");
            const auto& list = rspOrError.ValueOrThrow();
            auto keys = ConvertTo<std::vector<TString>>(list);
            for (const auto& key : keys) {
                int id;
                try {
                    id = FromString<int>(key);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING("Unrecognized item (Key: %v)%",
                        key);
                    continue;
                }
                if (id <= maxSnapshotId && id > latestSnapshotId) {
                    latestSnapshotId = id;
                }
            }
        };

        try {
            processStore(PrimaryPath_);
            processStore(SecondaryPath_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error computing the latest snapshot id in remote store")
                << TErrorAttribute("primary_path", PrimaryPath_)
                << TErrorAttribute("secondary_path", SecondaryPath_)
                << ex;
        }

        return latestSnapshotId;
    }

    TYPath GetSnapshotPath(const TYPath& storePath, int snapshotId)
    {
        return Format("%v/%09v", storePath, snapshotId);
    }

    const TYPath& GetCurrentStoragePath()
    {
        auto exists = WaitFor(Client_->NodeExists(PrimaryPath_))
            .ValueOrThrow();
        return exists ? PrimaryPath_ : SecondaryPath_;
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteSnapshotStore)

ISnapshotStorePtr CreateRemoteSnapshotStore(
    TRemoteSnapshotStoreConfigPtr storeConfig,
    TRemoteSnapshotStoreOptionsPtr storeOptions,
    TYPath primaryPath,
    TYPath secondaryPath,
    IClientPtr client,
    TTransactionId prerequisiteTransactionId)
{
    return New<TRemoteSnapshotStore>(
        storeConfig,
        storeOptions,
        std::move(primaryPath),
        std::move(secondaryPath),
        client,
        prerequisiteTransactionId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
