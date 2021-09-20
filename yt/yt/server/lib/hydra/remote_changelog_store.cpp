#include "remote_changelog_store.h"

#include "private.h"
#include "changelog.h"
#include "config.h"
#include "lazy_changelog.h"

#include <yt/yt/server/lib/security_server/resource_limits_manager.h>

#include <yt/yt/ytlib/api/native/journal_reader.h>
#include <yt/yt/ytlib/api/native/journal_writer.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>
#include <yt/yt/client/api/journal_reader.h>
#include <yt/yt/client/api/journal_writer.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/ytree/helpers.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NApi;
using namespace NYPath;
using namespace NYTree;
using namespace NObjectClient;
using namespace NHydra::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStore)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

namespace {

TYPath GetChangelogPath(const TYPath& path, int id)
{
    return Format("%v/%09d", path, id);
}

} // namespace

class TRemoteChangelogStore
    : public IChangelogStore
{
public:
    TRemoteChangelogStore(
        TRemoteChangelogStoreConfigPtr config,
        TRemoteChangelogStoreOptionsPtr options,
        TYPath remotePath,
        IClientPtr client,
        NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
        ITransactionPtr prerequisiteTransaction,
        std::optional<TVersion> reachableVersion,
        const TJournalWriterPerformanceCounters& counters)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Path_(std::move(remotePath))
        , Client_(std::move(client))
        , ResourceLimitsManager_(std::move(resourceLimitsManager))
        , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
        , ReachableVersion_(reachableVersion)
        , Counters_(counters)
        , Logger(HydraLogger.WithTag("Path: %v", Path_))
    { }

    bool IsReadOnly() const override
    {
        return !PrerequisiteTransaction_;
    }

    std::optional<TVersion> GetReachableVersion() const override
    {
        return ReachableVersion_;
    }

    TFuture<IChangelogPtr> CreateChangelog(int id) override
    {
        return BIND(&TRemoteChangelogStore::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id) override
    {
        return BIND(&TRemoteChangelogStore::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id);
    }

    void Abort() override
    {
        if (PrerequisiteTransaction_) {
            PrerequisiteTransaction_->Abort();
        }
    }

private:
    const TRemoteChangelogStoreConfigPtr Config_;
    const TRemoteChangelogStoreOptionsPtr Options_;
    const TYPath Path_;
    const IClientPtr Client_;
    const NSecurityServer::IResourceLimitsManagerPtr ResourceLimitsManager_;
    const ITransactionPtr PrerequisiteTransaction_;
    const std::optional<TVersion> ReachableVersion_;
    const TJournalWriterPerformanceCounters Counters_;

    const NLogging::TLogger Logger;

    IChangelogPtr DoCreateChangelog(int id)
    {
        auto path = GetChangelogPath(Path_, id);
        try {
            YT_LOG_DEBUG("Creating remote changelog (ChangelogId: %v)",
                id);

            if (!PrerequisiteTransaction_) {
                THROW_ERROR_EXCEPTION("Changelog store is read-only");
            }

            ResourceLimitsManager_->ValidateResourceLimits(
                Options_->ChangelogAccount,
                Options_->ChangelogPrimaryMedium);

            {
                TCreateNodeOptions options;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("erasure_codec", Options_->ChangelogErasureCodec);
                attributes->Set("replication_factor", Options_->ChangelogReplicationFactor);
                attributes->Set("read_quorum", Options_->ChangelogReadQuorum);
                attributes->Set("write_quorum", Options_->ChangelogWriteQuorum);
                attributes->Set("account", Options_->ChangelogAccount);
                attributes->Set("primary_medium", Options_->ChangelogPrimaryMedium);
                options.Attributes = std::move(attributes);
                options.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());

                auto asyncResult = Client_->CreateNode(
                    path,
                    EObjectType::Journal,
                    options);
                WaitFor(asyncResult)
                    .ThrowOnError();
            }

            YT_LOG_DEBUG("Remote changelog created (ChangelogId: %v)",
                id);

            return CreateRemoteChangelog(
                path,
                /*recordCount*/ 0,
                /*dataSize*/ 0,
                /*createWriter*/ true);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating remote changelog")
                << TErrorAttribute("changelog_path", path)
                << ex;
        }
    }

    IChangelogPtr DoOpenChangelog(int id)
    {
        auto path = GetChangelogPath(Path_, id);
        try {
            int recordCount;
            i64 dataSize;

            YT_LOG_DEBUG("Getting remote changelog attributes (ChangelogId: %v)",
                id);
            {
                TGetNodeOptions options;
                options.Attributes = {"uncompressed_data_size", "quorum_row_count"};
                auto result = WaitFor(Client_->GetNode(path, options));
                if (result.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::NoSuchChangelog,
                        "Changelog does not exist in remote store")
                        << TErrorAttribute("changelog_path", Path_)
                        << TErrorAttribute("store_id", id);
                }

                auto node = ConvertToNode(result.ValueOrThrow());
                const auto& attributes = node->Attributes();

                dataSize = attributes.Get<i64>("uncompressed_data_size");
                recordCount = attributes.Get<int>("quorum_row_count");
            }
            YT_LOG_DEBUG("Remote changelog attributes received (ChangelogId: %v)",
                id);

            return CreateRemoteChangelog(
                path,
                recordCount,
                dataSize,
                /*createWriter*/ false);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening remote changelog")
                << TErrorAttribute("changelog_path", path)
                << ex;
        }
    }

    IChangelogPtr CreateRemoteChangelog(
        TYPath path,
        int recordCount,
        i64 dataSize,
        bool createWriter)
    {
        return New<TRemoteChangelog>(
            std::move(path),
            PrerequisiteTransaction_,
            recordCount,
            dataSize,
            createWriter,
            this);
    }

    TJournalWriterOptions GetJournalWriterOptions() const
    {
        YT_VERIFY(!IsReadOnly());

        TJournalWriterOptions options;
        options.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());
        options.Config = Config_->Writer;
        options.EnableMultiplexing = Options_->EnableChangelogMultiplexing;
        options.EnableChunkPreallocation = Options_->EnableChangelogChunkPreallocation;
        options.ReplicaLagLimit = Options_->ChangelogReplicaLagLimit;
        options.Counters = Counters_;

        return options;
    }


    class TRemoteChangelog
        : public IChangelog
    {
    public:
        TRemoteChangelog(
            TYPath path,
            ITransactionPtr prerequisiteTransaction,
            int recordCount,
            i64 dataSize,
            bool createWriter,
            TRemoteChangelogStorePtr owner)
            : Path_(std::move(path))
            , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
            , Owner_(owner)
            , RecordCount_(recordCount)
            , DataSize_(dataSize)
        {
            if (createWriter) {
                CreateWriter();
            }
        }

        int GetRecordCount() const override
        {
            return RecordCount_;
        }

        i64 GetDataSize() const override
        {
            return DataSize_;
        }

        TFuture<void> Append(TRange<TSharedRef> records) override
        {
            if (!Writer_) {
                if (Owner_->IsReadOnly()) {
                    return MakeFuture(TError("Changelog is read-only"));
                }

                try {
                    CreateWriter();
                } catch (const std::exception& ex) {
                    return MakeFuture(TError(ex));
                }
            }

            auto recordsDataSize = GetByteSize(records);
            auto recordCount = records.size();

            // Fast path.
            if (WriterOpenedFuture_.IsSet() && WriterOpenedFuture_.Get().IsOK()) {
                FlushResult_ = Writer_->Write(records);
            } else {
                std::vector<TSharedRef> recordList(records.begin(), records.end());
                FlushResult_ = WriterOpenedFuture_.Apply(
                    BIND([=, this_ = MakeStrong(this)] {
                        return Writer_->Write(MakeRange(recordList));
                    }));
            }

            FlushResult_.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
                if (error.IsOK()) {
                    DataSize_ += recordsDataSize;
                    RecordCount_ += recordCount;
                }
            }));

            return FlushResult_;
        }

        TFuture<void> Flush() override
        {
            return FlushResult_;
        }

        TFuture<std::vector<TSharedRef>> Read(
            int firstRecordId,
            int maxRecords,
            i64 /*maxBytes*/) const override
        {
            return BIND(&TRemoteChangelog::DoRead, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker())
                .Run(firstRecordId, maxRecords);
        }

        TFuture<void> Truncate(int recordCount) override
        {
            if (Owner_->IsReadOnly()) {
                return MakeFuture(TError("Changelog is read-only"));
            }

            return Owner_->Client_->TruncateJournal(Path_, recordCount)
                .Apply(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                    if (error.IsOK()) {
                        RecordCount_ = std::min<int>(RecordCount_, recordCount);
                    }
                }));
        }

        TFuture<void> Close() override
        {
            return Writer_ ? Writer_->Close() : VoidFuture;
        }

    private:
        const TYPath Path_;
        const ITransactionPtr PrerequisiteTransaction_;
        const TRemoteChangelogStorePtr Owner_;

        IJournalWriterPtr Writer_;
        TFuture<void> WriterOpenedFuture_;

        std::atomic<int> RecordCount_;
        std::atomic<i64> DataSize_;

        TFuture<void> FlushResult_ = VoidFuture;

        std::vector<TSharedRef> DoRead(int firstRecordId, int maxRecords) const
        {
            try {
                TJournalReaderOptions options;
                options.FirstRowIndex = firstRecordId;
                options.RowCount = maxRecords;
                options.Config = Owner_->Config_->Reader;
                auto reader = Owner_->Client_->CreateJournalReader(Path_, options);

                WaitFor(reader->Open())
                    .ThrowOnError();

                return WaitFor(reader->Read())
                    .ValueOrThrow();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error reading remote changelog")
                    << TErrorAttribute("changelog_path", Path_)
                    << ex;
            }
        }

        void CreateWriter()
        {
            try {
                auto writerOptions = Owner_->GetJournalWriterOptions();
                Writer_ = Owner_->Client_->CreateJournalWriter(Path_, writerOptions);
                WriterOpenedFuture_ = Writer_->Open();
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to open remote changelog writer")
                    << TErrorAttribute("changelog_path", Path_)
                    << ex;
            }
        }
    };
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStore)

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStoreFactory
    : public IChangelogStoreFactory
{
public:
    TRemoteChangelogStoreFactory(
        TRemoteChangelogStoreConfigPtr config,
        TRemoteChangelogStoreOptionsPtr options,
        TYPath remotePath,
        IClientPtr client,
        NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
        TTransactionId prerequisiteTransactionId,
        TJournalWriterPerformanceCounters counters)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Path_(std::move(remotePath))
        , MasterClient_(std::move(client))
        , ResourceLimitsManager_(std::move(resourceLimitsManager))
        , PrerequisiteTransactionId_(prerequisiteTransactionId)
        , Counters_(std::move(counters))
        , Logger(HydraLogger.WithTag("Path: %v", Path_))
    { }

    TFuture<IChangelogStorePtr> Lock() override
    {
        return BIND(&TRemoteChangelogStoreFactory::DoLock, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

private:
    const TRemoteChangelogStoreConfigPtr Config_;
    const TRemoteChangelogStoreOptionsPtr Options_;
    const TYPath Path_;
    const IClientPtr MasterClient_;
    const NSecurityServer::IResourceLimitsManagerPtr ResourceLimitsManager_;
    const TTransactionId PrerequisiteTransactionId_;
    const TJournalWriterPerformanceCounters Counters_;

    const NLogging::TLogger Logger;

    IChangelogStorePtr DoLock()
    {
        try {
            ITransactionPtr prerequisiteTransaction;
            std::optional<TVersion> reachableVersion;
            if (PrerequisiteTransactionId_) {
                prerequisiteTransaction = CreatePrerequisiteTransaction();
                TakeLock(prerequisiteTransaction);
                reachableVersion = ComputeReachableVersion();
            }

            return New<TRemoteChangelogStore>(
                Config_,
                Options_,
                Path_,
                MasterClient_,
                ResourceLimitsManager_,
                prerequisiteTransaction,
                reachableVersion,
                Counters_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error locking remote changelog store %v",
                Path_)
                << ex;
        }
    }

    ITransactionPtr CreatePrerequisiteTransaction()
    {
        TTransactionStartOptions options;
        options.ParentId = PrerequisiteTransactionId_;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Lock for changelog store %v", Path_));
        options.Attributes = std::move(attributes);
        return WaitFor(MasterClient_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    }

    void TakeLock(ITransactionPtr prerequisiteTransaction)
    {
        TLockNodeOptions options;
        options.ChildKey = "lock";
        WaitFor(prerequisiteTransaction->LockNode(Path_, NCypressClient::ELockMode::Shared, options))
            .ThrowOnError();
    }

    TVersion ComputeReachableVersion()
    {
        YT_LOG_DEBUG("Requesting changelog list from remote store");
        TListNodeOptions options{
            .Attributes = std::vector<TString>{
                "sealed",
                "quorum_row_count"
            }
        };
        auto result = WaitFor(MasterClient_->ListNode(Path_, options))
            .ValueOrThrow();
        YT_LOG_DEBUG("Changelog list received");

        auto items = ConvertTo<IListNodePtr>(result);

        int latestId = -1;
        int latestRowCount = -1;
        for (const auto& item : items->GetChildren()) {
            auto key = item->GetValue<TString>();
            int id;
            if (!TryFromString(key, id)) {
                THROW_ERROR_EXCEPTION("Unrecognized item %Qv in changelog store %v",
                    key,
                    Path_);
            }
            if (!item->Attributes().Get<bool>("sealed", false)) {
                THROW_ERROR_EXCEPTION("Changelog %Qv in changelog store %v is not sealed",
                    key,
                    Path_);
            }
            if (id > latestId) {
                latestId = id;
                latestRowCount = item->Attributes().Get<i64>("quorum_row_count");
            }
        }

        if (latestId < 0) {
            return TVersion();
        }

        return TVersion(latestId, latestRowCount);
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

IChangelogStoreFactoryPtr CreateRemoteChangelogStoreFactory(
    TRemoteChangelogStoreConfigPtr config,
    TRemoteChangelogStoreOptionsPtr options,
    TYPath path,
    IClientPtr client,
    NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
    TTransactionId prerequisiteTransactionId,
    const TJournalWriterPerformanceCounters& counters)
{
    return New<TRemoteChangelogStoreFactory>(
        std::move(config),
        std::move(options),
        std::move(path),
        std::move(client),
        std::move(resourceLimitsManager),
        prerequisiteTransactionId,
        counters);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
