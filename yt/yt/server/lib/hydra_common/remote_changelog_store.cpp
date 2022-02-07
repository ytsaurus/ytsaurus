#include "private.h"
#include "remote_changelog_store.h"

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/lazy_changelog.h>
#include <yt/yt/server/lib/hydra_common/private.h>
#include <yt/yt/server/lib/hydra_common/serialize.h>

#include <yt/yt/server/lib/security_server/resource_limits_manager.h>

#include <yt/yt/ytlib/api/native/journal_reader.h>
#include <yt/yt/ytlib/api/native/journal_writer.h>

#include <yt/yt/ytlib/hydra/proto/hydra_manager.pb.h>
#include <yt/yt/ytlib/hydra/config.h>

#include <yt/yt/ytlib/tablet_client/config.h>

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
using namespace NLogging;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NObjectClient;
using namespace NHydra::NProto;
using namespace NTabletClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStore)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

namespace {

TYPath GetChangelogPath(const TYPath& storePath, int id)
{
    return Format("%v/%09d", storePath, id);
}

std::vector<TSharedRef> ReadRecordsOrThrow(
    const TYPath& path,
    const TJournalReaderConfigPtr& config,
    const IClientPtr& client,
    int firstRecordId,
    int maxRecords)
{
    TJournalReaderOptions options;
    options.FirstRowIndex = firstRecordId;
    options.RowCount = maxRecords;
    options.Config = config;
    auto reader = client->CreateJournalReader(path, options);

    WaitFor(reader->Open())
        .ThrowOnError();

    return WaitFor(reader->Read())
        .ValueOrThrow();
}

} // namespace

class TRemoteChangelogStore
    : public IChangelogStore
{
public:
    TRemoteChangelogStore(
        TRemoteChangelogStoreConfigPtr config,
        TTabletCellOptionsPtr options,
        TYPath remotePath,
        IClientPtr client,
        NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
        ITransactionPtr prerequisiteTransaction,
        std::optional<TVersion> reachableVersion,
        std::optional<TElectionPriority> electionPriority,
        std::optional<int> term,
        const TJournalWriterPerformanceCounters& counters)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Path_(std::move(remotePath))
        , Client_(std::move(client))
        , ResourceLimitsManager_(std::move(resourceLimitsManager))
        , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
        , ReachableVersion_(reachableVersion)
        , ElectionPriority_(electionPriority)
        , Counters_(counters)
        , Logger(HydraLogger.WithTag("Path: %v", Path_))
        , Term_(term)
    { }

    bool IsReadOnly() const override
    {
        return !PrerequisiteTransaction_;
    }

    std::optional<int> GetTerm() const override
    {
        return Term_.load();
    }

    TFuture<void> SetTerm(int term) override
    {
        return BIND(&TRemoteChangelogStore::DoSetTerm, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(term);
    }

    std::optional<TVersion> GetReachableVersion() const override
    {
        return ReachableVersion_;
    }

    std::optional<TElectionPriority> GetElectionPriority() const override
    {
        return ElectionPriority_;
    }

    TFuture<int> GetLatestChangelogId() const override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<IChangelogPtr> CreateChangelog(int id, const NProto::TChangelogMeta& meta) override
    {
        return BIND(&TRemoteChangelogStore::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id, meta);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id) override
    {
        return BIND(&TRemoteChangelogStore::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id);
    }

    TFuture<void> RemoveChangelog(int id) override
    {
        return BIND(&TRemoteChangelogStore::DoRemoveChangelog, MakeStrong(this))
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
    const TTabletCellOptionsPtr Options_;
    const TYPath Path_;
    const IClientPtr Client_;
    const NSecurityServer::IResourceLimitsManagerPtr ResourceLimitsManager_;
    const ITransactionPtr PrerequisiteTransaction_;
    const std::optional<TVersion> ReachableVersion_;
    const std::optional<TElectionPriority> ElectionPriority_;
    const TJournalWriterPerformanceCounters Counters_;

    const TLogger Logger;

    std::atomic<std::optional<int>> Term_;


    void DoSetTerm(int term)
    {
        try {
            ValidateWritable();

            YT_LOG_DEBUG("Setting remote changelog store term (Term: %v)",
                term);

            TSetNodeOptions options;
            options.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());
            WaitFor(Client_->SetNode(Path_ + "/@term", ConvertToYsonString(term), options))
                .ThrowOnError();

            Term_.store(term);

            YT_LOG_DEBUG("Remote changelog store term set (Term: %v)",
                term);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error set remote changelog store term")
                << TErrorAttribute("store_path", Path_)
                << ex;
        }
    }

    IChangelogPtr DoCreateChangelog(int id, const NProto::TChangelogMeta& meta)
    {
        auto path = GetChangelogPath(Path_, id);
        try {
            ValidateWritable();

            YT_LOG_DEBUG("Creating remote changelog (ChangelogId: %v)",
                id);

            ResourceLimitsManager_->ValidateResourceLimits(
                Options_->ChangelogAccount,
                Options_->ChangelogPrimaryMedium);
            // NB: Check snapshot account to prevent creating unlimited number of changelogs.
            ResourceLimitsManager_->ValidateResourceLimits(
                Options_->SnapshotAccount,
                Options_->SnapshotPrimaryMedium);

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

            auto future = Client_->CreateNode(
                path,
                EObjectType::Journal,
                options);
            WaitFor(future)
                .ThrowOnError();

            YT_LOG_DEBUG("Remote changelog created (ChangelogId: %v)",
                id);

            return MakeRemoteChangelog(
                id,
                path,
                meta,
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
            YT_LOG_DEBUG("Getting remote changelog attributes (ChangelogId: %v)",
                id);

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

            auto dataSize = attributes.Get<i64>("uncompressed_data_size");
            auto recordCount = attributes.Get<int>("quorum_row_count");

            YT_LOG_DEBUG("Remote changelog attributes received (ChangelogId: %v)",
                id);

            YT_LOG_DEBUG("Remote changelog opened (ChangelogId: %v)",
                id);

            return MakeRemoteChangelog(
                id,
                path,
                {},
                recordCount,
                dataSize,
                /*createWriter*/ false);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening remote changelog")
                << TErrorAttribute("changelog_path", path)
                << ex;
        }
    }

    void DoRemoveChangelog(int id)
    {
        auto path = GetChangelogPath(Path_, id);
        try {
            ValidateWritable();

            YT_LOG_DEBUG("Removing remote changelog (ChangelogId: %v)",
                id);

            TRemoveNodeOptions options;
            options.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());
            WaitFor(Client_->RemoveNode(path, options))
                .ThrowOnError();

            YT_LOG_DEBUG("Remote changelog removed (ChangelogId: %v)",
                id);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error removing remote changelog")
                << TErrorAttribute("changelog_path", path)
                << ex;
        }
    }


    IChangelogPtr MakeRemoteChangelog(
        int id,
        TYPath path,
        TChangelogMeta meta,
        int recordCount,
        i64 dataSize,
        bool createWriter)
    {
        return New<TRemoteChangelog>(
            id,
            std::move(path),
            std::move(meta),
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

    void ValidateWritable()
    {
        if (!PrerequisiteTransaction_) {
            THROW_ERROR_EXCEPTION("Changelog store is read-only");
        }
    }


    class TRemoteChangelog
        : public IChangelog
    {
    public:
        TRemoteChangelog(
            int id,
            TYPath path,
            TChangelogMeta meta,
            ITransactionPtr prerequisiteTransaction,
            int recordCount,
            i64 dataSize,
            bool createWriter,
            TRemoteChangelogStorePtr owner)
            : Id_(id)
            , Path_(std::move(path))
            , Meta_(std::move(meta))
            , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
            , Owner_(std::move(owner))
            , Logger(Owner_->Logger)
            , RecordCount_(recordCount)
            , DataSize_(dataSize)
        {
            if (createWriter) {
                auto guard = Guard(WriterLock_);
                CreateWriter();
            }
        }

        int GetId() const override
        {
            return Id_;
        }

        int GetRecordCount() const override
        {
            return RecordCount_;
        }

        const NProto::TChangelogMeta& GetMeta() const override
        {
            return Meta_;
        }

        i64 GetDataSize() const override
        {
            return DataSize_;
        }

        TFuture<void> Append(TRange<TSharedRef> records) override
        {
            auto guard = Guard(WriterLock_);

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

            if (WriterOpened_) {
                FlushResult_ = Writer_->Write(records);
            } else {
                PendingRecords_.insert(
                    PendingRecords_.end(),
                    records.begin(),
                    records.end());
                FlushResult_ = PendingRecordsFlushed_;
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
        const int Id_;
        const TYPath Path_;
        const TChangelogMeta Meta_;
        const ITransactionPtr PrerequisiteTransaction_;
        const TRemoteChangelogStorePtr Owner_;

        const TLogger Logger;

        IJournalWriterPtr Writer_;

        //! Protects #Writer_, #WriterOpened_ and #PendingRecords_.
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, WriterLock_);

        //! If #WriterOpened_ is true, records are sent directly to the writer.
        //! If #WriterOpened_ is false, records are being kept in #PendingRecords_
        //! until writer is opened and then flushed.
        bool WriterOpened_ = false;
        std::vector<TSharedRef> PendingRecords_;
        TFuture<void> PendingRecordsFlushed_;

        std::atomic<int> RecordCount_;
        std::atomic<i64> DataSize_;

        TFuture<void> FlushResult_ = VoidFuture;

        std::vector<TSharedRef> DoRead(int firstRecordId, int maxRecords) const
        {
            try {
                return ReadRecordsOrThrow(Path_, Owner_->Config_->Reader, Owner_->Client_, firstRecordId, maxRecords);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error reading remote changelog")
                    << TErrorAttribute("changelog_path", Path_)
                    << ex;
            }
        }

        void CreateWriter()
        {
            VERIFY_SPINLOCK_AFFINITY(WriterLock_);

            try {
                auto writerOptions = Owner_->GetJournalWriterOptions();
                Writer_ = Owner_->Client_->CreateJournalWriter(Path_, writerOptions);
                PendingRecordsFlushed_ = Writer_->Open()
                    .Apply(BIND(&TRemoteChangelog::FlushPendingRecords, MakeStrong(this))
                        .AsyncVia(GetHydraIOInvoker()));
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to open remote changelog writer")
                    << TErrorAttribute("changelog_path", Path_)
                    << ex;
            }
        }

        TFuture<void> FlushPendingRecords()
        {
            YT_LOG_DEBUG("Journal writer opened; flushing pending records (PendingRecordCount: %v)",
                PendingRecords_.size());

            auto guard = Guard(WriterLock_);

            auto rows = std::exchange(PendingRecords_, {});
            auto flushedFuture = rows.empty() ? VoidFuture : Writer_->Write(MakeRange(rows));

            YT_VERIFY(!WriterOpened_);
            WriterOpened_ = true;

            return flushedFuture;
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
        TTabletCellOptionsPtr options,
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
    const TTabletCellOptionsPtr Options_;
    const TYPath Path_;
    const IClientPtr MasterClient_;
    const NSecurityServer::IResourceLimitsManagerPtr ResourceLimitsManager_;
    const TTransactionId PrerequisiteTransactionId_;
    const TJournalWriterPerformanceCounters Counters_;

    const TLogger Logger;

    IChangelogStorePtr DoLock()
    {
        try {
            ITransactionPtr prerequisiteTransaction;
            std::optional<TVersion> reachableVersion;
            std::optional<TElectionPriority> electionPriority;
            std::optional<int> term;
            if (PrerequisiteTransactionId_) {
                prerequisiteTransaction = CreatePrerequisiteTransaction();
                TakeLock(prerequisiteTransaction);
                reachableVersion = ComputeReachableVersion();
                electionPriority = ComputeElectionPriority();
                term = GetTerm();
            }

            return New<TRemoteChangelogStore>(
                Config_,
                Options_,
                Path_,
                MasterClient_,
                ResourceLimitsManager_,
                prerequisiteTransaction,
                reachableVersion,
                electionPriority,
                term,
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
        options.Timeout = Config_->LockTransactionTimeout;
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

    void ListChangelogs(
        const std::vector<TString>& attributes,
        std::function<void(const INodePtr&, int id)> functor)
    {
        YT_LOG_DEBUG("Requesting changelog list from remote store");
        TListNodeOptions options{
            .Attributes = attributes
        };
        auto result = WaitFor(MasterClient_->ListNode(Path_, options))
            .ValueOrThrow();
        YT_LOG_DEBUG("Changelog list received");

        auto items = ConvertTo<IListNodePtr>(result);

        for (const auto& item : items->GetChildren()) {
            auto key = item->GetValue<TString>();
            int id;
            if (!TryFromString(key, id)) {
                THROW_ERROR_EXCEPTION("Unrecognized item %Qv in changelog store %v",
                    key,
                    Path_);
            }
            functor(item, id);
        }
    }

    void ValidateChangelogsSealed()
    {
        ListChangelogs({"sealed"}, [&] (const INodePtr& item, int id) {
            if (!item->Attributes().Get<bool>("sealed", false)) {
                THROW_ERROR_EXCEPTION("Changelog %v in changelog store %v is not sealed",
                    id,
                    Path_);
            }
        });

        YT_LOG_DEBUG("No unsealed changelogs in remote store");
    }

    TVersion ComputeReachableVersion()
    {
        ValidateChangelogsSealed();

        int latestId = -1;
        int latestRowCount = -1;
        ListChangelogs({"quorum_row_count"}, [&] (const INodePtr& item, int id) {
            if (id > latestId) {
                latestId = id;
                latestRowCount = item->Attributes().Get<i64>("quorum_row_count");
            }
        });

        if (latestId < 0) {
            return {};
        }

        return {latestId, latestRowCount};
    }

    TElectionPriority ComputeElectionPriority()
    {
        ValidateChangelogsSealed();

        int latestId = -1;
        int latestChangelogRecordCount = 0;
        ListChangelogs({"quorum_row_count"}, [&] (const INodePtr& item, int id) {
            if (id >= latestId) {
                auto recordCount = item->Attributes().Get<i64>("quorum_row_count");
                if (recordCount > 0) {
                    latestId = id;
                    latestChangelogRecordCount = recordCount;
                }
            }
        });

        if (latestId == -1) {
            return {};
        }

        auto path = GetChangelogPath(Path_, latestId);
        auto records = ReadRecordsOrThrow(path, Config_->Reader, MasterClient_, 0, 1);

        if (records.empty()) {
            THROW_ERROR_EXCEPTION("Read zero records from nonempty changelog %v",
                latestId);
        }

        TMutationHeader header;
        TSharedRef requestData;
        DeserializeMutationRecord(records[0], &header, &requestData);

        // All mutations have the same term in one changelog.
        // (Of course I am not actually sure in anything at this point, but this actually shoulbe true).
        return {header.term(), latestId, header.sequence_number() + latestChangelogRecordCount - 1};
    }

    int GetTerm()
    {
        YT_LOG_DEBUG("Requesting term from remote store");

        TGetNodeOptions options;
        options.Attributes = {"term"};
        auto yson = WaitFor(MasterClient_->GetNode(Path_ + "/@", options))
            .ValueOrThrow();
        auto attributes = ConvertToAttributes(yson);
        auto term = attributes->Get<int>("term", 0);

        YT_LOG_DEBUG("Term received (Term: %v)",
            term);

        return term;
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

IChangelogStoreFactoryPtr CreateRemoteChangelogStoreFactory(
    TRemoteChangelogStoreConfigPtr config,
    TTabletCellOptionsPtr options,
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
