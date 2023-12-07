#include "remote_changelog_store.h"

#include "private.h"
#include "changelog_store_helpers.h"

#include <yt/yt/server/lib/hydra_common/changelog.h>
#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/lazy_changelog.h>

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

#include <yt/yt/core/misc/atomic_object.h>
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

std::vector<TSharedRef> ReadRecords(
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

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStoreAccessor
{
protected:
    const TYPath Path_;
    const IClientPtr Client_;

    const TLogger Logger;


    TRemoteChangelogStoreAccessor(
        TYPath path,
        IClientPtr client)
        : Path_(std::move(path))
        , Client_(std::move(client))
        , Logger(HydraLogger.WithTag("Path: %v", Path_))
    { }

    void ListChangelogs(
        const std::vector<TString>& attributes,
        std::function<void(const INodePtr&, int id)> functor)
    {
        YT_LOG_DEBUG("Requesting changelog list from remote store");
        TListNodeOptions options{
            .Attributes = attributes
        };
        auto result = WaitFor(Client_->ListNode(Path_, options))
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

    IInvokerPtr GetInvoker()
    {
        return Client_->GetConnection()->GetInvoker();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStore
    : public TRemoteChangelogStoreAccessor
    , public IChangelogStore
{
public:
    TRemoteChangelogStore(
        TRemoteChangelogStoreConfigPtr config,
        TTabletCellOptionsPtr options,
        TYPath path,
        IClientPtr client,
        NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
        ITransactionPtr prerequisiteTransaction,
        std::optional<TVersion> reachableVersion,
        std::optional<TElectionPriority> electionPriority,
        std::optional<int> term,
        std::optional<int> latestChangelogId,
        const TJournalWriterPerformanceCounters& counters)
        : TRemoteChangelogStoreAccessor(
            std::move(path),
            std::move(client))
        , Config_(std::move(config))
        , Options_(std::move(options))
        , ResourceLimitsManager_(std::move(resourceLimitsManager))
        , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
        , ReachableVersion_(reachableVersion)
        , ElectionPriority_(electionPriority)
        , Counters_(counters)
        , Term_(term)
        , LatestChangelogId_(latestChangelogId)
    { }

    bool IsReadOnly() const override
    {
        return !PrerequisiteTransaction_;
    }

    std::optional<int> TryGetTerm() const override
    {
        return Term_.Load();
    }

    TFuture<void> SetTerm(int term) override
    {
        return BIND(&TRemoteChangelogStore::DoSetTerm, MakeStrong(this))
            .AsyncVia(GetInvoker())
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

    std::optional<int> TryGetLatestChangelogId() const override
    {
        return LatestChangelogId_.Load();
    }

    TFuture<IChangelogPtr> CreateChangelog(int id, const NProto::TChangelogMeta& meta, const TChangelogOptions& options) override
    {
        return BIND(&TRemoteChangelogStore::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(id, meta, options);
    }

    TFuture<IChangelogPtr> OpenChangelog(int id, const TChangelogOptions& options) override
    {
        return BIND(&TRemoteChangelogStore::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(id, options);
    }

    TFuture<void> RemoveChangelog(int id) override
    {
        return BIND(&TRemoteChangelogStore::DoRemoveChangelog, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run(id);
    }

    void Abort() override
    {
        if (PrerequisiteTransaction_) {
            YT_UNUSED_FUTURE(PrerequisiteTransaction_->Abort());
        }
    }

private:
    const TRemoteChangelogStoreConfigPtr Config_;
    const TTabletCellOptionsPtr Options_;
    const NSecurityServer::IResourceLimitsManagerPtr ResourceLimitsManager_;
    const ITransactionPtr PrerequisiteTransaction_;
    const std::optional<TVersion> ReachableVersion_;
    const std::optional<TElectionPriority> ElectionPriority_;
    const TJournalWriterPerformanceCounters Counters_;

    TAtomicObject<std::optional<int>> Term_;
    TAtomicObject<std::optional<int>> LatestChangelogId_;


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

            Term_.Store(term);

            YT_LOG_DEBUG("Remote changelog store term set (Term: %v)",
                term);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error set remote changelog store term")
                << TErrorAttribute("store_path", Path_)
                << ex;
        }
    }

    void UpdateLatestChangelogId(int id)
    {
        auto expected = LatestChangelogId_.Load();
        do {
            if (expected >= id) {
                return;
            }
        } while (!LatestChangelogId_.CompareExchange(expected, id));
        YT_LOG_DEBUG("Latest changelog id updated (NewChangelogId: %v)",
            id);
    }

    IChangelogPtr DoCreateChangelog(int id, const NProto::TChangelogMeta& meta, const TChangelogOptions& options)
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

            TCreateNodeOptions createNodeOptions;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("erasure_codec", Options_->ChangelogErasureCodec);
            attributes->Set("replication_factor", Options_->ChangelogReplicationFactor);
            attributes->Set("read_quorum", Options_->ChangelogReadQuorum);
            attributes->Set("write_quorum", Options_->ChangelogWriteQuorum);
            attributes->Set("account", Options_->ChangelogAccount);
            attributes->Set("primary_medium", Options_->ChangelogPrimaryMedium);
            if (Options_->ChangelogExternalCellTag) {
                attributes->Set("external", true);
                attributes->Set("external_cell_tag", *Options_->ChangelogExternalCellTag);
            }
            createNodeOptions.Attributes = std::move(attributes);
            createNodeOptions.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());

            auto future = Client_->CreateNode(
                path,
                EObjectType::Journal,
                createNodeOptions);
            WaitFor(future)
                .ThrowOnError();

            YT_LOG_DEBUG("Remote changelog created (ChangelogId: %v)",
                id);

            UpdateLatestChangelogId(id);

            return MakeRemoteChangelog(
                id,
                path,
                meta,
                /*recordCount*/ 0,
                /*dataSize*/ 0,
                options);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error creating remote changelog")
                << TErrorAttribute("changelog_path", path)
                << ex;
        }
    }

    IChangelogPtr DoOpenChangelog(int id, const TChangelogOptions& options)
    {
        auto path = GetChangelogPath(Path_, id);
        try {
            YT_LOG_DEBUG("Getting remote changelog attributes (ChangelogId: %v)",
                id);

            TGetNodeOptions getNodeOptions;
            getNodeOptions.Attributes = {"uncompressed_data_size", "quorum_row_count"};
            auto result = WaitFor(Client_->GetNode(path, getNodeOptions));
            if (result.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::NoSuchChangelog,
                    "Changelog does not exist in remote store")
                    << TErrorAttribute("changelog_path", Path_);
            }

            auto node = ConvertToNode(result.ValueOrThrow());
            const auto& attributes = node->Attributes();

            auto dataSize = attributes.Get<i64>("uncompressed_data_size");
            auto recordCount = attributes.Get<int>("quorum_row_count");

            YT_LOG_DEBUG("Remote changelog attributes received (ChangelogId: %v, DataSize: %v, RecordCount: %v)",
                id,
                dataSize,
                recordCount);

            YT_LOG_DEBUG("Remote changelog opened (ChangelogId: %v)",
                id);

            return MakeRemoteChangelog(
                id,
                path,
                /*meta*/ {},
                recordCount,
                dataSize,
                options);
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
        const TChangelogOptions& options)
    {
        return New<TRemoteChangelog>(
            id,
            std::move(path),
            std::move(meta),
            PrerequisiteTransaction_,
            recordCount,
            dataSize,
            options,
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
            const TChangelogOptions& options,
            TRemoteChangelogStorePtr owner)
            : Id_(id)
            , Path_(std::move(path))
            , Meta_(std::move(meta))
            , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
            , Owner_(std::move(owner))
            , Logger(Owner_->Logger.WithTag("ChangelogId: %v", id))
            , RecordCount_(recordCount)
            , DataSize_(dataSize)
        {
            if (options.CreateWriterEagerly) {
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

        i64 EstimateChangelogSize(i64 payloadSize) const override
        {
            return payloadSize;
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

            FlushResult_.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
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
                .AsyncVia(GetInvoker())
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
            TFuture<void> future;
            {
                auto guard = Guard(WriterLock_);
                if (!Writer_) {
                    YT_LOG_DEBUG("Remote changelog has no underlying writer and is now closed");
                    return VoidFuture;
                }
                YT_LOG_DEBUG("Closing remote changelog with its underlying writer");
                future = Writer_->Close();
            }

            future.Subscribe(BIND([Logger = Logger] (const TError& error) {
                if (error.IsOK()) {
                    YT_LOG_DEBUG("Remote changelog closed");
                } else {
                    YT_LOG_DEBUG(error, "Error closing remote changelog");
                }
            }));
            return future;
        }

    private:
        const int Id_;
        const TYPath Path_;
        const TChangelogMeta Meta_;
        const ITransactionPtr PrerequisiteTransaction_;
        const TRemoteChangelogStorePtr Owner_;
        const TLogger Logger;

        //! Protects #Writer_, #WriterOpened_ and #PendingRecords_.
        YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, WriterLock_);

        IJournalWriterPtr Writer_;

        //! If #WriterOpened_ is true, records are sent directly to #Writer_.
        //! If #WriterOpened_ is false, records are being kept in #PendingRecords_
        //! until writer is opened and then flushed.
        bool WriterOpened_ = false;
        std::vector<TSharedRef> PendingRecords_;
        TFuture<void> PendingRecordsFlushed_;

        std::atomic<int> RecordCount_;
        std::atomic<i64> DataSize_;

        TFuture<void> FlushResult_ = VoidFuture;


        IInvokerPtr GetInvoker() const
        {
            return Owner_->GetInvoker();
        }

        std::vector<TSharedRef> DoRead(int firstRecordId, int maxRecords) const
        {
            try {
                return ReadRecords(Path_, Owner_->Config_->Reader, Owner_->Client_, firstRecordId, maxRecords);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error reading remote changelog")
                    << TErrorAttribute("changelog_path", Path_)
                    << ex;
            }
        }

        void CreateWriter()
        {
            VERIFY_SPINLOCK_AFFINITY(WriterLock_);

            YT_LOG_DEBUG("Creating remote changelog writer");

            try {
                auto writerOptions = Owner_->GetJournalWriterOptions();
                Writer_ = Owner_->Client_->CreateJournalWriter(Path_, writerOptions);
                PendingRecordsFlushed_ = Writer_->Open()
                    .Apply(BIND(&TRemoteChangelog::FlushPendingRecords, MakeStrong(this))
                        .AsyncVia(GetInvoker()));
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
    : public TRemoteChangelogStoreAccessor
    , public IChangelogStoreFactory
{
public:
    TRemoteChangelogStoreFactory(
        TRemoteChangelogStoreConfigPtr config,
        TTabletCellOptionsPtr options,
        TYPath path,
        IClientPtr client,
        NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
        TTransactionId prerequisiteTransactionId,
        TJournalWriterPerformanceCounters counters)
        : TRemoteChangelogStoreAccessor(
            std::move(path),
            std::move(client))
        , Config_(std::move(config))
        , Options_(std::move(options))
        , ResourceLimitsManager_(std::move(resourceLimitsManager))
        , PrerequisiteTransactionId_(prerequisiteTransactionId)
        , Counters_(std::move(counters))
    { }

    TFuture<IChangelogStorePtr> Lock() override
    {
        return BIND(&TRemoteChangelogStoreFactory::DoLock, MakeStrong(this))
            .AsyncVia(GetInvoker())
            .Run();
    }

private:
    const TRemoteChangelogStoreConfigPtr Config_;
    const TTabletCellOptionsPtr Options_;
    const NSecurityServer::IResourceLimitsManagerPtr ResourceLimitsManager_;
    const TTransactionId PrerequisiteTransactionId_;
    const TJournalWriterPerformanceCounters Counters_;


    IChangelogStorePtr DoLock()
    {
        try {
            ITransactionPtr prerequisiteTransaction;
            std::optional<TVersion> reachableVersion;
            std::optional<TElectionPriority> electionPriority;
            std::optional<int> term;
            std::optional<int> latestChangelogId;
            if (PrerequisiteTransactionId_) {
                prerequisiteTransaction = CreatePrerequisiteTransaction();

                TakeLock(prerequisiteTransaction);

                auto scanResult = Scan();
                reachableVersion = TVersion(
                    scanResult.LatestChangelogId,
                    scanResult.LatestChangelogRecordCount);
                electionPriority = TElectionPriority(
                    scanResult.LastMutationTerm,
                    scanResult.LatestNonemptyChangelogId,
                    scanResult.LastMutationSequenceNumber);
                term = GetTerm();
                latestChangelogId = scanResult.LatestChangelogId;
            }

            return New<TRemoteChangelogStore>(
                Config_,
                Options_,
                Path_,
                Client_,
                ResourceLimitsManager_,
                prerequisiteTransaction,
                reachableVersion,
                electionPriority,
                term,
                latestChangelogId,
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
        return WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    }

    void TakeLock(ITransactionPtr prerequisiteTransaction)
    {
        TLockNodeOptions options;
        options.ChildKey = "lock";
        WaitFor(prerequisiteTransaction->LockNode(Path_, NCypressClient::ELockMode::Shared, options))
            .ThrowOnError();
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

    TChangelogStoreScanResult Scan()
    {
        ValidateChangelogsSealed();

        THashMap<int, i64> changelogIdToRecordCount;
        ListChangelogs({"quorum_row_count"}, [&] (const INodePtr& item, int id) {
            changelogIdToRecordCount.emplace(
                id,
                item->Attributes().Get<i64>("quorum_row_count"));
        });

        auto recordCountGetter = [&] (int changelogId) {
            return GetOrCrash(changelogIdToRecordCount, changelogId);
        };

        auto recordReader = [&] (int changelogId, i64 recordId) {
            auto path = GetChangelogPath(Path_, changelogId);
            auto recordsData = ReadRecords(path, Config_->Reader, Client_, recordId, 1);

            if (recordsData.empty()) {
                THROW_ERROR_EXCEPTION("Unable to read record %v in changelog %v",
                    recordId,
                    changelogId);
            }

            return recordsData[0];
        };

        return ScanChangelogStore(
            GetKeys(changelogIdToRecordCount),
            recordCountGetter,
            recordReader);
    }

    int GetTerm()
    {
        YT_LOG_DEBUG("Requesting term from remote store");

        TGetNodeOptions options;
        options.Attributes = {"term"};
        auto yson = WaitFor(Client_->GetNode(Path_ + "/@", options))
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
