#include "remote_changelog_store.h"

#include "private.h"
#include "changelog_store_helpers.h"
#include "changelog.h"
#include "config.h"

#include <yt/yt/server/lib/cellar_agent/public.h>

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
using namespace NCellarAgent;
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

// COMPAT(danilalexeev)
DEFINE_BIT_ENUM(EStorageState,
    (None)
    (Primary)
    (Secondary)
    (Both)
);

// COMPAT(danilalexeev): Purge `SecondaryPath_`.
class TRemoteChangelogStoreAccessor
{
protected:
    const TYPath PrimaryPath_;
    const TYPath SecondaryPath_;
    const IClientPtr Client_;

    EStorageState StorageState_;

    const TLogger Logger;


    TRemoteChangelogStoreAccessor(
        TYPath primaryPath,
        TYPath secondaryPath,
        IClientPtr client,
        EStorageState storageState)
        : PrimaryPath_(std::move(primaryPath))
        , SecondaryPath_(std::move(secondaryPath))
        , Client_(std::move(client))
        , StorageState_(storageState)
        , Logger(HydraLogger.WithTag("PrimaryPath: %v, SecondaryPath: %v, StorageState: %v",
            PrimaryPath_,
            SecondaryPath_,
            StorageState_))
    { }

    void ListChangelogs(
        const std::vector<TString>& attributes,
        std::function<void(const INodePtr&, int id, bool atPrimaryPath)> functor)
    {
        YT_LOG_DEBUG("Requesting changelog list from remote store");
        TListNodeOptions options{
            .Attributes = attributes
        };

        auto processChangelogs = [&] (const TYPath& path, bool isPrimaryPath) {
            auto rspOrError = WaitFor(Client_->ListNode(path, options));
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_WARNING("Missing storage at path (IsPrimary: %v)", isPrimaryPath);
                return;
            }
            YT_LOG_DEBUG("Changelog list received");

            auto items = ConvertTo<IListNodePtr>(rspOrError.ValueOrThrow());
            for (const auto& item : items->GetChildren()) {
                auto key = item->GetValue<TString>();
                int id;
                if (!TryFromString(key, id)) {
                    THROW_ERROR_EXCEPTION("Unrecognized item %Qv in changelog store %v",
                        key,
                        path);
                }
                functor(item, id, isPrimaryPath);
            }
        };

        if (Any(StorageState_ & EStorageState::Primary)) {
            processChangelogs(PrimaryPath_, /*isPrimaryPath*/ true);
        }
        if (Any(StorageState_ & EStorageState::Secondary)) {
            processChangelogs(SecondaryPath_, /*isPrimaryPath*/ false);
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
        TYPath primaryPath,
        TYPath secondaryPath,
        EStorageState storageState,
        IClientPtr client,
        NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
        ITransactionPtr prerequisiteTransaction,
        std::optional<TVersion> reachableVersion,
        std::optional<TElectionPriority> electionPriority,
        std::optional<int> term,
        std::optional<int> latestChangelogId,
        const TJournalWriterPerformanceCounters& counters)
        : TRemoteChangelogStoreAccessor(
            std::move(primaryPath),
            std::move(secondaryPath),
            std::move(client),
            storageState)
        , Config_(std::move(config))
        , Options_(std::move(options))
        , ResourceLimitsManager_(std::move(resourceLimitsManager))
        , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
        , ReachableVersion_(reachableVersion)
        , ElectionPriority_(electionPriority)
        , Counters_(counters)
        , Term_(term)
        , LatestChangelogId_(latestChangelogId)
    {
        YT_VERIFY(storageState != EStorageState::None);
    }

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

            auto setTerm = [&] (const TYPath& path) {
                WaitFor(Client_->SetNode(path + "/@term", ConvertToYsonString(term), options))
                    .ThrowOnError();
            };

            if (Any(StorageState_ & EStorageState::Primary)) {
                setTerm(PrimaryPath_);
            }
            if (Any(StorageState_ & EStorageState::Secondary)) {
                setTerm(SecondaryPath_);
            }

            Term_.Store(term);

            YT_LOG_DEBUG("Remote changelog store term set (Term: %v)",
                term);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error setting remote changelog store term")
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

    TFuture<IChangelogPtr> DoCreateChangelog(int id, const NProto::TChangelogMeta& meta, const TChangelogOptions& options)
    {
        auto path = GetChangelogPath(
            StorageState_ == EStorageState::Secondary ? SecondaryPath_ : PrimaryPath_,
            id);
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

            return CreateRemoteChangelog(
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

    TFuture<IChangelogPtr> DoOpenChangelog(int id, const TChangelogOptions& options)
    {
        TYPath path;
        try {
            YT_LOG_DEBUG("Getting remote changelog attributes (ChangelogId: %v)",
                id);

            TGetNodeOptions getNodeOptions;
            getNodeOptions.Attributes = {"uncompressed_data_size", "quorum_row_count"};

            INodePtr node;
            auto getNode = [&] (const TYPath& pathPrefix) -> INodePtr {
                path = GetChangelogPath(pathPrefix, id);
                auto rspOrError = WaitFor(Client_->GetNode(path, getNodeOptions));
                if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return nullptr;
                }
                return ConvertToNode(rspOrError.ValueOrThrow());
            };
            if (Any(StorageState_ & EStorageState::Primary)) {
                node = getNode(PrimaryPath_);
            }
            if (!node && Any(StorageState_ & EStorageState::Secondary)) {
                node = getNode(SecondaryPath_);
            }
            if (!node) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::NoSuchChangelog,
                    "Error opening remote changelog");
            }
            const auto& attributes = node->Attributes();

            auto dataSize = attributes.Get<i64>("uncompressed_data_size");
            auto recordCount = attributes.Get<int>("quorum_row_count");

            YT_LOG_DEBUG("Remote changelog attributes received (ChangelogId: %v, DataSize: %v, RecordCount: %v)",
                id,
                dataSize,
                recordCount);

            YT_LOG_DEBUG("Remote changelog opened (ChangelogId: %v, Path: %v)",
                id,
                path);

            return CreateRemoteChangelog(
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
        TYPath path;
        try {
            ValidateWritable();

            YT_LOG_DEBUG("Removing remote changelog (ChangelogId: %v)",
                id);

            TRemoveNodeOptions options;
            options.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());

            auto tryRemove = [&] (const TYPath& path) {
                auto rspOrError = WaitFor(Client_->RemoveNode(path, options));
                if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    return false;
                }
                rspOrError.ThrowOnError();
                return true;
            };

            bool removed = false;
            if (Any(StorageState_ & EStorageState::Primary)) {
                path = GetChangelogPath(PrimaryPath_, id);
                removed = tryRemove(path);
            }
            if (!removed && Any(StorageState_ & EStorageState::Secondary)) {
                path = GetChangelogPath(SecondaryPath_, id);
                removed = tryRemove(path);
            }
            if (!removed) {
                THROW_ERROR_EXCEPTION(
                    EErrorCode::NoSuchChangelog,
                    "Error removing remote changelog");
            }

            YT_LOG_DEBUG("Remote changelog removed (ChangelogId: %v, Path: %v)",
                id,
                path);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error removing remote changelog")
                << TErrorAttribute("changelog_path", path)
                << ex;
        }
    }

    TFuture<IChangelogPtr> CreateRemoteChangelog(
        int id,
        TYPath path,
        TChangelogMeta meta,
        int recordCount,
        i64 dataSize,
        const TChangelogOptions& options)
    {
        auto changelog = New<TRemoteChangelog>(
            id,
            std::move(path),
            std::move(meta),
            PrerequisiteTransaction_,
            recordCount,
            dataSize,
            this);
        if (options.CreateWriterEagerly) {
            return changelog->CreateWriter().Apply(BIND([=] () -> IChangelogPtr { return changelog; }));
        } else {
            return MakeFuture<IChangelogPtr>(changelog);
        }
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
            TRemoteChangelogStorePtr owner)
            : Id_(id)
            , Path_(std::move(path))
            , Meta_(std::move(meta))
            , PrerequisiteTransaction_(std::move(prerequisiteTransaction))
            , Owner_(std::move(owner))
            , Logger(Owner_->Logger.WithTag("ChangelogId: %v", id))
            , RecordCount_(recordCount)
            , DataSize_(dataSize)
        { }

        TFuture<void> CreateWriter()
        {
            auto guard = Guard(WriterLock_);

            try {
                DoCreateWriter();
            } catch (const std::exception& ex) {
                return MakeFuture(TError(ex));
            }

            return WriterOpenFuture_;
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
                try {
                    DoCreateWriter();
                } catch (const std::exception& ex) {
                    return MakeFuture(TError(ex));
                }
            }

            auto recordsDataSize = GetByteSize(records);
            auto recordCount = records.size();

            if (WriterOpened_) {
                FlushFuture_ = Writer_->Write(records);
            } else {
                PendingRecords_.insert(
                    PendingRecords_.end(),
                    records.begin(),
                    records.end());
                FlushFuture_ = PendingRecordsFlushFuture_;
            }

            FlushFuture_.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
                if (error.IsOK()) {
                    DataSize_ += recordsDataSize;
                    RecordCount_ += recordCount;
                }
            }));

            return FlushFuture_;
        }

        TFuture<void> Flush() override
        {
            return FlushFuture_;
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
            auto guard = Guard(WriterLock_);

            if (!Writer_) {
                YT_LOG_DEBUG("Remote changelog has no underlying writer and is now closed");
                return VoidFuture;
            }

            YT_LOG_DEBUG("Closing remote changelog with its underlying writer");

            auto future = PendingRecordsFlushFuture_.Apply(
                BIND(&IJournalWriter::Close, Writer_));
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
        TFuture<void> WriterOpenFuture_;

        //! If #WriterOpened_ is true, records are sent directly to #Writer_.
        //! If #WriterOpened_ is false, records are being kept in #PendingRecords_
        //! until writer is opened and then flushed.
        bool WriterOpened_ = false;
        std::vector<TSharedRef> PendingRecords_;
        TFuture<void> PendingRecordsFlushFuture_;

        std::atomic<int> RecordCount_;
        std::atomic<i64> DataSize_;

        TFuture<void> FlushFuture_ = VoidFuture;


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

        void DoCreateWriter()
        {
            VERIFY_SPINLOCK_AFFINITY(WriterLock_);

            if (Owner_->IsReadOnly()) {
                THROW_ERROR_EXCEPTION("Changelog is read-only");
            }

            YT_LOG_DEBUG("Creating remote changelog writer");

            try {
                auto writerOptions = Owner_->GetJournalWriterOptions();
                Writer_ = Owner_->Client_->CreateJournalWriter(Path_, writerOptions);
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to open remote changelog writer")
                    << TErrorAttribute("changelog_path", Path_)
                    << ex;
            }

            WriterOpenFuture_ = Writer_->Open();
            PendingRecordsFlushFuture_ = WriterOpenFuture_
                .Apply(BIND(&TRemoteChangelog::FlushPendingRecords, MakeStrong(this))
                    .AsyncVia(GetInvoker()));
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
        TYPath primaryPath,
        TYPath secondaryPath,
        IClientPtr client,
        NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
        TTransactionId prerequisiteTransactionId,
        TJournalWriterPerformanceCounters counters)
        : TRemoteChangelogStoreAccessor(
            std::move(primaryPath),
            std::move(secondaryPath),
            std::move(client),
            EStorageState::None)
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
            UpdateStorageState();
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
                PrimaryPath_,
                SecondaryPath_,
                StorageState_,
                Client_,
                ResourceLimitsManager_,
                prerequisiteTransaction,
                reachableVersion,
                electionPriority,
                term,
                latestChangelogId,
                Counters_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error locking remote changelog store")
                << TErrorAttribute("primary_path", PrimaryPath_)
                << TErrorAttribute("secondary_path", SecondaryPath_)
                << ex;
        }
    }

    void UpdateStorageState()
    {
        StorageState_ = EStorageState::None;
        auto updateStateIfExists = [&] (const TYPath& path, EStorageState stateUpdate) {
            auto exists = WaitFor(Client_->NodeExists(path))
                .ValueOrThrow();
            if (exists) {
                StorageState_ |= stateUpdate;
            }
        };

        updateStateIfExists(PrimaryPath_, EStorageState::Primary);

        bool ignoreSecondaryStorage = true;
        auto checkVirtualCellMap = [&] (const TYPath& path, EObjectType expectedType) {
            auto rspOrError = WaitFor(Client_->GetNode(path + "/@type"));
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                return;
            }
            ignoreSecondaryStorage &= expectedType == ConvertTo<EObjectType>(rspOrError.ValueOrThrow());
        };

        checkVirtualCellMap(NCellarAgent::TabletCellCypressPrefix, EObjectType::VirtualTabletCellMap);
        checkVirtualCellMap(NCellarAgent::ChaosCellCypressPrefix, EObjectType::VirtualChaosCellMap);

        if (!ignoreSecondaryStorage) {
            updateStateIfExists(SecondaryPath_, EStorageState::Secondary);
        }

        if (StorageState_ == EStorageState::None) {
            THROW_ERROR_EXCEPTION("Neither remote changelog storage exists");
        }
    }

    ITransactionPtr CreatePrerequisiteTransaction()
    {
        TTransactionStartOptions options;
        options.ParentId = PrerequisiteTransactionId_;
        options.Timeout = Config_->LockTransactionTimeout;
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title",
            Format("Lock for changelog store (PrimaryPath: %v, SecondaryPath: %v)",
                PrimaryPath_,
                SecondaryPath_));
        options.Attributes = std::move(attributes);
        return WaitFor(Client_->StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    }

    void TakeLock(ITransactionPtr prerequisiteTransaction)
    {
        TLockNodeOptions options;
        options.ChildKey = "lock";

        auto lockNode = [&] (const TYPath& path) {
            Y_UNUSED(WaitFor(prerequisiteTransaction->LockNode(path, NCypressClient::ELockMode::Shared, options))
                .ValueOrThrow());
        };
        if (Any(StorageState_ & EStorageState::Primary)) {
            lockNode(PrimaryPath_);
        }
        if (Any(StorageState_ & EStorageState::Secondary)) {
            lockNode(SecondaryPath_);
        }
    }

    void ValidateChangelogsSealed()
    {
        ListChangelogs({"sealed"}, [&] (const INodePtr& item, int id, bool atPrimaryPath) {

            if (!item->Attributes().Get<bool>("sealed", false)) {
                THROW_ERROR_EXCEPTION("Changelog %v in changelog store %v is not sealed",
                    id,
                    atPrimaryPath ? PrimaryPath_ : SecondaryPath_);
            }
        });

        YT_LOG_DEBUG("No unsealed changelogs in remote store");
    }

    TChangelogStoreScanResult Scan()
    {
        ValidateChangelogsSealed();

        THashMap<int, TChangelogScanInfo> changelogIdToScanInfo;
        ListChangelogs({"quorum_row_count"}, [&] (const INodePtr& item, int id, bool atPrimaryPath) {
            changelogIdToScanInfo.emplace(
                id,
                TChangelogScanInfo{
                    .RecordCount = item->Attributes().Get<i64>("quorum_row_count"),
                    .AtPrimaryPath = atPrimaryPath,
                });
        });

        auto scanInfoGetter = [&] (int changelogId) {
            return GetOrCrash(changelogIdToScanInfo, changelogId);
        };

        auto recordReader = [&] (int changelogId, i64 recordId, bool atPrimaryPath) {
            auto path = GetChangelogPath(atPrimaryPath ? PrimaryPath_ : SecondaryPath_, changelogId);
            auto recordsData = ReadRecords(path, Config_->Reader, Client_, recordId, 1);

            if (recordsData.empty()) {
                THROW_ERROR_EXCEPTION("Unable to read record %v in changelog %v",
                    recordId,
                    changelogId);
            }

            return recordsData[0];
        };

        return ScanChangelogStore(
            GetKeys(changelogIdToScanInfo),
            scanInfoGetter,
            recordReader);
    }

    int GetTerm()
    {
        YT_LOG_DEBUG("Requesting term from remote store");

        TGetNodeOptions options;
        options.Attributes = {"term"};

        std::optional<int> term;
        auto processPath = [&] (const TYPath& path) {
            auto rspOrError = WaitFor(Client_->GetNode(path + "/@", options));
            if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                return;
            }

            auto attributes = ConvertToAttributes(rspOrError.ValueOrThrow());
            term = std::max(term.value_or(0), attributes->Get<int>("term", 0));
        };

        processPath(PrimaryPath_);
        processPath(SecondaryPath_);

        if (!term) {
            THROW_ERROR_EXCEPTION(
                NRpc::EErrorCode::Unavailable,
                "Error requesting term from remote store");
        }

        YT_LOG_DEBUG("Term received (Term: %v)",
            term);

        return *term;
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreFactory)

////////////////////////////////////////////////////////////////////////////////

IChangelogStoreFactoryPtr CreateRemoteChangelogStoreFactory(
    TRemoteChangelogStoreConfigPtr config,
    TTabletCellOptionsPtr options,
    TYPath primaryPath,
    TYPath secondaryPath,
    IClientPtr client,
    NSecurityServer::IResourceLimitsManagerPtr resourceLimitsManager,
    TTransactionId prerequisiteTransactionId,
    const TJournalWriterPerformanceCounters& counters)
{
    return New<TRemoteChangelogStoreFactory>(
        std::move(config),
        std::move(options),
        std::move(primaryPath),
        std::move(secondaryPath),
        std::move(client),
        std::move(resourceLimitsManager),
        prerequisiteTransactionId,
        counters);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
