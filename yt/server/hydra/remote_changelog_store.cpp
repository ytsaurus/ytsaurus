#include "remote_changelog_store.h"
#include "private.h"
#include "changelog.h"
#include "config.h"
#include "lazy_changelog.h"

#include <yt/ytlib/api/native/journal_reader.h>
#include <yt/ytlib/api/native/journal_writer.h>

#include <yt/client/api/client.h>
#include <yt/client/api/transaction.h>
#include <yt/client/api/journal_reader.h>
#include <yt/client/api/journal_writer.h>

#include <yt/ytlib/hydra/hydra_manager.pb.h>
#include <yt/ytlib/hydra/config.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/ytree/helpers.h>

namespace NYT {
namespace NHydra {

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
        const TYPath& remotePath,
        IClientPtr client,
        ITransactionPtr prerequisiteTransaction,
        TVersion reachableVersion,
        const NProfiling::TTagIdList& profilerTags)
        : Config_(config)
        , Options_(options)
        , Path_(remotePath)
        , Client_(client)
        , PrerequisiteTransaction_(prerequisiteTransaction)
        , ReachableVersion_(reachableVersion)
        , ProfilerTags_(profilerTags)
    {
        Logger.AddTag("Path: %v", Path_);
    }

    virtual TVersion GetReachableVersion() const override
    {
        return ReachableVersion_;
    }

    virtual TFuture<IChangelogPtr> CreateChangelog(int id, const TChangelogMeta& meta) override
    {
        return BIND(&TRemoteChangelogStore::DoCreateChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id, meta);
    }

    virtual TFuture<IChangelogPtr> OpenChangelog(int id) override
    {
        return BIND(&TRemoteChangelogStore::DoOpenChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(id);
    }

private:
    const TRemoteChangelogStoreConfigPtr Config_;
    const TRemoteChangelogStoreOptionsPtr Options_;
    const TYPath Path_;
    const IClientPtr Client_;
    const ITransactionPtr PrerequisiteTransaction_;
    const TVersion ReachableVersion_;
    const NProfiling::TTagIdList ProfilerTags_;

    NLogging::TLogger Logger = HydraLogger;


    IChangelogPtr DoCreateChangelog(int id, const TChangelogMeta& meta)
    {
        auto path = GetChangelogPath(Path_, id);
        try {
            LOG_DEBUG("Creating remote changelog (ChangelogId: %v)",
                id);

            if (!PrerequisiteTransaction_) {
                THROW_ERROR_EXCEPTION("Changelog store is read-only");
            }

            {
                TCreateNodeOptions options;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("replication_factor", Options_->ChangelogReplicationFactor);
                attributes->Set("read_quorum", Options_->ChangelogReadQuorum);
                attributes->Set("write_quorum", Options_->ChangelogWriteQuorum);
                attributes->Set("account", Options_->ChangelogAccount);
                attributes->Set("primary_medium", Options_->ChangelogPrimaryMedium);
                attributes->Set("prev_record_count", meta.prev_record_count());
                options.Attributes = std::move(attributes);
                options.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());

                auto asyncResult = Client_->CreateNode(
                    path,
                    EObjectType::Journal,
                    options);
                WaitFor(asyncResult)
                    .ThrowOnError();
            }

            IJournalWriterPtr writer;
            {
                TJournalWriterOptions options;
                options.PrerequisiteTransactionIds.push_back(PrerequisiteTransaction_->GetId());
                options.Config = Config_->Writer;
                options.EnableMultiplexing = Options_->EnableChangelogMultiplexing;
                options.Profiler = HydraProfiler.AppendPath("/remote_changelog").AddTags(ProfilerTags_);
                writer = Client_->CreateJournalWriter(path, options);
                WaitFor(writer->Open())
                    .ThrowOnError();
            }

            LOG_DEBUG("Remote changelog created (ChangelogId: %v)",
                id);

            return CreateRemoteChangelog(
                id,
                path,
                meta,
                writer,
                0,
                0);
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
            TChangelogMeta meta;
            int recordCount;
            i64 dataSize;

            LOG_DEBUG("Getting remote changelog attributes (ChangelogId: %v)",
                id);
            {
                TGetNodeOptions options;
                options.Attributes = {"prev_record_count", "uncompressed_data_size", "quorum_row_count"};
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

                meta.set_prev_record_count(attributes.Get<int>("prev_record_count"));
                dataSize = attributes.Get<i64>("uncompressed_data_size");
                recordCount = attributes.Get<int>("quorum_row_count");
            }
            LOG_DEBUG("Remote changelog attributes received (ChangelogId: %v)",
                id);

            return CreateRemoteChangelog(
                id,
                path,
                meta,
                nullptr,
                recordCount,
                dataSize);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening remote changelog")
                << TErrorAttribute("changelog_path", path)
                << ex;
        }
    }

    IChangelogPtr CreateRemoteChangelog(
        int id,
        const TYPath& path,
        const TChangelogMeta& meta,
        IJournalWriterPtr writer,
        int recordCount,
        i64 dataSize)
    {
        return New<TRemoteChangelog>(
            path,
            meta,
            recordCount,
            dataSize,
            writer,
            this);
    }


    class TRemoteChangelog
        : public IChangelog
    {
    public:
        TRemoteChangelog(
            const TYPath& path,
            const TChangelogMeta& meta,
            int recordCount,
            i64 dataSize,
            IJournalWriterPtr writer,
            TRemoteChangelogStorePtr owner)
            : Path_(path)
            , Meta_(meta)
            , Writer_(writer)
            , Owner_(owner)
            , RecordCount_(recordCount)
            , DataSize_(dataSize)
        { }

        virtual const TChangelogMeta& GetMeta() const override
        {
            return Meta_;
        }

        virtual int GetRecordCount() const override
        {
            return RecordCount_;
        }

        virtual i64 GetDataSize() const override
        {
            return DataSize_;
        }

        virtual TFuture<void> Append(const TSharedRef& data) override
        {
            if (!Writer_) {
                return MakeFuture<void>(TError("Changelog is read-only"));
            }

            DataSize_ += data.Size();
            RecordCount_ += 1;
            FlushResult_ = Writer_->Write(std::vector<TSharedRef>(1, data));
            return FlushResult_;
        }

        virtual TFuture<void> Flush() override
        {
            return FlushResult_;
        }

        virtual TFuture<std::vector<TSharedRef>> Read(
            int firstRecordId,
            int maxRecords,
            i64 /*maxBytes*/) const override
        {
            return BIND(&TRemoteChangelog::DoRead, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker())
                .Run(firstRecordId, maxRecords);
        }

        virtual TFuture<void> Truncate(int /*recordCount*/) override
        {
            Y_UNREACHABLE();
        }

        virtual TFuture<void> Close() override
        {
            return Writer_ ? Writer_->Close() : VoidFuture;
        }

        virtual TFuture<void> Preallocate(size_t size) override
        {
            Y_UNREACHABLE();
        }

    private:
        const TYPath Path_;
        const TChangelogMeta Meta_;
        const IJournalWriterPtr Writer_;
        const TRemoteChangelogStorePtr Owner_;

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

    };

};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStore)

class TRemoteChangelogStoreFactory
    : public IChangelogStoreFactory
{
public:
    TRemoteChangelogStoreFactory(
        TRemoteChangelogStoreConfigPtr config,
        TRemoteChangelogStoreOptionsPtr options,
        const TYPath& remotePath,
        IClientPtr client,
        const TTransactionId& prerequisiteTransactionId,
        const NProfiling::TTagIdList& profilerTags)
        : Config_(config)
        , Options_(options)
        , Path_(remotePath)
        , MasterClient_(client)
        , PrerequisiteTransactionId_(prerequisiteTransactionId)
        , ProfilerTags_(profilerTags)
        , Logger(NLogging::TLogger(HydraLogger)
            .AddTag("Path: %v", Path_))
    { }

    virtual TFuture<IChangelogStorePtr> Lock() override
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
    const TTransactionId PrerequisiteTransactionId_;
    const NProfiling::TTagIdList ProfilerTags_;

    const NLogging::TLogger Logger;


    IChangelogStorePtr DoLock()
    {
        try {
            ITransactionPtr prerequisiteTransaction;
            TVersion reachableVersion;
            if (PrerequisiteTransactionId_) {
                prerequisiteTransaction = CreatePrerequisiteTransaction();
                TakeLock(prerequisiteTransaction);
                reachableVersion = ComputeReachableVersion();
            }

            {
                auto asyncRsp = MasterClient_->GetNode(Format(
                    "//sys/accounts/%v/@violated_resource_limits",
                    ToYPathLiteral(Options_->ChangelogAccount)));

                auto rspOrError = WaitFor(asyncRsp);
                THROW_ERROR_EXCEPTION_IF_FAILED(
                    rspOrError,
                    "Error requesting resource limits for account %Qv",
                    Options_->ChangelogAccount);

                auto rsp = rspOrError.Value();

                auto attributes = ConvertToAttributes(rsp);
                auto chunkCount = attributes->Get<bool>("chunk_count");
                auto diskSpace = attributes->Get<bool>("disk_space");

                if (chunkCount || diskSpace) {
                    THROW_ERROR_EXCEPTION(
                        "Resource limits for changelog account %Qv are violated",
                        Options_->ChangelogAccount);
                }
            }

            return New<TRemoteChangelogStore>(
                Config_,
                Options_,
                Path_,
                MasterClient_,
                prerequisiteTransaction,
                reachableVersion,
                ProfilerTags_);
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

    int GetLatestChangelogId()
    {
        LOG_DEBUG("Requesting changelog list from remote store");
        auto result = WaitFor(MasterClient_->ListNode(Path_))
            .ValueOrThrow();
        LOG_DEBUG("Changelog list received");

        auto keys = ConvertTo<std::vector<TString>>(result);
        int latestId = InvalidSegmentId;
        for (const auto& key : keys) {
            int id;
            try {
                id = FromString<int>(key);
            } catch (const std::exception&) {
                LOG_WARNING("Unrecognized item %Qv in remote changelog store",
                    key);
                continue;
            }
            if (id > latestId || latestId == InvalidSegmentId) {
                latestId = id;
            }
        }

        return latestId;
    }

    TVersion ComputeReachableVersion()
    {
        int latestId = GetLatestChangelogId();

        if (latestId == InvalidSegmentId) {
            return TVersion();
        }

        auto path = GetChangelogPath(Path_, latestId);

        int recordCount;
        LOG_DEBUG("Getting remote changelog attributes (ChangelogId: %v)",
            latestId);
        {
            TGetNodeOptions options;
            options.Attributes = {"sealed", "quorum_row_count"};
            auto result = WaitFor(MasterClient_->GetNode(path, options));
            auto node = ConvertToNode(result.ValueOrThrow());

            const auto& attributes = node->Attributes();
            if (!attributes.Get<bool>("sealed")) {
                THROW_ERROR_EXCEPTION("Changelog %v is not sealed",
                    path);
            }
            recordCount = attributes.Get<int>("quorum_row_count");
        }
        LOG_DEBUG("Remote changelog attributes received (ChangelogId: %v)",
            latestId);

        return TVersion(latestId, recordCount);
    }

};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStoreFactory)

IChangelogStoreFactoryPtr CreateRemoteChangelogStoreFactory(
    TRemoteChangelogStoreConfigPtr config,
    TRemoteChangelogStoreOptionsPtr options,
    const TYPath& path,
    IClientPtr client,
    const TTransactionId& prerequisiteTransactionId,
    const NProfiling::TTagIdList& profilerTags)
{
    return New<TRemoteChangelogStoreFactory>(
        config,
        options,
        path,
        client,
        prerequisiteTransactionId,
        profilerTags);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
