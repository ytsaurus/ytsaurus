#include "stdafx.h"
#include "remote_changelog_store.h"
#include "changelog.h"
#include "config.h"
#include "lazy_changelog.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/scheduler.h>

#include <core/ytree/attribute_helpers.h>

#include <core/logging/log.h>

#include <ytlib/api/client.h>
#include <ytlib/api/journal_reader.h>
#include <ytlib/api/journal_writer.h>

#include <ytlib/hydra/hydra_manager.pb.h>

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

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStore
    : public IChangelogStore
{
public:
    TRemoteChangelogStore(
        TRemoteChangelogStoreConfigPtr config,
        TRemoteChangelogStoreOptionsPtr options,
        const TYPath& remotePath,
        IClientPtr masterClient,
        const std::vector<TTransactionId>& prerequisiteTransactionIds)
        : Config_(config)
        , Options_(options)
        , RemotePath_(remotePath)
        , MasterClient_(masterClient)
        , PrerequisiteTransactionIds_(prerequisiteTransactionIds)
    {
        Logger.AddTag("Path: %v", RemotePath_);
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

    virtual TFuture<int> GetLatestChangelogId(int initialId) override
    {
        return BIND(&TRemoteChangelogStore::DoGetLatestChangelog, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(initialId);
    }

private:
    TRemoteChangelogStoreConfigPtr Config_;
    TRemoteChangelogStoreOptionsPtr Options_;
    TYPath RemotePath_;
    IClientPtr MasterClient_;
    std::vector<TTransactionId> PrerequisiteTransactionIds_;

    NLog::TLogger Logger = HydraLogger;


    IChangelogPtr DoCreateChangelog(int id, const TChangelogMeta& meta)
    {
        auto path = GetRemotePath(id);

        LOG_DEBUG("Creating changelog %v",
            id);

        {
            TCreateNodeOptions options;
            auto attributes = CreateEphemeralAttributes();
            attributes->Set("replication_factor", Options_->ChangelogReplicationFactor);
            attributes->Set("read_quorum", Options_->ChangelogReadQuorum);
            attributes->Set("write_quorum", Options_->ChangelogWriteQuorum);
            attributes->Set("prev_record_count", meta.prev_record_count());
            options.Attributes = attributes.get();
            options.PrerequisiteTransactionIds = PrerequisiteTransactionIds_;

            auto result = WaitFor(MasterClient_->CreateNode(
                path,
                EObjectType::Journal,
                options));
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        IJournalWriterPtr writer;
        {
            TJournalWriterOptions options;
            options.PrerequisiteTransactionIds = PrerequisiteTransactionIds_;
            writer = MasterClient_->CreateJournalWriter(
                path,
                options,
                Config_->Writer);
            auto result = WaitFor(writer->Open());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
        }

        LOG_DEBUG("Changelog %v created",
            id);

        return CreateRemoteChangelog(
            id,
            path,
            meta,
            writer,
            0,
            0);
    }

    IChangelogPtr DoOpenChangelog(int id)
    {
        auto path = GetRemotePath(id);

        TChangelogMeta meta;
        int recordCount;
        i64 dataSize;

        LOG_DEBUG("Getting attributes of changelog %v",
            id);
        {
            TGetNodeOptions options;
            options.AttributeFilter.Mode = EAttributeFilterMode::MatchingOnly;
            options.AttributeFilter.Keys.push_back("sealed");
            options.AttributeFilter.Keys.push_back("prev_record_count");
            options.AttributeFilter.Keys.push_back("uncompressed_data_size");
            auto result = WaitFor(MasterClient_->GetNode(path, options));
            if (result.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::NoSuchChangelog,
                    "Changelog %v does not exist in remote store %v",
                    id,
                    RemotePath_);                
            }
            THROW_ERROR_EXCEPTION_IF_FAILED(result);

            auto node = ConvertToNode(result.Value());
            const auto& attributes = node->Attributes();

            if (!attributes.Get<bool>("sealed")) {
                THROW_ERROR_EXCEPTION("Changelog %v in remote store %v is not sealed",
                    id,
                    RemotePath_);
            }

            meta.set_prev_record_count(attributes.Get<int>("prev_record_count"));
            dataSize = attributes.Get<i64>("uncompressed_data_size");
        }
        LOG_DEBUG("Changelog %v attributes received",
            id);

        // TODO(babenko): consolidate with the above when YT-624 is done
        LOG_DEBUG("Getting quorum record count for changelog %v",
            id);
        {
            auto result = WaitFor(MasterClient_->GetNode(path + "/@quorum_row_count"));
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
            recordCount = ConvertTo<int>(result.Value());
        }
        LOG_DEBUG("Changelog %v quorum record count received",
            id);

        return CreateRemoteChangelog(
            id,
            path,
            meta,
            nullptr,
            recordCount,
            dataSize);
    }

    int DoGetLatestChangelog(int initialId)
    {
        LOG_DEBUG("Requesting changelog list from remote store");
        auto result = WaitFor(MasterClient_->ListNodes(RemotePath_));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        LOG_DEBUG("Changelog list received");

        auto keys = ConvertTo<std::vector<Stroka>>(result.Value());
        int latestId = NonexistingSegmentId;
        yhash_set<int> ids;

        for (const auto& key : keys) {
            int id;
            try {
                id = FromString<int>(key);
            } catch (const std::exception&) {
                LOG_WARNING("Unrecognized item %Qv in remote store %v",
                    key,
                    RemotePath_);
                continue;
            }
            YCHECK(ids.insert(id).second);
            if (id >= initialId && (id > latestId || latestId == NonexistingSegmentId)) {
                latestId = id;
            }
        }

        if (latestId != NonexistingSegmentId) {
            for (int id = initialId; id <= latestId; ++id) {
                if (ids.find(id) == ids.end()) {
                    THROW_ERROR_EXCEPTION("Interim changelog %v is missing in remote store %v",
                        id,
                        RemotePath_);                    
                }
            }
        }

        return latestId;
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

    TYPath GetRemotePath(int id)
    {
        return Format("%v/%09d", RemotePath_, id);
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
            , RecordCount_(recordCount)
            , DataSize_(dataSize)
            , Owner_(owner)
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

        virtual bool IsSealed() const override
        {
            // TODO(babenko): implement
            return false;
        }

        virtual TFuture<void> Append(const TSharedRef& data) override
        {
            YCHECK(Writer_);
            DataSize_ += data.Size();
            RecordCount_ += 1;
            FlushResult_ = Writer_->Write(std::vector<TSharedRef>(1, data));
            return FlushResult_;
        }

        virtual TFuture<void> Flush() override
        {
            return FlushResult_;
        }

        virtual std::vector<TSharedRef> Read(
            int firstRecordId,
            int maxRecords,
            i64 /*maxBytes*/) const override
        {
            TJournalReaderOptions options;
            options.FirstRowIndex = firstRecordId;
            options.RowCount = maxRecords;

            auto reader = Owner_->MasterClient_->CreateJournalReader(
                Path_,
                options,
                Owner_->Config_->Reader);

            THROW_ERROR_EXCEPTION_IF_FAILED(WaitFor(reader->Open()));

            auto result = WaitFor(reader->Read());
            THROW_ERROR_EXCEPTION_IF_FAILED(result);
            return result.Value();
        }

        virtual TFuture<void> Seal(int recordCount) override
        {
            // TODO(babenko): implement
            YCHECK(recordCount == RecordCount_);
            return VoidFuture;
        }

        virtual TFuture<void> Unseal() override
        {
            YUNREACHABLE();
        }

        virtual TFuture<void> Close() override
        {
            YCHECK(Writer_);
            return Writer_->Close();
        }

    private:
        TYPath Path_;
        TChangelogMeta Meta_;
        IJournalWriterPtr Writer_;
        int RecordCount_;
        i64 DataSize_;
        TRemoteChangelogStorePtr Owner_;

        TFuture<void> FlushResult_ = VoidFuture;

    };

};

DEFINE_REFCOUNTED_TYPE(TRemoteChangelogStore)

IChangelogStorePtr CreateRemoteChangelogStore(
    TRemoteChangelogStoreConfigPtr config,
    TRemoteChangelogStoreOptionsPtr options,
    const TYPath& remotePath,
    IClientPtr masterClient,
    const std::vector<TTransactionId>& prerequisiteTransactionIds)
{
    return New<TRemoteChangelogStore>(
        config,
        options,
        remotePath,
        masterClient,
        prerequisiteTransactionIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
