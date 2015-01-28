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
        IClientPtr masterClient,
        const std::vector<TTransactionId>& prerequisiteTransactionIds)
        : Config_(config)
        , Options_(options)
        , Path_(path)
        , MasterClient_(masterClient)
        , PrerequisiteTransactionIds_(prerequisiteTransactionIds)
    {
        Logger.AddTag("Path: %v", Path_);
    }

    virtual ISnapshotReaderPtr CreateReader(int snapshotId) override
    {
        return New<TReader>(this, snapshotId);
    }

    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const TSnapshotMeta& meta) override
    {
        return New<TWriter>(this, snapshotId, meta);
    }

    virtual TFuture<int> GetLatestSnapshotId(int maxSnapshotId) override
    {
        return BIND(&TRemoteSnapshotStore::DoGetLatestSnapshotId, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(maxSnapshotId);
    }

private:
    TRemoteSnapshotStoreConfigPtr Config_;
    TRemoteSnapshotStoreOptionsPtr Options_;
    TYPath Path_;
    IClientPtr MasterClient_;
    std::vector<TTransactionId> PrerequisiteTransactionIds_;

    NLog::TLogger Logger = HydraLogger;


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

        virtual TFuture<size_t> Read(void* buf, size_t len) override
        {
            return BIND(&TReader::DoRead, MakeStrong(this))
                .AsyncVia(GetHydraIOInvoker())
                .Run(buf, len);
        }

        virtual TSnapshotParams GetParams() const override
        {
            return Params_;
        }

    private:
        TRemoteSnapshotStorePtr Store_;
        int SnapshotId_;

        TYPath Path_;

        TSnapshotParams Params_;

        IFileReaderPtr Reader_;

        TSharedRef CurrentBlock_;
        size_t CurrentOffset_ = -1;

        NLog::TLogger Logger = HydraLogger;


        void DoOpen()
        {
            try {
                LOG_DEBUG("Requesting remote snapshot parameters");
                INodePtr node;
                {
                    TGetNodeOptions options;
                    options.AttributeFilter.Mode = EAttributeFilterMode::MatchingOnly;
                    options.AttributeFilter.Keys.push_back("prev_record_count");
                    auto asyncResult = Store_->MasterClient_->GetNode(Path_, options);
                    auto result = WaitFor(asyncResult)
                        .ValueOrThrow();
                    node = ConvertToNode(result);
                }
                LOG_DEBUG("Remote snapshot parameters received");

                {
                    const auto& attributes = node->Attributes();
                    Params_.Meta.set_prev_record_count(attributes.Get<i64>("prev_record_count"));
                    Params_.Checksum = 0;
                    Params_.CompressedLength = Params_.UncompressedLength = -1;
                }

                LOG_DEBUG("Opening remote snapshot reader");
                {
                    TFileReaderOptions options;
                    options.Config = Store_->Config_->Reader;
                    Reader_ = Store_->MasterClient_->CreateFileReader(Store_->GetSnapshotPath(SnapshotId_), options);

                    WaitFor(Reader_->Open())
                        .ThrowOnError();
                }
                LOG_DEBUG("Remote snapshot reader opened");
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error opening remote snapshot %v for reading",
                    Path_)
                    << ex;
            }
        }

        size_t DoRead(void* buf, size_t len)
        {
            try {
                if (!CurrentBlock_ || CurrentOffset_ >= CurrentBlock_.Size()) {
                    // NB: This is a part of TInputStream implementation; block, do not yield.
                    CurrentBlock_ = Reader_->Read()
                        .Get()
                        .ValueOrThrow();
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
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error reading remote snapshot %v",
                    Path_)
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

        virtual TFuture<void> Write(const void* buf, size_t len) override
        {
            YCHECK(Opened_ && !Closed_);
            Length_ += len;
            return Writer_->Write(TRef(const_cast<void*>(buf), len));
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
        TRemoteSnapshotStorePtr Store_;
        int SnapshotId_;
        TSnapshotMeta Meta_;

        TYPath Path_;

        ITransactionPtr Transaction_;

        IFileWriterPtr Writer_;
        i64 Length_ = 0;
        TSnapshotParams Params_;
        bool Opened_ = false;
        bool Closed_ = false;

        NLog::TLogger Logger = HydraLogger;


        void DoOpen()
        {
            try {
                YCHECK(!Opened_);

                LOG_DEBUG("Starting remote snapshot upload transaction");
                {
                    TTransactionStartOptions options;
                    auto attributes = CreateEphemeralAttributes();
                    attributes->Set("title", Format("Snapshot upload to %v",
                        Path_));
                    options.Attributes = std::move(attributes);

                    auto asyncResult = Store_->MasterClient_->StartTransaction(
                        NTransactionClient::ETransactionType::Master,
                        options);
                    Transaction_ = WaitFor(asyncResult)
                        .ValueOrThrow();
                }
                LOG_DEBUG("Remote snapshot upload transaction started (TransactionId: %v)",
                    Transaction_->GetId());

                LOG_DEBUG("Creating remote snapshot");
                {
                    TCreateNodeOptions options;
                    auto attributes = CreateEphemeralAttributes();
                    attributes->Set("replication_factor", Store_->Options_->SnapshotReplicationFactor);
                    attributes->Set("prev_record_count", Meta_.prev_record_count());
                    options.Attributes = std::move(attributes);
                    options.PrerequisiteTransactionIds = Store_->PrerequisiteTransactionIds_;

                    auto asyncResult = Transaction_->CreateNode(
                        Path_,
                        EObjectType::File,
                        options);
                    WaitFor(asyncResult)
                        .ThrowOnError();
                }
                LOG_DEBUG("Remote snapshot created");

                LOG_DEBUG("Opening remote snapshot writer");
                {
                    TFileWriterOptions options;
                    options.TransactionId = Transaction_->GetId();
                    options.PrerequisiteTransactionIds = Store_->PrerequisiteTransactionIds_;
                    options.Config = Store_->Config_->Writer;
                    Writer_ = Store_->MasterClient_->CreateFileWriter(Store_->GetSnapshotPath(SnapshotId_), options);

                    WaitFor(Writer_->Open())
                        .ThrowOnError();
                }
                LOG_DEBUG("Remote snapshot writer opened");

                Opened_ = true;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error opening remote snapshot %v for writing",
                    Path_)
                    << ex;
            }
        }

        void DoClose()
        {
            try {
                YCHECK(Opened_ && !Closed_);

                LOG_DEBUG("Closing remote snapshot writer");
                WaitFor(Writer_->Close())
                    .ThrowOnError();
                LOG_DEBUG("Remote snapshot writer closed");

                LOG_DEBUG("Committing snapshot upload transaction");
                WaitFor(Transaction_->Commit())
                    .ThrowOnError();
                LOG_DEBUG("Snapshot upload transaction committed");

                Params_.Meta = Meta_;
                Params_.CompressedLength = Length_;
                Params_.UncompressedLength = Length_;

                Closed_ = true;
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error closing remote snapshot %v",
                    Path_)
                    << ex;
            }
        }

    };


    int DoGetLatestSnapshotId(int maxSnapshotId)
    {
        try {
            LOG_DEBUG("Requesting snapshot list from remote store");
            auto asyncResult = MasterClient_->ListNode(Path_);
            auto result = WaitFor(asyncResult)
                .ValueOrThrow();
            LOG_DEBUG("Snapshot list received");

            auto keys = ConvertTo<std::vector<Stroka>>(result);
            int lastestSnapshotId = NonexistingSegmentId;
            for (const auto& key : keys) {
                int id;
                try {
                    id = FromString<int>(key);
                } catch (const std::exception& ex) {
                    LOG_WARNING("Unrecognized item %Qv in remote store %v",
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
            THROW_ERROR_EXCEPTION("Error computing the latest snapshot id in remote store %v",
                Path_)
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
    IClientPtr masterClient,
    const std::vector<TTransactionId>& prerequisiteTransactionIds)
{
    return New<TRemoteSnapshotStore>(
        config,
        options,
        path,
        masterClient,
        prerequisiteTransactionIds);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
