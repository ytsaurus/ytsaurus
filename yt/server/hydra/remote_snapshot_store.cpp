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
            , Path_(Store_->GetRemotePath(SnapshotId_))
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
            LOG_DEBUG("Requesting remote snapshot parameters");
            INodePtr node;
            {
                TGetNodeOptions options;
                options.AttributeFilter.Mode = EAttributeFilterMode::MatchingOnly;
                options.AttributeFilter.Keys.push_back("prev_record_count");
                auto result = WaitFor(Store_->MasterClient_->GetNode(Path_, options));
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
                node = ConvertToNode(result.Value());
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
                Reader_ = Store_->MasterClient_->CreateFileReader(Store_->GetRemotePath(SnapshotId_), options);

                auto result = Reader_->Open().Get();
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
            LOG_DEBUG("Remote snapshot reader opened");
        }

        size_t DoRead(void* buf, size_t len)
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

    };

    class TWriter
        : public ISnapshotWriter
    {
    public:
        TWriter(TRemoteSnapshotStorePtr store, int snapshotId, const TSnapshotMeta& meta)
            : Store_(store)
            , SnapshotId_(snapshotId)
            , Meta_(meta)
            , Path_(Store_->GetRemotePath(SnapshotId_))
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
            YCHECK(!Opened_);

            LOG_DEBUG("Starting remote snapshot upload transaction");
            {
                TTransactionStartOptions options;
                auto attributes = CreateEphemeralAttributes();
                attributes->Set("title", Format("Snapshot upload to %v",
                    Path_));
                options.Attributes = std::move(attributes);

                auto transactionOrError = WaitFor(Store_->MasterClient_->StartTransaction(
                    NTransactionClient::ETransactionType::Master,
                    options));
                Transaction_ = transactionOrError.ValueOrThrow();
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

                auto error = WaitFor(Transaction_->CreateNode(
                    Path_,
                    EObjectType::File,
                    options));
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }
            LOG_DEBUG("Remote snapshot created");

            LOG_DEBUG("Opening remote snapshot writer");
            {
                TFileWriterOptions options;
                options.TransactionId = Transaction_->GetId();
                options.PrerequisiteTransactionIds = Store_->PrerequisiteTransactionIds_;
                options.Config = Store_->Config_->Writer;
                Writer_ = Store_->MasterClient_->CreateFileWriter(Store_->GetRemotePath(SnapshotId_), options);

                auto error = WaitFor(Writer_->Open());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }
            LOG_DEBUG("Remote snapshot writer opened");

            Opened_ = true;
        }

        void DoClose()
        {
            YCHECK(Opened_ && !Closed_);

            LOG_DEBUG("Closing remote snapshot writer");
            {
                auto result = WaitFor(Writer_->Close());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
            LOG_DEBUG("Remote snapshot writer closed");

            LOG_DEBUG("Committing snapshot upload transaction");
            {
                auto result = WaitFor(Transaction_->Commit());
                THROW_ERROR_EXCEPTION_IF_FAILED(result);
            }
            LOG_DEBUG("Snapshot upload transaction committed");

            Params_.Meta = Meta_;
            Params_.CompressedLength = Length_;
            Params_.UncompressedLength = Length_;

            Closed_ = true;
        }

    };


    int DoGetLatestSnapshotId(int maxSnapshotId)
    {
        LOG_DEBUG("Requesting snapshot list from remote store");
        auto result = WaitFor(MasterClient_->ListNodes(Path_));
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        LOG_DEBUG("Snapshot list received");

        auto keys = ConvertTo<std::vector<Stroka>>(result.Value());
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
    }

    TYPath GetRemotePath(int snapshotId)
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
