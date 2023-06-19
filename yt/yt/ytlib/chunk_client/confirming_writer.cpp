#include "confirming_writer.h"

#include "block.h"
#include "chunk_service_proxy.h"
#include "config.h"
#include "deferred_chunk_meta.h"
#include "dispatcher.h"
#include "erasure_part_writer.h"
#include "erasure_writer.h"
#include "helpers.h"
#include "private.h"
#include "replication_writer.h"
#include "session_id.h"
#include "striped_erasure_writer.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NChunkClient {

using namespace NApi;
using namespace NRpc;
using namespace NObjectClient;
using namespace NErasure;
using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;
using namespace NNodeTrackerClient;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TConfirmingWriter
    : public IChunkWriter
{
public:
    TConfirmingWriter(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        TCellTag cellTag,
        TTransactionId transactionId,
        TChunkListId parentChunkListId,
        NNative::IClientPtr client,
        TString localHostName,
        IBlockCachePtr blockCache,
        IThroughputThrottlerPtr throttler,
        TTrafficMeterPtr trafficMeter,
        TSessionId sessionId,
        TChunkReplicaWithMediumList targetReplicas)
        : Config_(CloneYsonStruct(config))
        , Options_(CloneYsonStruct(options))
        , CellTag_(cellTag)
        , TransactionId_(transactionId)
        , ParentChunkListId_(parentChunkListId)
        , Client_(std::move(client))
        , LocalHostName_(std::move(localHostName))
        , BlockCache_(std::move(blockCache))
        , Throttler_(std::move(throttler))
        , TrafficMeter_(std::move(trafficMeter))
        , TargetReplicas_(std::move(targetReplicas))
        , SessionId_(sessionId)
        , Logger(ChunkClientLogger.WithTag("TransactionId: %v", TransactionId_))
    {
        Config_->UploadReplicationFactor = std::min(
            Config_->UploadReplicationFactor,
            Options_->ReplicationFactor);
        Config_->MinUploadReplicationFactor = std::min(
            Config_->MinUploadReplicationFactor,
            Options_->ReplicationFactor);

        // TODO(gritukan): Unify.
        Config_->EnableStripedErasure |= Options_->EnableStripedErasure;

        if (Config_->UseEffectiveErasureCodecs) {
            Options_->ErasureCodec = GetEffectiveCodecId(Options_->ErasureCodec);
        }
    }

    TFuture<void> Open() override
    {
        YT_VERIFY(!Initialized_);
        YT_VERIFY(!OpenFuture_);

        OpenFuture_ = BIND(&TConfirmingWriter::OpenSession, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
        return OpenFuture_;
    }

    bool WriteBlock(const TWorkloadDescriptor& workloadDescriptor, const TBlock& block) override
    {
        return WriteBlocks(workloadDescriptor, {block});
    }

    bool WriteBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override
    {
        YT_VERIFY(Initialized_);
        YT_VERIFY(OpenFuture_.IsSet());

        if (!OpenFuture_.Get().IsOK()) {
            return false;
        } else {
            return UnderlyingWriter_->WriteBlocks(workloadDescriptor, blocks);
        }
    }

    TFuture<void> GetReadyEvent() override
    {
        YT_VERIFY(Initialized_);
        YT_VERIFY(OpenFuture_.IsSet());
        if (!OpenFuture_.Get().IsOK()) {
            return OpenFuture_;
        } else {
            return UnderlyingWriter_->GetReadyEvent();
        }
    }

    TFuture<void> Close(
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& chunkMeta) override
    {
        YT_VERIFY(Initialized_);
        YT_VERIFY(OpenFuture_.IsSet());

        ChunkMeta_ = chunkMeta;

        return BIND(
            &TConfirmingWriter::DoClose,
            MakeWeak(this),
            workloadDescriptor)
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    const NProto::TChunkInfo& GetChunkInfo() const override
    {
        YT_VERIFY(Closed_);
        return UnderlyingWriter_->GetChunkInfo();
    }

    const NProto::TDataStatistics& GetDataStatistics() const override
    {
        if (!Closed_) {
            static const NProto::TDataStatistics EmptyDataStatistics;
            return EmptyDataStatistics;
        }

        return DataStatistics_;
    }

    TChunkReplicaWithLocationList GetWrittenChunkReplicas() const override
    {
        YT_VERIFY(UnderlyingWriter_);
        return UnderlyingWriter_->GetWrittenChunkReplicas();
    }

    TChunkId GetChunkId() const override
    {
        return SessionId_.ChunkId;
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        return Options_->ErasureCodec;
    }

    bool IsCloseDemanded() const override
    {
        if (UnderlyingWriter_) {
            return UnderlyingWriter_->IsCloseDemanded();
        } else {
            return false;
        }
    }

    TFuture<void> Cancel() override
    {
        YT_VERIFY(UnderlyingWriter_);
        return UnderlyingWriter_->Cancel();
    }

private:
    const TMultiChunkWriterConfigPtr Config_;
    const TMultiChunkWriterOptionsPtr Options_;
    const TCellTag CellTag_;
    const TTransactionId TransactionId_;
    const TChunkListId ParentChunkListId_;
    const NNative::IClientPtr Client_;
    const TString LocalHostName_;
    const IBlockCachePtr BlockCache_;
    const IThroughputThrottlerPtr Throttler_;
    const TTrafficMeterPtr TrafficMeter_;
    const TChunkReplicaWithMediumList TargetReplicas_;

    IChunkWriterPtr UnderlyingWriter_;

    std::atomic<bool> Initialized_ = false;
    std::atomic<bool> Closed_ = false;
    TSessionId SessionId_;
    TFuture<void> OpenFuture_;

    TDeferredChunkMetaPtr ChunkMeta_;
    NProto::TDataStatistics DataStatistics_;

    NLogging::TLogger Logger;

    void OpenSession()
    {
        auto finally = Finally([&] () {
            Initialized_ = true;
        });

        if (SessionId_.ChunkId) {
            YT_LOG_DEBUG("Writing existing chunk (ChunkId: %v)", SessionId_.ChunkId);
        } else {
            if (Options_->Account.empty()) {
                THROW_ERROR_EXCEPTION("Error creating chunk: account should not be empty");
            }
            SessionId_ = NChunkClient::CreateChunk(
                Client_,
                CellTag_,
                Options_,
                TransactionId_,
                ParentChunkListId_,
                Logger);
            YT_LOG_DEBUG("Chunk created");
        }

        Logger.AddTag("ChunkId: %v", SessionId_);

        UnderlyingWriter_ = CreateUnderlyingWriter();
        WaitFor(UnderlyingWriter_->Open())
            .ThrowOnError();

        YT_LOG_DEBUG("Chunk writer opened");
    }

    IChunkWriterPtr CreateUnderlyingWriter() const
    {
        if (Options_->ErasureCodec == ECodec::None) {
            return CreateReplicationWriter(
                Config_,
                Options_,
                SessionId_,
                std::move(TargetReplicas_),
                Client_,
                LocalHostName_,
                BlockCache_,
                TrafficMeter_,
                Throttler_);
        }

        auto* erasureCodec = GetCodec(Options_->ErasureCodec);
        // NB(psushin): we don't ask master for new erasure replicas,
        // because we cannot guarantee proper replica placement.
        auto options = CloneYsonStruct(Options_);
        options->AllowAllocatingNewTargetNodes = Config_->EnableErasureTargetNodeReallocation;
        auto config = CloneYsonStruct(Config_);
        // Block reordering is done in erasure writer.
        config->EnableBlockReordering = false;

        auto writers = CreateAllErasurePartWriters(
            config,
            options,
            SessionId_,
            erasureCodec,
            Client_,
            TrafficMeter_,
            Throttler_,
            BlockCache_,
            std::move(TargetReplicas_));
        if (Config_->EnableStripedErasure) {
            return CreateStripedErasureWriter(
                Config_,
                Options_->ErasureCodec,
                SessionId_,
                Config_->WorkloadDescriptor,
                std::move(writers));
        } else {
            return CreateErasureWriter(
                Config_,
                SessionId_,
                Options_->ErasureCodec,
                writers,
                Config_->WorkloadDescriptor);
        }
    }

    void DoClose(const TWorkloadDescriptor& workloadDescriptor)
    {
        auto error = WaitFor(UnderlyingWriter_->Close(workloadDescriptor, ChunkMeta_));

        THROW_ERROR_EXCEPTION_IF_FAILED(
            error,
            "Failed to close chunk %v",
            SessionId_.ChunkId);

        YT_LOG_DEBUG("Chunk closed");

        auto replicas = UnderlyingWriter_->GetWrittenChunkReplicas();
        YT_VERIFY(!replicas.empty());

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTag_);
        TChunkServiceProxy proxy(channel);

        auto req = proxy.ConfirmChunk();
        GenerateMutationId(req);
        ToProto(req->mutable_chunk_id(), SessionId_.ChunkId);
        *req->mutable_chunk_info() = UnderlyingWriter_->GetChunkInfo();
        *req->mutable_chunk_meta() = *ChunkMeta_;

        auto* multicellSyncExt = req->Header().MutableExtension(NObjectClient::NProto::TMulticellSyncExt::multicell_sync_ext);
        multicellSyncExt->set_suppress_upstream_sync(true);

        auto memoryUsageGuard = TMemoryUsageTrackerGuard::Acquire(
            Options_->MemoryTracker,
            req->mutable_chunk_meta()->ByteSize());

        FilterProtoExtensions(req->mutable_chunk_meta()->mutable_extensions(), GetMasterChunkMetaExtensionTagsFilter());
        req->set_request_statistics(true);
        ToProto(req->mutable_legacy_replicas(), replicas);

        req->set_location_uuids_supported(true);

        for (const auto& replica : replicas) {
            auto* replicaInfo = req->add_replicas();
            replicaInfo->set_replica(ToProto<ui64>(replica));
            ToProto(replicaInfo->mutable_location_uuid(), replica.GetChunkLocationUuid());
        }

        auto rspOrError = WaitFor(req->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            rspOrError,
            EErrorCode::MasterCommunicationFailed,
            "Failed to confirm chunk %v",
            SessionId_.ChunkId);

        const auto& rsp = rspOrError.Value();
        DataStatistics_ = rsp->statistics();

        Closed_ = true;

        YT_LOG_DEBUG("Chunk confirmed");
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateConfirmingWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    TCellTag cellTag,
    TTransactionId transactionId,
    TChunkListId parentChunkListId,
    NNative::IClientPtr client,
    TString localHostName,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    TSessionId sessionId,
    TChunkReplicaWithMediumList targetReplicas)
{
    return New<TConfirmingWriter>(
        std::move(config),
        std::move(options),
        cellTag,
        transactionId,
        parentChunkListId,
        std::move(client),
        std::move(localHostName),
        std::move(blockCache),
        std::move(throttler),
        std::move(trafficMeter),
        sessionId,
        std::move(targetReplicas));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
