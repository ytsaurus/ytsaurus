#include "confirming_writer.h"
#include "private.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "dispatcher.h"
#include "erasure_writer.h"
#include "helpers.h"
#include "block.h"
#include "replication_writer.h"
#include "chunk_service_proxy.h"
#include "helpers.h"
#include "session_id.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/finally.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NChunkClient {

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
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        TNodeDirectoryPtr nodeDirectory,
        NNative::IClientPtr client,
        IBlockCachePtr blockCache,
        IThroughputThrottlerPtr throttler,
        TTrafficMeterPtr trafficMeter)
        : Config_(CloneYsonSerializable(config))
        , Options_(options)
        , CellTag_(cellTag)
        , TransactionId_(transactionId)
        , ParentChunkListId_(parentChunkListId)
        , NodeDirectory_(nodeDirectory)
        , Client_(client)
        , BlockCache_(blockCache)
        , Throttler_(throttler)
        , TrafficMeter_(trafficMeter)
        , Logger(NLogging::TLogger(ChunkClientLogger)
            .AddTag("TransactionId: %v", TransactionId_))
    {
        Config_->UploadReplicationFactor = std::min(
            Config_->UploadReplicationFactor,
            Options_->ReplicationFactor);
        Config_->MinUploadReplicationFactor = std::min(
            Config_->MinUploadReplicationFactor,
            Options_->ReplicationFactor);
    }


    virtual TFuture<void> Open() override
    {
        YCHECK(!Initialized_);
        YCHECK(!OpenFuture_);

        OpenFuture_ = BIND(&TConfirmingWriter::OpenSession, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
        return OpenFuture_;
    }

    virtual bool WriteBlock(const TBlock& block) override
    {
        return WriteBlocks({block});
    }

    virtual bool WriteBlocks(const std::vector<TBlock>& blocks) override
    {
        YCHECK(Initialized_);
        YCHECK(OpenFuture_.IsSet());

        if (!OpenFuture_.Get().IsOK()) {
            return false;
        } else {
            return UnderlyingWriter_->WriteBlocks(blocks);
        }
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        YCHECK(Initialized_);
        YCHECK(OpenFuture_.IsSet());
        if (!OpenFuture_.Get().IsOK()) {
            return OpenFuture_;
        } else {
            return UnderlyingWriter_->GetReadyEvent();
        }
    }

    virtual TFuture<void> Close(const TRefCountedChunkMetaPtr& chunkMeta) override
    {
        YCHECK(Initialized_);
        YCHECK(OpenFuture_.IsSet());

        ChunkMeta_ = chunkMeta;

        return BIND(
            &TConfirmingWriter::DoClose,
            MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual const NProto::TChunkInfo& GetChunkInfo() const override
    {
        YCHECK(Closed_);
        return UnderlyingWriter_->GetChunkInfo();
    }

    virtual const NProto::TDataStatistics& GetDataStatistics() const override
    {
        YCHECK(Closed_);
        return DataStatistics_;
    }

    virtual TChunkReplicaList GetWrittenChunkReplicas() const override
    {
        YCHECK(UnderlyingWriter_);
        return UnderlyingWriter_->GetWrittenChunkReplicas();
    }

    virtual TChunkId GetChunkId() const override
    {
        return SessionId_.ChunkId;
    }

    virtual NErasure::ECodec GetErasureCodecId() const override
    {
        return Options_->ErasureCodec;
    }

    virtual bool HasSickReplicas() const override
    {
        if (UnderlyingWriter_) {
            return UnderlyingWriter_->HasSickReplicas();
        } else {
            return false;
        }
    }

private:
    const TMultiChunkWriterConfigPtr Config_;
    const TMultiChunkWriterOptionsPtr Options_;
    const TCellTag CellTag_;
    const TTransactionId TransactionId_;
    const TChunkListId ParentChunkListId_;
    const TNodeDirectoryPtr NodeDirectory_;
    const NNative::IClientPtr Client_;
    const IBlockCachePtr BlockCache_;
    const IThroughputThrottlerPtr Throttler_;
    const TTrafficMeterPtr TrafficMeter_;

    IChunkWriterPtr UnderlyingWriter_;

    std::atomic<bool> Initialized_ = {false};
    std::atomic<bool> Closed_ = {false};
    TSessionId SessionId_;
    TFuture<void> OpenFuture_;

    TRefCountedChunkMetaPtr ChunkMeta_;
    NProto::TDataStatistics DataStatistics_;

    NLogging::TLogger Logger;

    void OpenSession()
    {
        auto finally = Finally([&] () {
            Initialized_ = true;
        });

        SessionId_ = NChunkClient::CreateChunk(
            Client_,
            CellTag_,
            Options_,
            TransactionId_,
            ParentChunkListId_,
            Logger);

        Logger.AddTag("ChunkId: %v", SessionId_);
        LOG_DEBUG("Chunk created");

        UnderlyingWriter_ = CreateUnderlyingWriter();
        WaitFor(UnderlyingWriter_->Open())
            .ThrowOnError();

        LOG_DEBUG("Chunk writer opened");
    }

    IChunkWriterPtr CreateUnderlyingWriter() const
    {
        if (Options_->ErasureCodec == ECodec::None) {
            return CreateReplicationWriter(
                Config_,
                Options_,
                SessionId_,
                TChunkReplicaList(),
                NodeDirectory_,
                Client_,
                BlockCache_,
                TrafficMeter_,
                Throttler_);
        }

        auto* erasureCodec = GetCodec(Options_->ErasureCodec);
        // NB(psushin): we don't ask master for new erasure replicas,
        // because we cannot guarantee proper replica placement.
        auto options = CloneYsonSerializable(Options_);
        options->AllowAllocatingNewTargetNodes = Config_->EnableErasureTargetNodeReallocation;
        auto writers = CreateErasurePartWriters(
            Config_,
            options,
            SessionId_,
            erasureCodec,
            NodeDirectory_,
            Client_,
            TrafficMeter_,
            Throttler_,
            BlockCache_);
        return CreateErasureWriter(
            Config_,
            SessionId_,
            Options_->ErasureCodec,
            erasureCodec,
            writers,
            Config_->WorkloadDescriptor);
    }

    void DoClose()
    {
        auto error = WaitFor(UnderlyingWriter_->Close(ChunkMeta_));

        THROW_ERROR_EXCEPTION_IF_FAILED(
            error,
            "Failed to close chunk %v",
            SessionId_.ChunkId);

        LOG_DEBUG("Chunk closed");

        auto replicas = UnderlyingWriter_->GetWrittenChunkReplicas();
        YCHECK(!replicas.empty());

        static const THashSet<int> masterMetaTags{
            TProtoExtensionTag<NChunkClient::NProto::TMiscExt>::Value,
            TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value
        };

        NChunkClient::NProto::TChunkMeta masterChunkMeta(*ChunkMeta_);
        FilterProtoExtensions(
            masterChunkMeta.mutable_extensions(),
            ChunkMeta_->extensions(),
            masterMetaTags);

        // Sanity check.
        YCHECK(FindProtoExtension<NChunkClient::NProto::TMiscExt>(masterChunkMeta.extensions()));

        auto channel = Client_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTag_);
        TChunkServiceProxy proxy(channel);

        auto batchReq = proxy.ExecuteBatch();
        GenerateMutationId(batchReq);
        batchReq->set_suppress_upstream_sync(true);

        auto* req = batchReq->add_confirm_chunk_subrequests();
        ToProto(req->mutable_chunk_id(), SessionId_.ChunkId);
        *req->mutable_chunk_info() = UnderlyingWriter_->GetChunkInfo();
        req->mutable_chunk_meta()->Swap(&masterChunkMeta);
        req->set_request_statistics(true);
        ToProto(req->mutable_replicas(), replicas);

        auto batchRspOrError = WaitFor(batchReq->Invoke());
        THROW_ERROR_EXCEPTION_IF_FAILED(
            GetCumulativeError(batchRspOrError),
            EErrorCode::MasterCommunicationFailed,
            "Failed to confirm chunk %v",
            SessionId_.ChunkId);

        const auto& batchRsp = batchRspOrError.Value();
        const auto& rsp = batchRsp->confirm_chunk_subresponses(0);
        DataStatistics_ = rsp.statistics();

        Closed_ = true;

        LOG_DEBUG("Chunk confirmed");
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateConfirmingWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    TCellTag cellTag,
    const TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NNative::IClientPtr client,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler)
{
    return New<TConfirmingWriter>(
        config,
        options,
        cellTag,
        transactionId,
        parentChunkListId,
        nodeDirectory,
        client,
        blockCache,
        throttler,
        trafficMeter);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
