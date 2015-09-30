#include "stdafx.h"

#include "lazy_chunk_writer.h"
#include "config.h"
#include "chunk_ypath_proxy.h"
#include "chunk_replica.h"
#include "private.h"
#include "dispatcher.h"
#include "replication_writer.h"
#include "erasure_writer.h"
#include "chunk_meta_extensions.h"
#include "helpers.h"

#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>

#include <ytlib/table_client/chunk_meta_extensions.h>

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <core/erasure/codec.h>

#include <core/misc/finally.h>

#include <core/ytree/yson_serializable.h>

#include <core/concurrency/scheduler.h>

#include <core/logging/log.h>

namespace NYT {
namespace NChunkClient {

using namespace NApi;
using namespace NProto;
using namespace NRpc;
using namespace NObjectClient;
using namespace NErasure;
using namespace NConcurrency;
using namespace NYTree;
using namespace NTableClient;
using namespace NNodeTrackerClient;

///////////////////////////////////////////////////////////////////////////////

class TLazyChunkWriter
    : public IChunkWriter
{
public:
    TLazyChunkWriter(
        TMultiChunkWriterConfigPtr config,
        TMultiChunkWriterOptionsPtr options,
        const TTransactionId& transactionId,
        const TChunkListId& parentChunkListId,
        TNodeDirectoryPtr nodeDirectory,
        IClientPtr client,
        IBlockCachePtr blockCache,
        IThroughputThrottlerPtr throttler);

    virtual TFuture<void> Open() override;
    virtual bool WriteBlock(const TSharedRef& block) override;
    virtual bool WriteBlocks(const std::vector<TSharedRef>& blocks) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close(const NProto::TChunkMeta& chunkMeta) override;

    virtual const NProto::TChunkInfo& GetChunkInfo() const override;

    virtual TChunkReplicaList GetWrittenChunkReplicas() const override;

    virtual TChunkId GetChunkId() const override;

private:
    TMultiChunkWriterConfigPtr Config_;
    TMultiChunkWriterOptionsPtr Options_;

    const NTransactionClient::TTransactionId TransactionId_;
    const TChunkListId ParentChunkListId_;

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    NApi::IClientPtr Client_;
    IBlockCachePtr BlockCache_;
    NConcurrency::IThroughputThrottlerPtr Throttler_;

    IChunkWriterPtr UnderlyingWriter_;

    std::atomic<bool> Initialized_ = { false };
    TChunkId ChunkId_ = NullChunkId;
    std::vector<TSharedRef> PendingBlocks_;

    TFuture<void> OpenedFuture_ = VoidFuture;

    NProto::TChunkMeta ChunkMeta_;

    NLogging::TLogger Logger;

    void OpenSession();
    TChunkId CreateChunk() const;
    IChunkWriterPtr CreateUnderlyingWriter() const;
    void DoClose();
};

///////////////////////////////////////////////////////////////////////////////

TLazyChunkWriter::TLazyChunkWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    const NTransactionClient::TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NApi::IClientPtr client,
    IBlockCachePtr blockCache,
    NConcurrency::IThroughputThrottlerPtr throttler)
    : Config_(config)
    , Options_(options)
    , TransactionId_(transactionId)
    , ParentChunkListId_(parentChunkListId)
    , NodeDirectory_(nodeDirectory)
    , Client_(client)
    , BlockCache_(blockCache)
    , Throttler_(throttler)
    , Logger(ChunkClientLogger)
{
    Logger.AddTag("TransactionId: %v", TransactionId_);
}

TFuture<void> TLazyChunkWriter::Open()
{
    return VoidFuture;
}

bool TLazyChunkWriter::WriteBlock(const TSharedRef& block)
{
    return WriteBlocks(std::vector<TSharedRef>(1, block));
}

bool TLazyChunkWriter::WriteBlocks(const std::vector<TSharedRef>& blocks)
{
    if (!Initialized_) {
        // We haven't started lazy chunk creation yet.
        YCHECK(OpenedFuture_.IsSet());
        PendingBlocks_.insert(PendingBlocks_.end(), blocks.begin(), blocks.end());
        OpenedFuture_ = BIND(&TLazyChunkWriter::OpenSession, MakeWeak(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();

        return false;
    } else if (!OpenedFuture_.Get().IsOK()) {
        return false;
    } else {
        return UnderlyingWriter_->WriteBlocks(blocks);
    }
}

TFuture<void> TLazyChunkWriter::GetReadyEvent()
{
    if (!Initialized_ || !OpenedFuture_.Get().IsOK()) {
        return OpenedFuture_;
    } else {
        return UnderlyingWriter_->GetReadyEvent();
    }
}

TFuture<void> TLazyChunkWriter::Close(const TChunkMeta& chunkMeta)
{
    ChunkMeta_ = chunkMeta;
    return OpenedFuture_.Apply(BIND(
        &TLazyChunkWriter::DoClose,
        MakeWeak(this))
    .AsyncVia(TDispatcher::Get()->GetWriterInvoker()));
}

const TChunkInfo& TLazyChunkWriter::GetChunkInfo() const
{
    YCHECK(UnderlyingWriter_);
    return UnderlyingWriter_->GetChunkInfo();
}

TChunkReplicaList TLazyChunkWriter::GetWrittenChunkReplicas() const
{
    YCHECK(UnderlyingWriter_);
    return UnderlyingWriter_->GetWrittenChunkReplicas();
}

TChunkId TLazyChunkWriter::GetChunkId() const
{
    return ChunkId_;
}

void TLazyChunkWriter::OpenSession()
{
    TFinallyGuard finally([&] () {
        Initialized_ = true;
    });

    LOG_DEBUG(
        "Creating chunk (ReplicationFactor: %v)",
        Options_->ReplicationFactor);

    ChunkId_ = CreateChunk();

    Logger.AddTag("ChunkId: %v", ChunkId_);
    LOG_DEBUG("Chunk created");

    UnderlyingWriter_ = CreateUnderlyingWriter();
    WaitFor(UnderlyingWriter_->Open())
        .ThrowOnError();

    LOG_DEBUG("Chunk writer opened");

    if (!UnderlyingWriter_->WriteBlocks(PendingBlocks_)) {
        WaitFor(UnderlyingWriter_->GetReadyEvent())
            .ThrowOnError();
    }
    PendingBlocks_.clear();

    LOG_DEBUG("Initial blocks written");
}

TChunkId TLazyChunkWriter::CreateChunk() const
{
    try {
        return NChunkClient::CreateChunk(
            Client_,
            CellTagFromId(ParentChunkListId_),
            Options_,
            TransactionId_,
            ParentChunkListId_,
            Logger);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::MasterCommunicationFailed,
            "Error creating chunk")
            << ex;
    }
}

IChunkWriterPtr TLazyChunkWriter::CreateUnderlyingWriter() const
{
    if (Options_->ErasureCodec == ECodec::None) {
        return CreateReplicationWriter(
            Config_,
            Options_,
            ChunkId_,
            TChunkReplicaList(),
            NodeDirectory_,
            Client_,
            BlockCache_,
            Throttler_);
    }

    auto* erasureCodec = GetCodec(Options_->ErasureCodec);
    // NB(psushin): we don't ask master for new erasure replicas,
    // because we cannot guarantee proper replica placement.
    auto options = CloneYsonSerializable(Options_);
    options->AllowAllocatingNewTargetNodes = false;
    auto writers = CreateErasurePartWriters(
        Config_,
        options,
        ChunkId_,
        erasureCodec,
        NodeDirectory_,
        Client_,
        Throttler_,
        BlockCache_);
    return CreateErasureWriter(
        Config_,
        ChunkId_,
        erasureCodec,
        writers);
}

void TLazyChunkWriter::DoClose()
{
    auto error = WaitFor(UnderlyingWriter_->Close(ChunkMeta_));

    THROW_ERROR_EXCEPTION_IF_FAILED(
        error,
        "Failed to close chunk %v",
        ChunkId_);

    LOG_DEBUG("Chunk closed");

    auto replicas = UnderlyingWriter_->GetWrittenChunkReplicas();
    YCHECK(!replicas.empty());

    static const yhash_set<int> masterMetaTags{
        TProtoExtensionTag<TMiscExt>::Value,
        TProtoExtensionTag<NTableClient::NProto::TBoundaryKeysExt>::Value,
        TProtoExtensionTag<NTableClient::NProto::TOldBoundaryKeysExt>::Value
    };

    auto masterChunkMeta = ChunkMeta_;
    FilterProtoExtensions(
        masterChunkMeta.mutable_extensions(),
        ChunkMeta_.extensions(),
        masterMetaTags);

    auto req = TChunkYPathProxy::Confirm(FromObjectId(ChunkId_));
    GenerateMutationId(req);
    *req->mutable_chunk_info() = UnderlyingWriter_->GetChunkInfo();
    *req->mutable_chunk_meta() = masterChunkMeta;

    NYT::ToProto(req->mutable_replicas(), replicas);

    TObjectServiceProxy objectProxy(Client_->GetMasterChannel(EMasterChannelKind::Leader));
    auto rspOrError = WaitFor(objectProxy.Execute(req));

    THROW_ERROR_EXCEPTION_IF_FAILED(
        rspOrError,
        EErrorCode::MasterCommunicationFailed,
        "Failed to confirm chunk %v",
        ChunkId_);

    LOG_DEBUG("Chunk confirmed");
}

///////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateLazyChunkWriter(
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    const NTransactionClient::TTransactionId& transactionId,
    const TChunkListId& parentChunkListId,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NApi::IClientPtr client,
    IBlockCachePtr blockCache,
    NConcurrency::IThroughputThrottlerPtr throttler)
{
    return New<TLazyChunkWriter>(
        config,
        options,
        transactionId,
        parentChunkListId,
        nodeDirectory,
        client,
        blockCache,
        throttler);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
