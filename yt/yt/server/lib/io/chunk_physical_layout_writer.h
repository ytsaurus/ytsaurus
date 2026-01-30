#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

// IPhysicalLayerWriter
struct IPhysicalLayerWriter
    : public virtual TRefCounted
{
    virtual TFuture<void> Open() = 0;

    struct TWriteRequest
    {
        i64 StartOffset = 0;
        i64 EndOffset = 0;
        std::vector<TSharedRef> Buffers;
        i64 BlockCount = 0;
    };

    virtual bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        TWriteRequest request,
        TFairShareSlotId fairShareSlotId) = 0;

    virtual TFuture<void> GetReadyEvent() = 0;

    virtual TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TSharedMutableRef& chunkMetaBlob,
        TFairShareSlotId fairShareSlotId,
        i64 dataSize,
        i64 metadataSize) = 0;

    virtual const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const = 0;
    virtual NChunkClient::TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const = 0;

    virtual NChunkClient::TChunkId GetChunkId() const = 0;
    virtual NErasure::ECodec GetErasureCodecId() const = 0;

    virtual const TString& GetFileName() const = 0;

    virtual bool IsCloseDemanded() const = 0;

    virtual TFuture<void> Cancel() = 0;

    // for file
    virtual TFuture<void> PreallocateDiskSpace(
        const TWorkloadDescriptor& workloadDescriptor,
        i64 spaceSize) = 0;
};

DEFINE_REFCOUNTED_TYPE(IPhysicalLayerWriter)

////////////////////////////////////////////////////////////////////////////////

struct IWrapperFairShareChunkWriter
    : public NChunkClient::IChunkWriter
{
    using NChunkClient::IChunkWriter::Close;
    virtual TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TDeferredChunkMetaPtr& chunkMeta,
        TFairShareSlotId fairShareSlotId,
        std::optional<int> truncateBlocks) = 0;

    using NChunkClient::IChunkWriter::WriteBlocks;
    virtual bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<NChunkClient::TBlock>& blocks,
        TFairShareSlotId fairShareSlotId) = 0;

    //! Returns the chunk meta.
    /*!
     *  The writer must be already closed.
     */
    virtual const NChunkClient::TRefCountedChunkMetaPtr& GetChunkMeta() const = 0;

    //! Returns the name of the file passed to the writer upon construction.
    // TODO: NOT APLICABLE TO S3
    virtual const TString& GetFileName() const = 0;

    //! Returns the total data size accumulated so far.
    /*!
     *  Can be called at any time.
     */
    virtual i64 GetDataSize() const = 0;

    // TODO: NOT APLICABLE TO S3
    virtual TFuture<void> PreallocateDiskSpace(
        const TWorkloadDescriptor& workloadDescriptor,
        i64 spaceSize) = 0;
};

DEFINE_REFCOUNTED_TYPE(IWrapperFairShareChunkWriter);

////////////////////////////////////////////////////////////////////////////////

// TODO: virtual destructors!
IWrapperFairShareChunkWriterPtr CreateChunkLayoutWriterAdapter(
    IPhysicalLayerWriterPtr underlying,
    IInvokerPtr invoker,
    bool syncOnClose = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
