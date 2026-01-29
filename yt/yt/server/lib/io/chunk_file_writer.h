#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <util/system/file.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFileWriterState,
    (Created)
    (Opening)
    (Ready)
    (WritingBlocks)
    (Closing)
    (Closed)
    (Aborting)
    (Aborted)
    (Failed)
);

// IPhysicalLayerDataWriter
struct IPhysicalChunkWriter
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
};

DEFINE_REFCOUNTED_TYPE(IPhysicalChunkWriter)

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
    virtual const TString& GetFileName() const = 0;

    //! Returns the total data size accumulated so far.
    /*!
     *  Can be called at any time.
     */
    virtual i64 GetDataSize() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IWrapperFairShareChunkWriter);

// TODO(cherepashka): remove the implementation from the header file.
class TChunkFileWriter
    : public IPhysicalChunkWriter
{
public:
    TChunkFileWriter(
        IIOEnginePtr ioEngine,
        NChunkClient::TChunkId chunkId,
        TString fileName,
        bool syncOnClose = true,
        bool useDirectIO = false);

    // IChunkWriter implementation.
    TFuture<void> Open() override;

    TFuture<void> PreallocateDiskSpace(
        const TWorkloadDescriptor& workloadDescriptor,
        i64 spaceSize);

    bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        TWriteRequest request,
        TFairShareSlotId fairShareSlotId) override;

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TSharedMutableRef& chunkMetaBlob,
        TFairShareSlotId fairShareSlotId,
        i64 dataSize,
        i64 metadataSize) override;

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override;
    NChunkClient::TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override;

    NChunkClient::TChunkId GetChunkId() const override;

    NErasure::ECodec GetErasureCodecId() const override;

    bool IsCloseDemanded() const override;

    //! Returns the name of the file passed to the writer upon construction.
    const TString& GetFileName() const override;

    //! Aborts the writer and removes temporary files.
    TFuture<void> Cancel() override;

private:
    const IIOEnginePtr IOEngine_;
    const TString FileName_;
    const NChunkClient::TChunkId ChunkId_;
    const bool SyncOnClose_;
    const bool UseDirectIO_;
    const NLogging::TLogger Logger;

    using EState = EFileWriterState;
    std::atomic<EState> State_ = EFileWriterState::Created;
    NThreading::TAtomicObject<TError> Error_;

    TFuture<void> ReadyEvent_ = OKFuture;

    i64 DiskSpace_ = 0;

    TIOEngineHandlePtr DataFile_;

    void TryLockDataFile(TPromise<void> promise);

    void SetFailed(const TError& error);
    TError TryChangeState(EState oldState, EState newState);
    TFlags<EOpenModeFlag> GetFileMode() const;
};

DEFINE_REFCOUNTED_TYPE(TChunkFileWriter)

////////////////////////////////////////////////////////////////////////////////

IWrapperFairShareChunkWriterPtr CreateChunkFileWriter(
    IIOEnginePtr ioEngine,
    NChunkClient::TChunkId chunkId,
    TString fileName,
    bool syncOnClose = true,
    bool useDirectIO = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
