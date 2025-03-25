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

////////////////////////////////////////////////////////////////////////////////

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

class TChunkFileWriter
    : public NChunkClient::IChunkWriter
{
public:
    TChunkFileWriter(
        IIOEnginePtr ioEngine,
        NChunkClient::TChunkId chunkId,
        TString fileName,
        bool syncOnClose = true);

    // IChunkWriter implementation.
    TFuture<void> Open() override;

    bool WriteBlock(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TBlock& block) override;

    bool WriteBlock(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TBlock& block,
        TFairShareSlotId fairShareSlotId);

    bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<NChunkClient::TBlock>& blocks) override;

    bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<NChunkClient::TBlock>& blocks,
        TFairShareSlotId fairShareSlotId);

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TDeferredChunkMetaPtr& chunkMeta,
        std::optional<int> truncateBlocks) override;

    TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TDeferredChunkMetaPtr& chunkMeta,
        TFairShareSlotId fairShareSlotId,
        std::optional<int> truncateBlocks);

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;
    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override;
    NChunkClient::TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override;

    NChunkClient::TChunkId GetChunkId() const override;

    NErasure::ECodec GetErasureCodecId() const override;

    bool IsCloseDemanded() const override;

    //! Returns the chunk meta.
    /*!
     *  The writer must be already closed.
     */
    const NChunkClient::TRefCountedChunkMetaPtr& GetChunkMeta() const;

    //! Returns the name of the file passed to the writer upon construction.
    const TString& GetFileName() const;

    //! Returns the total data size accumulated so far.
    /*!
     *  Can be called at any time.
     */
    i64 GetDataSize() const;

    //! Aborts the writer and removes temporary files.
    TFuture<void> Cancel() override;

private:
    const IIOEnginePtr IOEngine_;
    const NChunkClient::TChunkId ChunkId_;
    const TString FileName_;
    const bool SyncOnClose_;

    using EState = EFileWriterState;
    std::atomic<EState> State_ = EFileWriterState::Created;
    NThreading::TAtomicObject<TError> Error_;

    TFuture<void> ReadyEvent_ = VoidFuture;

    i64 DataSize_ = 0;
    i64 MetaDataSize_ = 0;

    TIOEngineHandlePtr DataFile_;

    const NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_ = New<NChunkClient::TRefCountedChunkMeta>();
    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    void TryLockDataFile(TPromise<void> promise);

    void SetFailed(const TError& error);
    TError TryChangeState(EState oldState, EState newState);
};

DEFINE_REFCOUNTED_TYPE(TChunkFileWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
