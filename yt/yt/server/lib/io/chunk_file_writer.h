#pragma once

#include "public.h"
#include "chunk_physical_layout_writer.h"

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

// TODO(cherepashka): move the implementation away from the header file.
class TChunkFileWriter
    : public IIPhysicalLayerWriter
{
public:
    TChunkFileWriter(
        IIOEnginePtr ioEngine,
        NChunkClient::TChunkId chunkId,
        TString fileName,
        bool syncOnClose = true,
        bool useDirectIO = false);

    TFuture<void> Open() override;

    TFuture<void> PreallocateDiskSpace(
        const TWorkloadDescriptor& workloadDescriptor,
        i64 spaceSize) override;

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
