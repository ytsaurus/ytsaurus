#pragma once

#include "public.h"
// #include "chunk_physical_layout_writer.h"

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

    struct TPhysicalWriteRequest
    {
        i64 StartOffset = 0;
        i64 EndOffset = 0;
        std::vector<TSharedRef> Buffers;
        i64 BlockCount = 0;
    };
// TODO(cherepashka): move the implementation away from the header file.
class TChunkFileWriter
    // : public IPhysicalLayerWriter
    // TODO: public TSomeCommonInterface
{
public:
    struct TOptions
    {
        IIOEnginePtr IoEngine;
        IInvokerPtr Invoker;
        NChunkClient::TChunkId ChunkId;
        TString FileName;
        bool SyncOnClose;
        bool UseDirectIO;
    };
    // todo: hide into private
    TChunkFileWriter(TOptions options);

    // TChunkFileWriter(const TChunkFileWriter& other);
    // TChunkFileWriter(TChunkFileWriter&& other) = default;

    TFuture<void> Open();

    TFuture<void> PreallocateDiskSpace(
        const TWorkloadDescriptor& workloadDescriptor,
        i64 spaceSize);

    bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        TPhysicalWriteRequest request,
        TFairShareSlotId fairShareSlotId);

    TFuture<void> GetReadyEvent();

    TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TSharedMutableRef& chunkMetaBlob,
        TFairShareSlotId fairShareSlotId,
        i64 dataSize,
        i64 metadataSize);

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const;
    NChunkClient::TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const;

    NChunkClient::TChunkId GetChunkId() const;

    NErasure::ECodec GetErasureCodecId() const;

    bool IsCloseDemanded() const;

    //! Returns the name of the file passed to the writer upon construction.
    const TString& GetFileName() const;

    //! Aborts the writer and removes temporary files.
    TFuture<void> Cancel();

    virtual ~TChunkFileWriter() = default;

private:
    const IIOEnginePtr IOEngine_;
    const TString FileName_;
    const NChunkClient::TChunkId ChunkId_;
    const bool SyncOnClose_;
    const bool UseDirectIO_;
    const NLogging::TLogger Logger;
    const IInvokerPtr Invoker_;

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

// DEFINE_REFCOUNTED_TYPE(TChunkFileWriter)

////////////////////////////////////////////////////////////////////////////////

TChunkLayoutWriterAdapterPtr<TChunkFileWriter> CreateChunkFileWriter(TChunkFileWriter::TOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
