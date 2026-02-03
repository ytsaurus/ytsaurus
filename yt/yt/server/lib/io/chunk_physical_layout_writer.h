#pragma once

#include "public.h"

#include "chunk_file_writer.h"

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct IPhysicalLayerWriter
{
    virtual TFuture<void> Open() = 0;

    virtual bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        TPhysicalWriteRequest request,
        TFairShareSlotId fairShareSlotId) = 0;

    virtual TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TSharedMutableRef& chunkMetaBlob,
        TFairShareSlotId fairShareSlotId,
        i64 dataSize,
        i64 metadataSize) = 0;

    virtual NChunkClient::TChunkId GetChunkId() const = 0;

    virtual TFuture<void> Cancel() = 0;
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPhysicalLayerChunkWriterAdapterState,
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

template<class TBaseChunkWriter>
class TChunkLayoutWriterAdapter final
    : public TBaseChunkWriter
{
public:
    bool WriteBlock(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TBlock& block);

    bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<NChunkClient::TBlock>& blocks,
        TFairShareSlotId fairShareSlotId = {});
    TFuture<void> Open();

    TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TDeferredChunkMetaPtr& chunkMeta,
        std::optional<int> truncateBlockCount,
        TFairShareSlotId fairShareSlotId = {});

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const;
    const NChunkClient::TRefCountedChunkMetaPtr& GetChunkMeta() const;
    i64 GetDataSize() const;

    TChunkLayoutWriterAdapter(TBaseChunkWriter::TOptions options);
private:
    const NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_ = New<NChunkClient::TRefCountedChunkMeta>();
    const NLogging::TLogger Logger;
    const IInvokerPtr Invoker_;

    using EState = EPhysicalLayerChunkWriterAdapterState;
    std::atomic<EState> PState_ = EPhysicalLayerChunkWriterAdapterState::Created;

    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    // todo: what about thread safety?
    i64 DataSize_ = 0;
    i64 MetaDataSize_ = 0;

    TError TryChangeState(EState oldState, EState newState);
    void UpdateChunkInfoDiskSpace();

    void FinalizeChunkMeta(NChunkClient::TDeferredChunkMetaPtr chunkMeta);

    TSharedMutableRef PrepareChunkMetaBlob();

    friend TChunkLayoutWriterAdapterPtr<TBaseChunkWriter> CreateChunkLayoutWriterAdapter(typename TBaseChunkWriter::TOptions options);

};

DEFINE_REFCOUNTED_TYPE(TChunkLayoutWriterAdapter<TChunkFileWriter>)

////////////////////////////////////////////////////////////////////////////////

template<class TBaseChunkWriter>
TChunkLayoutWriterAdapterPtr<TBaseChunkWriter> CreateChunkLayoutWriterAdapter(typename TBaseChunkWriter::TOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

#define CONVERT_INL_H_
#include "chunk_physical_layout_writer-inl.h"
#undef CONVERT_INL_H_