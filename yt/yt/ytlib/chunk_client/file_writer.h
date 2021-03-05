#pragma once

#include "chunk_writer.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <util/system/file.h>

namespace NYT::NChunkClient {

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

//! Provides a local and synchronous implementation of #IAsyncWriter.
class TFileWriter
    : public IChunkWriter
{
public:
    TFileWriter(
        IIOEnginePtr ioEngine,
        TChunkId chunkId,
        TString fileName,
        bool syncOnClose = true);

    // IChunkWriter implementation.
    virtual TFuture<void> Open() override;

    virtual bool WriteBlock(const TBlock& block) override;
    virtual bool WriteBlocks(const std::vector<TBlock>& blocks) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close(const TDeferredChunkMetaPtr& chunkMeta) override;

    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;
    virtual const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override;
    virtual TChunkReplicaWithMediumList GetWrittenChunkReplicas() const override;

    virtual TChunkId GetChunkId() const override;

    virtual NErasure::ECodec GetErasureCodecId() const override;

    virtual bool IsCloseDemanded() const override;

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
    TFuture<void> Abort();

private:
    const IIOEnginePtr IOEngine_;
    const TChunkId ChunkId_;
    const TString FileName_;
    const bool SyncOnClose_;

    using EState = EFileWriterState;
    std::atomic<EState> State_ = EFileWriterState::Created;

    TFuture<void> ReadyEvent_ = VoidFuture;

    i64 DataSize_ = 0;
    i64 MetaDataSize_ = 0;

    std::shared_ptr<TFileHandle> DataFile_;

    const TRefCountedChunkMetaPtr ChunkMeta_ = New<TRefCountedChunkMeta>();
    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    void TryLockDataFile(TPromise<void> promise);
};

DEFINE_REFCOUNTED_TYPE(TFileWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
