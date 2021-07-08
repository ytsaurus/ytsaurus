#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

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
    virtual TFuture<void> Open() override;

    virtual bool WriteBlock(const NChunkClient::TBlock& block) override;
    virtual bool WriteBlocks(const std::vector<NChunkClient::TBlock>& blocks) override;

    virtual TFuture<void> GetReadyEvent() override;

    virtual TFuture<void> Close(const NChunkClient::TDeferredChunkMetaPtr& chunkMeta) override;

    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;
    virtual const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override;
    virtual NChunkClient::TChunkReplicaWithMediumList GetWrittenChunkReplicas() const override;

    virtual NChunkClient::TChunkId GetChunkId() const override;

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
    const NChunkClient::TChunkId ChunkId_;
    const TString FileName_;
    const bool SyncOnClose_;

    using EState = EFileWriterState;
    std::atomic<EState> State_ = EFileWriterState::Created;
    TAtomicObject<TError> Error_;

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
