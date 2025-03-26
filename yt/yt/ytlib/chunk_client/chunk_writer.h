#pragma once

#include "chunk_writer_options.h"
#include "public.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/erasure/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Provides a basic interface for uploading chunks to a suitable target.
struct IChunkWriter
    : public virtual TRefCounted
{
    struct TWriteBlocksOptions
    {
        TClientChunkWriteOptions ClientOptions;
    };

    //! Starts a new upload session.
    virtual TFuture<void> Open() = 0;

    //! Enqueues another block to be written.
    /*!
     *  If |false| is returned then the block was accepted but the window is already full.
     *  The client must call #GetReadyEvent and wait for the result to be set.
     */
    virtual bool WriteBlock(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) = 0;

    //! Similar to #WriteBlock but enqueues a bunch of blocks at once.
    virtual bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) = 0;

    //! Returns an asynchronous flag used to backpressure the upload.
    virtual TFuture<void> GetReadyEvent() = 0;

    //! Called when the client has added all blocks and is
    //! willing to finalize the upload.
    /*!
     *  For journal chunks, #chunkMeta is not used.
     *  Blocks truncation may not be supported by some writers.
     */
    virtual TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor = {},
        const TDeferredChunkMetaPtr& chunkMeta = nullptr,
        std::optional<int> truncateBlockCount = std::nullopt) = 0;

    //! Returns the chunk info.
    /*!
     *  This method can only be called when the writer is successfully closed.
     */
    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const = 0;

    //! Returns the chunk data statistics.
    /*!
     *  This method can only be called when the writer is successfully closed.
     *  Currently only lazy chunk writer supports this call.
     */
    virtual const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const = 0;

    //! Returns the indices of replicas that were successfully written.
    /*!
     *  Can only be called when the writer is successfully closed.
     *  Not every writer implements this method.
     */
    virtual TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const = 0;

    //! Returns the id of the chunk being written.
    /*!
     *  Can only be called when the writer is successfully open.
     */
    virtual TChunkId GetChunkId() const = 0;

    //! Returns the erasure codec of the chunk being written.
    virtual NErasure::ECodec GetErasureCodecId() const = 0;

    //! Returns true if one of the replicas demanded transmission close.
    virtual bool IsCloseDemanded() const = 0;

    //! Cancels chunk write. The returned future is set when cancellation completes. Do not call other
    //! methods after this one.
    virtual TFuture<void> Cancel() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
