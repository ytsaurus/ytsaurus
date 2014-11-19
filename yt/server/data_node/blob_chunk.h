#pragma once

#include "public.h"
#include "chunk_detail.h"

#include <core/misc/async_cache.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! A base for both TStoredBlobChunk and TCachedBlobChunk.
class TBlobChunkBase
    : public TChunkBase
{
public:
    virtual NChunkClient::NProto::TChunkInfo GetInfo() const override;

    virtual bool IsActive() const override;

    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncReadBlocksResult ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority) override;

    virtual void SyncRemove() override;

protected:
    TBlobChunkBase(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta);
    ~TBlobChunkBase();

    virtual TFuture<void> AsyncRemove() override;

private:
    NChunkClient::NProto::TChunkInfo Info_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;


    void DoSyncRemove(const Stroka& dataFileName);

    TAsyncError ReadMeta(i64 priority);
    void DoReadMeta(
        TChunkReadGuard readGuard,
        TPromise<TError> promise);
    void InitializeCachedMeta(const NChunkClient::NProto::TChunkMeta& meta);

    void AdjustReadRange(
        int firstBlockIndex,
        int* blockCount, // inout
        i64* dataSize);  // out

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPendingReadSizeGuard pendingReadSizeGuard,
        TPromise<TErrorOr<std::vector<TSharedRef>>> promise);


};

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkStore.
class TStoredBlobChunk
    : public TBlobChunkBase
{
public:
    TStoredBlobChunk(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta = nullptr);

};

DEFINE_REFCOUNTED_TYPE(TStoredBlobChunk)

////////////////////////////////////////////////////////////////////////////////

//! A blob chunk owned by TChunkCache.
class TCachedBlobChunk
    : public TBlobChunkBase
    , public TAsyncCacheValueBase<TChunkId, TCachedBlobChunk>
{
public:
    TCachedBlobChunk(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkDescriptor& descriptor,
        const NChunkClient::NProto::TChunkMeta* meta = nullptr);

    ~TCachedBlobChunk();

private:
    TWeakPtr<TChunkCache> ChunkCache_;

};

DEFINE_REFCOUNTED_TYPE(TCachedBlobChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

