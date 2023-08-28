#pragma once

#include "public.h"
#include "chunk_reader.h"
#include "chunk_reader_allowing_repair.h"
#include "chunk_meta_extensions.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/library/erasure/impl/public.h>

namespace NYT::NChunkClient::NErasureHelpers {

////////////////////////////////////////////////////////////////////////////////

//! TPartRange identifies a byte range across all erasure parts. For a given part this includes padding zero
//! bytes of a stripe that are not explicitly stored.
struct TPartRange
{
    i64 Begin;
    i64 End;

    i64 Size() const;
    bool IsEmpty() const;

    explicit operator bool() const;
};

bool operator == (const TPartRange& lhs, const TPartRange& rhs);

TPartRange Intersection(const TPartRange& lhs, const TPartRange& rhs);

std::vector<TPartRange> Union(const std::vector<TPartRange>& ranges);

////////////////////////////////////////////////////////////////////////////////

class TParityPartSplitInfo
{
public:
    TParityPartSplitInfo() = default;
    TParityPartSplitInfo(const NProto::TErasurePlacementExt& placementExt);

    i64 GetStripeOffset(int stripeIndex) const;
    i64 GetPartSize() const;

    std::vector<TPartRange> GetParityBlockRanges() const;
    std::vector<TPartRange> GetBlockRanges(int partIndex, const NProto::TErasurePlacementExt& placementExt) const;
    std::vector<TPartRange> SplitRangesByStripesAndAlignToParityBlocks(const std::vector<TPartRange>& ranges) const;

private:
    i64 BlockSize_ = 0;
    std::vector<int> StripeBlockCounts_;
    std::vector<i64> StripeLastBlockSizes_;
};

////////////////////////////////////////////////////////////////////////////////

struct IPartBlockConsumer
    : public TRefCounted
{
    virtual TFuture<void> Consume(const TPartRange& range, const TSharedRef& block) = 0;
};

DECLARE_REFCOUNTED_TYPE(IPartBlockConsumer)
DEFINE_REFCOUNTED_TYPE(IPartBlockConsumer)

////////////////////////////////////////////////////////////////////////////////

struct IPartBlockProducer
    : public TRefCounted
{
    virtual TFuture<TSharedRef> Produce(const TPartRange& range) = 0;
};

DECLARE_REFCOUNTED_TYPE(IPartBlockProducer)
DEFINE_REFCOUNTED_TYPE(IPartBlockProducer)

////////////////////////////////////////////////////////////////////////////////

//! Subinterface of IChunkReader without any knowledge about meta information.
struct IBlocksReader
    : public TRefCounted
{
    virtual TFuture<std::vector<TBlock>> ReadBlocks(const std::vector<int>& blockIndexes) = 0;
};

DECLARE_REFCOUNTED_TYPE(IBlocksReader)
DEFINE_REFCOUNTED_TYPE(IBlocksReader)

////////////////////////////////////////////////////////////////////////////////

class TPartWriter
    : public IPartBlockConsumer
{
public:
    TPartWriter(
        const TWorkloadDescriptor& workloadDescriptor,
        IChunkWriterPtr writer,
        const std::vector<TPartRange>& blockRanges,
        bool computeChecksum);

    ~TPartWriter();

    TFuture<void> Consume(const TPartRange& range, const TSharedRef& block) override;

    TChecksum GetPartChecksum() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DECLARE_REFCOUNTED_TYPE(TPartWriter)
DEFINE_REFCOUNTED_TYPE(TPartWriter)

////////////////////////////////////////////////////////////////////////////////

class TPartReader
    : public IPartBlockProducer
{
public:
    TPartReader(IBlocksReaderPtr reader, const std::vector<TPartRange>& blockRanges);

    ~TPartReader();

    TFuture<TSharedRef> Produce(const TPartRange& range) override;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DECLARE_REFCOUNTED_TYPE(TPartReader)
DEFINE_REFCOUNTED_TYPE(TPartReader)

////////////////////////////////////////////////////////////////////////////////

//! Iterate over encode ranges and perform following logic:
//! retrieve blocks from parts producer, encode these blocks
//! and pass it to part consumers.
class TPartEncoder
    : public TRefCounted
{
public:
    TPartEncoder(
        const NYT::NErasure::ICodec* codec,
        const NYT::NErasure::TPartIndexList missingPartIndices,
        const TParityPartSplitInfo& splitInfo,
        const std::vector<TPartRange>& encodeRanges,
        const std::vector<IPartBlockProducerPtr>& producers,
        const std::vector<IPartBlockConsumerPtr>& consumers);

    ~TPartEncoder();

    void Run();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DECLARE_REFCOUNTED_TYPE(TPartEncoder)
DEFINE_REFCOUNTED_TYPE(TPartEncoder)

////////////////////////////////////////////////////////////////////////////////

TFuture<TRefCountedChunkMetaPtr> GetPlacementMeta(
    const IChunkReaderPtr& reader,
    const TClientChunkReadOptions& options);

////////////////////////////////////////////////////////////////////////////////

struct TDataBlocksPlacementInPart
{
    std::vector<int> IndexesInPart;
    std::vector<int> IndexesInRequest;
    std::vector<TPartRange> Ranges;
};

using TDataBlocksPlacementInParts = std::vector<TDataBlocksPlacementInPart>;

TDataBlocksPlacementInParts BuildDataBlocksPlacementInParts(
    const std::vector<int>& blockIndexes,
    const NProto::TErasurePlacementExt& placementExt,
    const TParityPartSplitInfo& paritySplitInfo);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient::NErasureHelpers
