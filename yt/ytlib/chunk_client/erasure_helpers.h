#pragma once

#include "public.h"
#include "chunk_reader.h"
#include "chunk_reader_allowing_repair.h"
#include "chunk_meta_extensions.h"

#include <yt/core/actions/future.h>

#include <yt/core/erasure/codec.h>

namespace NYT {
namespace NChunkClient {
namespace NErasureHelpers {

////////////////////////////////////////////////////////////////////////////////

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

struct TParityPartSplitInfo
{
    TParityPartSplitInfo() = default;
    TParityPartSplitInfo(int parityBlockCount, i64 parityBlockSize, i64 lastBlockSize);

    static TParityPartSplitInfo Build(i64 parityBlockSize, i64 parityPartSize);

    i64 GetPartSize() const;

    std::vector<TPartRange> GetRanges() const;
    std::vector<i64> GetSizes() const;

    int BlockCount = 0;
    i64 BlockSize = 0;
    i64 LastBlockSize = 0;
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
    TPartWriter(IChunkWriterPtr writer, const std::vector<i64>& blockSizes, bool computeChecksum);

    virtual TFuture<void> Consume(const TPartRange& range, const TSharedRef& block) override;

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
    TPartReader(IBlocksReaderPtr reader, const std::vector<i64>& blockSizes);

    virtual TFuture<TSharedRef> Produce(const TPartRange& range) override;

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

    void Run();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DECLARE_REFCOUNTED_TYPE(TPartEncoder)
DEFINE_REFCOUNTED_TYPE(TPartEncoder)

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TErasurePlacementExt> GetPlacementMeta(
    const IChunkReaderPtr& reader,
    const TWorkloadDescriptor& workloadDescriptor);

TParityPartSplitInfo GetParityPartSplitInfo(const NProto::TErasurePlacementExt& placementExt);

std::vector<i64> GetBlockSizes(int partIndex, const NProto::TErasurePlacementExt& placementExt);

////////////////////////////////////////////////////////////////////////////////

struct TDataBlocksPlacementInPart
{
    std::vector<int> IndexesInPart;
    std::vector<int> IndexesInRequest;
    std::vector<TPartRange> Ranges;
};

typedef std::vector<TDataBlocksPlacementInPart> TDataBlocksPlacementInParts;

TDataBlocksPlacementInParts BuildDataBlocksPlacementInParts(
    const std::vector<int>& blockIndexes,
    const NProto::TErasurePlacementExt& placementExt);

////////////////////////////////////////////////////////////////////////////////

class TErasureChunkReaderBase
    : public IChunkReader
{
public:
    TErasureChunkReaderBase(NErasure::ICodec* codec, const std::vector<IChunkReaderAllowingRepairPtr>& readers);

    virtual TFuture<NProto::TChunkMeta> GetMeta(
        const TWorkloadDescriptor& workloadDescriptor,
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>>& extensionTags) override;

    virtual TChunkId GetChunkId() const override;

protected:
    TFuture<void> PreparePlacementMeta(const TWorkloadDescriptor& workloadDescriptor);
    void OnGotPlacementMeta(const NProto::TErasurePlacementExt& placementExt);

    NErasure::ICodec* const Codec_;
    const std::vector<IChunkReaderAllowingRepairPtr> Readers_;

    TSpinLock PlacementExtLock_;
    TFuture<void> PlacementExtFuture_;
    NProto::TErasurePlacementExt PlacementExt_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasureHelpers
} // namespace NChunkClient
} // namespace NYT

