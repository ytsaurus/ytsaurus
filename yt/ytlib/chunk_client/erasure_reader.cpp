#include "erasure_reader.h"

#include "async_reader.h"
#include "chunk_meta_extensions.h"
#include "dispatcher.h"

#include <ytlib/actions/parallel_awaiter.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TErasureReader
    : public IAsyncReader
{
public:
    explicit TErasureReader(const std::vector<IAsyncReaderPtr>& readers)
        : Readers_(readers)
    {
        YCHECK(!Readers_.empty());
    }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = NULL) override;

    virtual TChunkId GetChunkId() const override;

private:
    TError GetBlocksRanges();

    std::vector<IAsyncReaderPtr> Readers_;

    std::vector<NProto::TBlocksRange> BlocksRanges_;

    // TODO(ignat): REFACTOR IT
    // It should be seprate for different read queries.
    std::vector<TSharedRef> Result_;
    TAsyncReadPromise ResultPromise_;

    TSpinLock AddReadErrorLock_;
    std::vector<TError> ReadErrors_;
};

///////////////////////////////////////////////////////////////////////////////

namespace {

struct BlockRangeComparator {
    bool operator()(int position, const NProto::TBlocksRange& range) {
        return position < range.start();
    }
};

} // anonymous namespace

///////////////////////////////////////////////////////////////////////////////

TError TErasureReader::GetBlocksRanges()
{
    std::vector<int> tags;
    tags.push_back(TProtoExtensionTag<NProto::TErasurePlacementExt>::Value);

    auto chunkMeta = AsyncGetChunkMeta(Null, &tags).Get();
    if (!chunkMeta.IsOK()) {
        return TError("Cannot get chunk meta");
    }

    auto extension = GetProtoExtension<NProto::TErasurePlacementExt>(chunkMeta.GetOrThrow().extensions());
    BlocksRanges_ = std::vector<NProto::TBlocksRange>(extension.ranges().begin(), extension.ranges().end());

    YCHECK(BlocksRanges_.front().start() == 0);
    for (int i = 0; i + 1 < BlocksRanges_.size(); ++i) {
        YCHECK(BlocksRanges_[i].start() + BlocksRanges_[i].count() == BlocksRanges_[i + 1].start());
    }

    return TError();
}

IAsyncReader::TAsyncReadResult TErasureReader::AsyncReadBlocks(const std::vector<int>& blockIndexes)
{
    ResultPromise_ = NewPromise<TReadResult>();

    if (BlocksRanges_.empty()) {
        auto error = GetBlocksRanges();
        if (!error.IsOK()) {
            ResultPromise_.Set(error);
            return ResultPromise_;
        }
    }

    Result_.clear();
    Result_.resize(blockIndexes.size());
    
    // For each reader we find blocks to read and their initial indices
    std::vector<std::pair<std::vector<int>, std::vector<int>>> BlockLocations_(Readers_.size());

    int pos = 0;
    FOREACH(int index, blockIndexes) {
        YCHECK(index >= 0);

        auto it = upper_bound(BlocksRanges_.begin(), BlocksRanges_.end(), index, BlockRangeComparator());
        YASSERT(it != BlocksRanges_.begin());
        do {
            --it;
        } while (it != BlocksRanges_.begin() && (it->start() > index || it->count() == 0));

        std::cerr << index << " " << it->start() << " " << it->count() << std::endl;

        YCHECK(it != BlocksRanges_.end());
        YCHECK(index >= it->start());
        int readerIndex = it - BlocksRanges_.begin();
        int blockIndex = index - it->start();
        YCHECK(blockIndex < it->count());

        BlockLocations_[readerIndex].first.push_back(blockIndex);
        BlockLocations_[readerIndex].second.push_back(pos++);
    }
    
    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetReaderInvoker());
    for (int i = 0; i < Readers_.size(); ++i) {
        const auto& blocks = BlockLocations_[i].first;
        const auto& initialIndices = BlockLocations_[i].second;
        auto reader = Readers_[i];
        awaiter->Await(
            reader->AsyncReadBlocks(blocks),
            BIND([=](TReadResult result) {
                if (result.IsOK()) {
                    auto refs = result.GetOrThrow();
                    for (int j = 0; j < refs.size(); ++j) {
                        Result_[initialIndices[j]] = refs[j];
                    }
                }
                else {
                    TGuard<TSpinLock> guard(AddReadErrorLock_);
                    ReadErrors_.push_back(result);
                }
            })
        );
    }

    awaiter->Complete(
        BIND([&]() {
            if (ReadErrors_.empty()) {
                ResultPromise_.Set(Result_);
            }
            else {
                auto error = TError("Failed read erasure chunk");
                error.InnerErrors() = ReadErrors_;
            }
        })
    );

    return ResultPromise_;
}

IAsyncReader::TAsyncGetMetaResult TErasureReader::AsyncGetChunkMeta(
    const TNullable<int>& partitionTag,
    const std::vector<int>* tags)
{
    // TODO(ignat): add check that requests extensions indepent of the chunk part
    YCHECK(!partitionTag);
    return Readers_.front()->AsyncGetChunkMeta(partitionTag, tags);
}

TChunkId TErasureReader::GetChunkId() const
{
    return Readers_.front()->GetChunkId();
}


///////////////////////////////////////////////////////////////////////////////

class TRepairReader
{
public:
    TRepairReader(
        NErasure::ICodec* codec,
        const std::vector<int>& erasedIndices,
        const std::vector<IAsyncReaderPtr>& readers)
            : Codec_(codec)
            , ErasedIndices_(erasedIndices)
            , Readers_(readers)
    {
        YASSERT(Codec_->GetRecoveryIndices(ErasedIndices_));
        YASSERT(Codec_->GetRecoveryIndices(ErasedIndices_)->size() == Readers_.size());
    }




private:
    NErasure::ICodec* Codec_;
    std::vector<int> ErasedIndices_;
    std::vector<IAsyncReaderPtr> Readers_;
};


///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateErasureReader(
    const std::vector<IAsyncReaderPtr>& dataBlocksReaders)
{
    return New<TErasureReader>(dataBlocksReaders);
}

///////////////////////////////////////////////////////////////////////////////

//IAsyncReaderPtr CreateErasureRepairReader(
//    NErasure::ICodec* codec,
//    const std::vector<int>& erasedIndices,
//    const std::vector<IAsyncReaderPtr>& readers);

///////////////////////////////////////////////////////////////////////////////

//TAsyncError RepairErasedBlocks(
//    NErasure::ICodec* codec,
//    const std::vector<int>& erasedIndices,
//    const std::vector<IAsyncReaderPtr>& readers,
//    const std::vector<IAsyncWriterPtr>& writers);

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

