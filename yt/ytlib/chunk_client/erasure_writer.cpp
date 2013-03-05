#include "public.h"
#include "config.h"
#include "dispatcher.h"
#include "async_writer.h"
#include "chunk_meta_extensions.h"

#include <ytlib/erasure_codecs/codec.h>
#include <ytlib/actions/async_pipeline.h>
#include <ytlib/actions/parallel_awaiter.h>

namespace NYT {
namespace NChunkClient {

class TErasureWriter
    : public IAsyncWriter
{
public:
    TErasureWriter(
        const NErasure::ICodec* codec,
        const std::vector<IAsyncWriterPtr>& writers)
            : Codec_(codec)
            , Writers_(writers)
            , WrittenBlockCount(0)
    {
        YCHECK(writers.size() == codec->GetTotalBlockCount());

        //VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetWriterInvoker(), WriterThread);
        //VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetErasureInvoker(), ErasureThread);
    }

    virtual void Open() override
    {
        FOREACH(auto writer, Writers_) {
            writer->Open();
        }
    }

    virtual bool TryWriteBlock(const TSharedRef& block) override
    {
        Blocks_.push_back(block);
        return true;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        auto error = TAsyncErrorPromise();
        error.Set(TError());
        return error;
    }

    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override
    {
        YUNREACHABLE();
    }

    virtual const std::vector<int> GetWrittenIndexes() const override
    {
        YUNREACHABLE();
    }

    virtual TAsyncError AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta) override;

private:
    const NErasure::ICodec* Codec_;

    std::vector<IAsyncWriterPtr> Writers_;
    std::vector<TSharedRef> Blocks_;

    void OnBlocksWritten(int numberOfBlocks, TValueOrError<void> error);

    TAsyncError WriteWindow(int windowIndex);

    TAsyncError CloseParityWriters(const NChunkClient::NProto::TChunkMeta& chunkMeta);

    // TODO: move to config
    static const i64 ErasureWindowSize = 1024 * 1024;

    // Number of blocks (data and parity) that already written
    int WrittenBlockCount;
    TAsyncErrorPromise Result_;

    // Promises for generated window of parity blocks
    std::vector<TPromise<void>> WindowEncodedPromise_;

    // Parity blocks, group by pieces of size ErasureWindowSize
    std::vector<std::vector<TSharedRef>> ParityBlocks_;

    // Error of writing parity blocks
    std::vector<TError> WriteErrors_;

    // Error of closing parity blocks
    std::vector<TError> CloseErrors_;

    // Promises to write window of parity blocks
    std::vector<TAsyncErrorPromise> WritePromises_;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);
    DECLARE_THREAD_AFFINITY_SLOT(ErasureThread);
};

///////////////////////////////////////////////////////////////////////////////

namespace {

// Split blocks to the continuous groups of approximately equal sizes.
std::vector<std::vector<TSharedRef>> SplitBlocks(
    const std::vector<TSharedRef>& blocks,
    int groupCount)
{
    i64 totalSize = 0;
    FOREACH (const auto& block, blocks) {
        totalSize += block.Size();
    }

    std::vector<std::vector<TSharedRef>> groups(1);
    i64 currentSize = 0;
    FOREACH (const auto& block, blocks) {
        groups.back().push_back(block);
        currentSize += block.Size();
        // Current group is fullfilled if currentSize / currentGroupCount >= totalSize / groupCount
        while (currentSize * groupCount >= totalSize * groups.size() &&
               groups.size() < groupCount)
        {
            groups.push_back(std::vector<TSharedRef>());
        }
    }

    YCHECK(groups.size() == groupCount);

    return groups;
}

i64 RoundUp(i64 num, i64 mod) {
    if (num % mod == 0) {
        return num;
    }
    return num + mod - (num % mod);
}

class TDataBlock
{
public:
    explicit TDataBlock(const std::vector<TSharedRef>& blocks)
        : Blocks_(blocks)
    { }

    TSharedRef GetSlice(i64 start, i64 end) const
    {
        YCHECK(start >= 0);
        YCHECK(start <= end);

        i64 pos = 0;
        auto result = TSharedRef::Allocate(end - start);

        i64 currentStart = 0;

        FOREACH (const auto& block, Blocks_) {
            i64 innerStart = std::max((i64)0, start - currentStart);
            i64 innerEnd = std::min((i64)block.Size(), end - currentStart);

            if (innerStart < innerEnd) {
                std::copy(
                    block.Begin() + innerStart,
                    block.Begin() + innerEnd,
                    result.Begin() + pos);
                pos += innerEnd - innerStart;
            }
            currentStart += block.Size();

            if (pos == result.Size() || currentStart >= end) {
                break;
            }
        }

        return result;
    }

private:
    std::vector<TSharedRef> Blocks_;
};

} // anonymous namespace

///////////////////////////////////////////////////////////////////////////////


TAsyncError TErasureWriter::AsyncClose(const NChunkClient::NProto::TChunkMeta& chunkMeta) override
{
    // Prepare result promise
    Result_ = NewPromise<TError>();

    // Split data blocks to proper number of groups
    auto groups = SplitBlocks(Blocks_, Codec_->GetDataBlockCount());

    // Add groups to meta
    int start = 0;
    NProto::TErasurePlacementExt placementExtension;
    FOREACH (const auto& group, groups) {
        NProto::TBlocksRange range;
        range.set_start(start);
        range.set_count(group.size());
        start += group.size();

        *placementExtension.add_ranges() = range;
    }
    
    NChunkClient::NProto::TChunkMeta updatedMeta(chunkMeta);
    SetProtoExtension(updatedMeta.mutable_extensions(), placementExtension);

    // Write data blocks
    for (int i = 0; i < groups.size(); ++i) {
        const auto& group = groups[i];
        const auto& writer = Writers_[i];
        auto pipeline = StartAsyncPipeline(TDispatcher::Get()->GetWriterInvoker());
        FOREACH(const auto& block, group) {
            pipeline = pipeline->Add(
                BIND([=]() {
                    writer->TryWriteBlock(block);
                    return writer->GetReadyEvent();
                })
            );
        }
        pipeline = pipeline->Add(
            BIND([=](){
                return writer->AsyncClose(updatedMeta);
            })
        );
        pipeline->Run().Subscribe(BIND(&TErasureWriter::OnBlocksWritten, MakeStrong(this), 1));
    }

    // Calculate size of parity blocks and form DataBlock to efficiently extract slices
    // of data blocks
    std::vector<TDataBlock> dataBlocks;
    i64 maxSize = 0;
    FOREACH (const auto& group, groups) {
        i64 size = 0;
        FOREACH(const auto& block, group) {
            size += block.Size();
        }
        maxSize = std::max(maxSize, size);

        dataBlocks.push_back(TDataBlock(group));
    }
    maxSize = RoundUp(maxSize, Codec_->GetWordSize());

    // Generate and write parity blocks.
    auto windowCount = RoundUp(maxSize, ErasureWindowSize) / ErasureWindowSize;
    ParityBlocks_.resize(windowCount);
    WindowEncodedPromise_.resize(windowCount);
    WritePromises_.resize(windowCount);

    auto pipeline = StartAsyncPipeline(TDispatcher::Get()->GetWriterInvoker());
    int windowIndex = 0;
    for (i64 begin = 0; begin < maxSize; begin += ErasureWindowSize) {
        WindowEncodedPromise_[windowIndex] = NewPromise<void>();

        i64 end = std::min(begin + ErasureWindowSize, maxSize);

        // generate bytes from [begin, end) for parity blocks
        std::vector<TSharedRef> slices;
        FOREACH (const auto& block, dataBlocks) {
            slices.push_back(block.GetSlice(begin, end));
        }

        TDispatcher::Get()->GetErasureInvoker()->Invoke(
            BIND([=](){
                ParityBlocks_[windowIndex] = Codec_->Encode(slices);
                WindowEncodedPromise_[windowIndex].Set();
            })
        );

        pipeline = pipeline->Add(BIND(&TErasureWriter::WriteWindow, MakeStrong(this), windowIndex));

        windowIndex += 1;
    }
    pipeline->Add(BIND(&TErasureWriter::CloseParityWriters, MakeStrong(this), updatedMeta));
    pipeline->Run().Subscribe(BIND(&TErasureWriter::OnBlocksWritten, MakeStrong(this), Codec_->GetParityBlockCount()));

    return Result_;
}

void TErasureWriter::OnBlocksWritten(int numberOfBlocks, TValueOrError<void> error)
{
    //VERIFY_THREAD_AFFINITY(WriterThread);

    // !!! We should add inner errors instead of returning first one
    if (Result_.IsSet()) {
        return;
    }

    if (error.IsOK()) {
        WrittenBlockCount += numberOfBlocks;
        if (WrittenBlockCount == Codec_->GetTotalBlockCount()) {
            // All blocks are successfully written
            Result_.Set(TError());
        }
    }
    else {
        Result_.Set(error);
    }
}

TAsyncError TErasureWriter::WriteWindow(int windowIndex)
{
    //VERIFY_THREAD_AFFINITY(WriterThread);

    // Wait encoding to complete
    WindowEncodedPromise_[windowIndex].Get();

    // Get parity blocks of current window
    const std::vector<TSharedRef>& parityBlocks = ParityBlocks_[windowIndex];

    // Write blocks of current window in parallel manner
    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    for (int i = 0; i < Codec_->GetParityBlockCount(); ++i) {
        auto& writer = Writers_[Codec_->GetDataBlockCount() + i];
        writer->TryWriteBlock(parityBlocks[i]);
        awaiter->Await(
            writer->GetReadyEvent(),
            BIND([=](TError error) {
                // Process write error
                if (!error.IsOK()) {
                    WriteErrors_.push_back(error);
                }
            })
        );
    }

    // Set promise of written window of blocks
    auto& promise = WritePromises_[windowIndex];
    promise = NewPromise<TError>();
    awaiter->Complete(
        BIND([&](){
            if (WriteErrors_.empty()) {
                promise.Set(TError());
            } else {
                // Some block writers failed
                auto error = TError("Write parity blocks failed");
                error.InnerErrors() = WriteErrors_;
                promise.Set(error);
            }
        })
    );

    return promise;
}

TAsyncError TErasureWriter::CloseParityWriters(const NChunkClient::NProto::TChunkMeta& chunkMeta)
{
    // TODO(ignat): remove this copypaste

    auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetWriterInvoker());
    for (int i = 0; i < Codec_->GetParityBlockCount(); ++i) {
        auto& writer = Writers_[Codec_->GetDataBlockCount() + i];
        awaiter->Await(
            writer->AsyncClose(chunkMeta),
            BIND([=](TError error) {
                // Process write error
                if (!error.IsOK()) {
                    CloseErrors_.push_back(error);
                }
            }));
    }

    // Set promise of written window of blocks
    auto promise = NewPromise<TError>();
    awaiter->Complete(
        BIND([&](){
            if (CloseErrors_.empty()) {
                promise.Set(TError());
            } else {
                // Some block writers failed
                auto error = TError("Closing parity writers failed");
                error.InnerErrors() = CloseErrors_;
                promise.Set(error);
            }
        })
    );

    return promise;
}


///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr GetErasureWriter(
    const NErasure::ICodec* codec,
    const std::vector<IAsyncWriterPtr>& writers)
{
    return New<TErasureWriter>(codec, writers);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


