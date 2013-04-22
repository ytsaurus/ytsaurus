#include "public.h"

#include "config.h"
#include "dispatcher.h"
#include "async_writer.h"
#include "chunk_meta_extensions.h"

#include <ytlib/actions/async_pipeline.h>
#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/actions/parallel_collector.h>

#include <ytlib/erasure/codec.h>

namespace NYT {
namespace NChunkClient {
namespace {

///////////////////////////////////////////////////////////////////////////////
// Helpers

// TODO(babenko): remove this when TValueOrError<void> becomes just TError
TFuture<TError> ConvertToTErrorFuture(TFuture<TValueOrError<void>> future)
{
    return future.Apply(BIND([](TValueOrError<void> error) -> TError {
        return error;
    }));
}

// Split blocks into continuous groups of approximately equal sizes.
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
        // Current group is fulfilled if currentSize / currentGroupCount >= totalSize / groupCount
        while (currentSize * groupCount >= totalSize * groups.size() &&
               groups.size() < groupCount)
        {
            groups.push_back(std::vector<TSharedRef>());
        }
    }

    YCHECK(groups.size() == groupCount);

    return groups;
}

i64 RoundUp(i64 num, i64 mod)
{
    if (num % mod == 0) {
        return num;
    }
    return num + mod - (num % mod);
}

class TSlicer
{
public:
    explicit TSlicer(const std::vector<TSharedRef>& blocks)
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

class TErasureWriter
    : public IAsyncWriter
{
public:
    TErasureWriter(
        TErasureWriterConfigPtr config,
        NErasure::ICodec* codec,
        const std::vector<IAsyncWriterPtr>& writers)
            : Config_(config)
            , Codec_(codec)
            , Writers_(writers)
    {
        YCHECK(writers.size() == codec->GetTotalBlockCount());
        VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetWriterInvoker(), WriterThread);
        ChunkInfo_.set_size(0);
    }

    virtual void Open() override
    {
        FOREACH (auto writer, Writers_) {
            writer->Open();
        }
    }

    virtual bool WriteBlock(const TSharedRef& block) override
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
        return ChunkInfo_;
    }

    virtual const std::vector<int> GetWrittenIndexes() const override
    {
        std::vector<int> result;
        result.reserve(Codec_->GetTotalBlockCount());
        for (int i = 0; i < Codec_->GetTotalBlockCount(); ++i) {
            result.push_back(i);
        }
        return result;
    }

    virtual TAsyncError AsyncClose(const NProto::TChunkMeta& chunkMeta) override;

private:
    void PrepareBlocks();

    void PrepareChunkMeta(const NProto::TChunkMeta& chunkMeta);

    TAsyncError WriteDataBlocks();

    TAsyncError EncodeAndWriteParityBlocks();

    TAsyncError WriteParityBlocks(int windowIndex);

    TAsyncError CloseParityWriters();

    TAsyncError OnClosed(TError error);

    TErasureWriterConfigPtr Config_;
    NErasure::ICodec* Codec_;

    std::vector<IAsyncWriterPtr> Writers_;
    std::vector<TSharedRef> Blocks_;

    // Information about blocks, necessary to write blocks
    // and encode parity parts
    std::vector<std::vector<TSharedRef>> Groups_;
    std::vector<TSlicer> Slicers_;
    i64 ParityDataSize_;
    int WindowCount_;
    i64 MemoryConsumption_;

    // Chunk meta with information about block placement
    NChunkClient::NProto::TChunkMeta ChunkMeta_;
    NChunkClient::NProto::TChunkInfo ChunkInfo_;

    // Parity blocks
    std::vector<std::vector<TSharedRef>> ParityBlocks_;

    // Promises to write window of parity blocks
    std::vector<TAsyncError> WriteFutures_;

    // Promises for generated window of parity blocks
    std::vector<TPromise<void>> WindowEncodedPromise_;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

};

///////////////////////////////////////////////////////////////////////////////

void TErasureWriter::PrepareBlocks()
{
    Groups_ = SplitBlocks(Blocks_, Codec_->GetDataBlockCount());

    // Calculate size of parity blocks and form slicers
    Slicers_.clear();
    ParityDataSize_ = 0;
    MemoryConsumption_ = Codec_->GetDataBlockCount() * Config_->ErasureWindowSize;
    FOREACH (const auto& group, Groups_) {
        i64 size = 0;
        i64 maxBlockSize = 0;
        FOREACH (const auto& block, group) {
            size += block.Size();
            maxBlockSize = std::max(maxBlockSize, (i64)block.Size());
        }
        ParityDataSize_ = std::max(ParityDataSize_, size);

        Slicers_.push_back(TSlicer(group));

        MemoryConsumption_ += maxBlockSize;
    }

    // Calculate number of windows
    ParityDataSize_ = RoundUp(ParityDataSize_, Codec_->GetWordSize());

    WindowCount_ = ParityDataSize_ / Config_->ErasureWindowSize;
    if (ParityDataSize_ % Config_->ErasureWindowSize != 0) {
        WindowCount_ += 1;
    }


    ParityBlocks_.resize(WindowCount_);
    WindowEncodedPromise_.resize(WindowCount_);
    WriteFutures_.resize(WindowCount_);
}

void TErasureWriter::PrepareChunkMeta(const NProto::TChunkMeta& chunkMeta)
{
    int start = 0;
    NProto::TErasurePlacementExt placementExtension;
    FOREACH (const auto& group, Groups_) {
        NProto::TPartInfo info;
        info.set_start(start);
        FOREACH (const auto& block, group) {
            info.add_block_sizes(block.Size());
        }
        start += group.size();

        *placementExtension.add_part_infos() = info;
    }
    placementExtension.set_parity_part_count(Codec_->GetParityBlockCount());
    placementExtension.set_parity_block_count(WindowCount_);
    placementExtension.set_parity_block_size(Config_->ErasureWindowSize);
    placementExtension.set_parity_last_block_size(ParityDataSize_ - (Config_->ErasureWindowSize * (WindowCount_ - 1)));

    ChunkMeta_ = chunkMeta;
    SetProtoExtension(ChunkMeta_.mutable_extensions(), placementExtension);

    NProto::TMiscExt miscExtension;
    miscExtension.set_repair_memory_limit(MemoryConsumption_);
    UpdateProtoExtension(ChunkMeta_.mutable_extensions(), miscExtension);
}

TAsyncError TErasureWriter::WriteDataBlocks()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(Groups_.size() <= Writers_.size());

    auto this_ = MakeStrong(this);
    auto parallelCollector = New<TParallelCollector<void>>();
    for (int index = 0; index < Groups_.size(); ++index) {
        const auto& group = Groups_[index];
        const auto& writer = Writers_[index];
        auto pipeline = StartAsyncPipeline(TDispatcher::Get()->GetWriterInvoker());
        FOREACH (const auto& block, group) {
            pipeline = pipeline->Add(BIND([this, this_, block, writer] () -> TAsyncError {
                if (!writer->WriteBlock(block)) {
                    return writer->GetReadyEvent();
                }
                return MakeFuture(TError());
            }));
        }
        pipeline = pipeline->Add(BIND(&IAsyncWriter::AsyncClose, writer, ChunkMeta_));
        parallelCollector->Collect(ConvertToTErrorFuture(pipeline->Run()));
    }
    return parallelCollector->Complete();
}

TAsyncError TErasureWriter::EncodeAndWriteParityBlocks()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto this_ = MakeStrong(this);
    auto pipeline = StartAsyncPipeline(TDispatcher::Get()->GetWriterInvoker());
    int windowIndex = 0;
    for (i64 begin = 0; begin < ParityDataSize_; begin += Config_->ErasureWindowSize) {
        WindowEncodedPromise_[windowIndex] = NewPromise<void>();

        i64 end = std::min(begin + Config_->ErasureWindowSize, ParityDataSize_);

        // Generate bytes from [begin, end) for parity blocks.
        std::vector<TSharedRef> slices;
        FOREACH (const auto& slicer, Slicers_) {
            slices.push_back(slicer.GetSlice(begin, end));
        }

        TDispatcher::Get()->GetErasureInvoker()->Invoke(BIND([this, this_, windowIndex, slices] () {
            ParityBlocks_[windowIndex] = Codec_->Encode(slices);
            WindowEncodedPromise_[windowIndex].Set();
        }));

        pipeline = pipeline->Add(BIND(&TErasureWriter::WriteParityBlocks, this_, windowIndex));

        ++windowIndex;
    }
    pipeline = pipeline->Add(BIND(&TErasureWriter::CloseParityWriters, this_));
    return ConvertToTErrorFuture(pipeline->Run());
}

TAsyncError TErasureWriter::WriteParityBlocks(int windowIndex)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Wait encoding to complete
    WindowEncodedPromise_[windowIndex].Get();

    // Get parity blocks of current window
    const std::vector<TSharedRef>& parityBlocks = ParityBlocks_[windowIndex];

    // Write blocks of current window in parallel manner
    auto collector = New<TParallelCollector<void>>();
    for (int i = 0; i < Codec_->GetParityBlockCount(); ++i) {
        auto& writer = Writers_[Codec_->GetDataBlockCount() + i];
        writer->WriteBlock(parityBlocks[i]);
        collector->Collect(writer->GetReadyEvent());
    }
    auto writeFuture = collector->Complete();
    return WriteFutures_[windowIndex] = writeFuture;
}

TAsyncError TErasureWriter::CloseParityWriters()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto collector = New<TParallelCollector<void>>();
    for (int i = 0; i < Codec_->GetParityBlockCount(); ++i) {
        auto& writer = Writers_[Codec_->GetDataBlockCount() + i];
        collector->Collect(writer->AsyncClose(ChunkMeta_));
    }
    return collector->Complete();
}

TAsyncError TErasureWriter::AsyncClose(const NProto::TChunkMeta& chunkMeta)
{
    PrepareBlocks();
    PrepareChunkMeta(chunkMeta);

    auto this_ = MakeStrong(this);
    auto invoker = TDispatcher::Get()->GetWriterInvoker();
    auto collector = New<TParallelCollector<void>>();
    collector->Collect(
        BIND(&TErasureWriter::WriteDataBlocks, MakeStrong(this))
        .AsyncVia(invoker)
        .Run());
    collector->Collect(
        BIND(&TErasureWriter::EncodeAndWriteParityBlocks, MakeStrong(this))
        .AsyncVia(invoker)
        .Run());
    return collector->Complete().Apply(BIND(&TErasureWriter::OnClosed, MakeStrong(this)));
}


TAsyncError TErasureWriter::OnClosed(TError error)
{
    if (!error.IsOK()) {
        return MakeFuture(error);
    }

    i64 chunkDataSize = 0;
    for (auto writer: Writers_) {
        chunkDataSize += writer->GetChunkInfo().size();
    }
    ChunkInfo_.set_size(chunkDataSize);

    return MakeFuture(TError());
}

///////////////////////////////////////////////////////////////////////////////

IAsyncWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ICodec* codec,
    const std::vector<IAsyncWriterPtr>& writers)
{
    return New<TErasureWriter>(config, codec, writers);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT


