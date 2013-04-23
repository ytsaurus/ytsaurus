#include "erasure_reader.h"
#include "async_writer.h"
#include "async_reader.h"
#include "chunk_meta_extensions.h"
#include "dispatcher.h"

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/actions/parallel_collector.h>

#include <ytlib/erasure/codec.h>
#include <ytlib/erasure/helpers.h>

#include <numeric>

namespace NYT {
namespace NChunkClient {

using namespace NErasure;

///////////////////////////////////////////////////////////////////////////////
// Helpers

#define RETURN_IF_ERROR(valueOrError) \
    if (!valueOrError.IsOK()) { \
        return TError(valueOrError); \
    }

#define RETURN_PROMISE_IF_ERROR(valueOrError, type) \
    if (!valueOrError.IsOK()) { \
        return MakePromise< type >(TError(valueOrError)); \
    }

///////////////////////////////////////////////////////////////////////////////

namespace {

IAsyncReader::TAsyncGetMetaResult AsyncGetPlacementMeta(IAsyncReaderPtr reader)
{
    std::vector<int> tags;
    tags.push_back(TProtoExtensionTag<NProto::TErasurePlacementExt>::Value);
    return reader->AsyncGetChunkMeta(Null, &tags);
}

} // anonymous namespace

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// Non-reparing reader

class TNonReparingReaderSession
    : public TRefCounted
{
public:
    TNonReparingReaderSession(
        const std::vector<IAsyncReaderPtr>& readers,
        const std::vector<NProto::TPartInfo>& partInfos,
        const std::vector<int>& blockIndexes)
            : Readers_(readers)
            , PartInfos_(partInfos)
            , BlockIndexes_(blockIndexes)
            , Result_(BlockIndexes_.size())
            , ResultPromise_(NewPromise<IAsyncReader::TReadResult>())
    { }


    IAsyncReader::TAsyncReadResult Run()
    {
        // For each reader we find blocks to read and their initial indices
        std::vector<
            std::pair<
                std::vector<int>, // indices of blocks in the part
                TBlockIndexList   // indices of blocks in the requested blockIndexes
            > > BlockLocations_(Readers_.size());

        // Fill BlockLocations_ using information about blocks in parts
        int initialPosition = 0;
        FOREACH (int blockIndex, BlockIndexes_) {
            YCHECK(blockIndex >= 0);

            // Searching for the part of given block
            auto it = upper_bound(PartInfos_.begin(), PartInfos_.end(), blockIndex, PartComparator());
            YASSERT(it != PartInfos_.begin());
            do {
                --it;
            } while (it != PartInfos_.begin() && (it->start() > blockIndex || it->block_sizes().size() == 0));

            YCHECK(it != PartInfos_.end());
            int readerIndex = it - PartInfos_.begin();

            YCHECK(blockIndex >= it->start());
            int blockInPartIndex = blockIndex - it->start();

            YCHECK(blockInPartIndex < it->block_sizes().size());
            BlockLocations_[readerIndex].first.push_back(blockInPartIndex);
            BlockLocations_[readerIndex].second.push_back(initialPosition++);
        }

        auto this_ = MakeStrong(this);
        auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetReaderInvoker());
        for (int readerIndex = 0; readerIndex < Readers_.size(); ++readerIndex) {
            auto reader = Readers_[readerIndex];
            awaiter->Await(
                reader->AsyncReadBlocks(BlockLocations_[readerIndex].first),
                BIND(
                    &TNonReparingReaderSession::OnBlocksRead,
                    this_,
                    BlockLocations_[readerIndex].second));
        }

        awaiter->Complete(BIND(&TThis::OnComplete, this_));

        return ResultPromise_;
    }

    void OnBlocksRead(const TBlockIndexList& indicesInPart, IAsyncReader::TReadResult readResult) {
        if (readResult.IsOK()) {
            auto dataRefs = readResult.Value();
            for (int i = 0; i < dataRefs.size(); ++i) {
                Result_[indicesInPart[i]] = dataRefs[i];
            }
        } else {
            TGuard<TSpinLock> guard(AddReadErrorLock_);
            ReadErrors_.push_back(readResult);
        }
    }

    void OnComplete() {
        if (ReadErrors_.empty()) {
            ResultPromise_.Set(Result_);
        } else {
            auto error = TError("Failed read erasure chunk");
            error.InnerErrors() = ReadErrors_;
            ResultPromise_.Set(error);
        }
    }

private:
    typedef TNonReparingReaderSession TThis;

    struct PartComparator {
        bool operator()(int position, const NProto::TPartInfo& info) {
            return position < info.start();
        }
    };

    std::vector<IAsyncReaderPtr> Readers_;
    std::vector<NProto::TPartInfo> PartInfos_;

    std::vector<int> BlockIndexes_;

    std::vector<TSharedRef> Result_;
    IAsyncReader::TAsyncReadPromise ResultPromise_;

    TSpinLock AddReadErrorLock_;
    std::vector<TError> ReadErrors_;
};

///////////////////////////////////////////////////////////////////////////////

class TNonReparingReader
    : public IAsyncReader
{
public:
    explicit TNonReparingReader(const std::vector<IAsyncReaderPtr>& readers)
        : Readers_(readers)
    {
        YCHECK(!Readers_.empty());
    }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override
    {
        auto this_ = MakeStrong(this);
        return PreparePartInfos().Apply(
            BIND([this, this_, blockIndexes] (TError error) -> TAsyncReadResult {
                RETURN_PROMISE_IF_ERROR(error, TReadResult);
                return New<TNonReparingReaderSession>(Readers_, PartInfos_, blockIndexes)->Run();
            }));
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = nullptr) override
    {
        // TODO(ignat): check that no storage-layer extensions are being requested
        YCHECK(!partitionTag);
        return Readers_.front()->AsyncGetChunkMeta(partitionTag, tags);
    }

    virtual TChunkId GetChunkId() const override
    {
        return Readers_.front()->GetChunkId();
    }

private:
    std::vector<IAsyncReaderPtr> Readers_;
    std::vector<NProto::TPartInfo> PartInfos_;

    TAsyncError PreparePartInfos();
};

TAsyncError TNonReparingReader::PreparePartInfos()
{
    if (!PartInfos_.empty()) {
        return MakePromise(TError());
    }

    auto this_ = MakeStrong(this);
    return AsyncGetPlacementMeta(this).Apply(
        BIND([this, this_] (IAsyncReader::TGetMetaResult metaOrError) -> TError {
            RETURN_IF_ERROR(metaOrError);

            auto extension = GetProtoExtension<NProto::TErasurePlacementExt>(metaOrError.Value().extensions());
            PartInfos_ = std::vector<NProto::TPartInfo>(extension.part_infos().begin(), extension.part_infos().end());

            // Check that part infos are correct.
            YCHECK(PartInfos_.front().start() == 0);
            for (int i = 0; i + 1 < PartInfos_.size(); ++i) {
                YCHECK(PartInfos_[i].start() + PartInfos_[i].block_sizes().size() == PartInfos_[i + 1].start());
            }

            return TError();
        })
    );
}

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// Repairing readers

//! Asynchronously reads data by window of size windowSize.
//! It is guaranteed that each original block will be read only once.
class TWindowReader
    : public TRefCounted
{
public:
    typedef TValueOrError<TSharedRef> TReadResult;
    typedef TPromise<TReadResult> TReadPromise;
    typedef TFuture<TReadResult> TReadFuture;

    TWindowReader(IAsyncReaderPtr reader, int blockCount)
        : Reader_(reader)
        , BlockCount_(blockCount)
        , BlockIndex_(0)
        , BlocksDataSize_(0)
        , BuildDataSize_(0)
        , FirstBlockOffset_(0)
    { }

    TReadFuture Read(i64 windowSize)
    {
        if (BlockIndex_ < BlockCount_ &&  BlocksDataSize_ < BuildDataSize_ + windowSize) {
            // Reader one more block if it is necessary
            auto blocksToRead = std::vector<int>(1, BlockIndex_);
            auto this_ = MakeStrong(this);
            return Reader_->AsyncReadBlocks(blocksToRead).Apply(
                BIND(&TWindowReader::OnBlockRead, this_, windowSize));
        } else {
            // We have enough block to build the window
            return MakePromise(TReadResult(BuildWindow(windowSize)));
        }
    }

private:
    TReadFuture OnBlockRead(i64 windowSize, IAsyncReader::TReadResult readResult)
    {
        RETURN_PROMISE_IF_ERROR(readResult, TReadResult);

        YCHECK(readResult.Value().size() == 1);
        auto block = readResult.Value().front();

        BlockIndex_ += 1;
        Blocks_.push_back(block);
        BlocksDataSize_ += block.Size();
        return Read(windowSize);
    }

    TSharedRef BuildWindow(i64 windowSize)
    {
        // Allocate result with zeroes
        auto result = TSharedRef::Allocate(windowSize);
        std::fill(result.Begin(), result.End(), 0);

        i64 resultPosition = 0;
        while (!Blocks_.empty()) {
            auto block = Blocks_.front();

            // Begin and end inside of current block

            i64 beginIndex = FirstBlockOffset_;
            i64 endIndex = std::min(beginIndex + windowSize - resultPosition, (i64)block.Size());
            i64 size = endIndex - beginIndex;

            std::copy(block.Begin() + beginIndex, block.Begin() + endIndex, result.Begin() + resultPosition);
            resultPosition += size;

            FirstBlockOffset_ += size;
            if (endIndex == block.Size()) {
                Blocks_.pop_front();
                FirstBlockOffset_ = 0;
            } else {
                break;
            }
        }
        BuildDataSize_ += windowSize;

        return result;
    }

    std::deque<TSharedRef> Blocks_;

    IAsyncReaderPtr Reader_;

    int BlockCount_;

    // Current number of read blocks.
    int BlockIndex_;

    // Total blocks data size.
    i64 BlocksDataSize_;

    // Total size of data that returned by Read()
    i64 BuildDataSize_;

    // Offset of used data in first block.
    i64 FirstBlockOffset_;
};

typedef TIntrusivePtr<TWindowReader> TWindowReaderPtr;

//! Does the job reverse to that of TWindowReader.
//! Consumes windows and returns blocks of the current part that
//! can be reconstructed.
class TRepairPartReader
{
public:
    explicit TRepairPartReader(const std::vector<i64>& blockSizes)
        : BlockNumber_(0)
        , BlockSizes_(blockSizes)
    {
        PrepareNextBlock();
    }

    std::vector<TSharedRef> Add(TSharedRef window)
    {
        std::vector<TSharedRef> result;

        i64 offset = 0;
        while (offset < window.Size() && BlockNumber_ < BlockSizes_.size()) {
            i64 size = std::min(window.Size() - offset, CurrentBlock_.Size() - CompletedOffset_);
            std::copy(
                window.Begin() + offset,
                window.Begin() + offset + size,
                CurrentBlock_.Begin() + CompletedOffset_);

            offset += size;
            CompletedOffset_ += size;
            if (CompletedOffset_ == CurrentBlock_.Size()) {
                result.push_back(CurrentBlock_);
                BlockNumber_ += 1;
                if (BlockNumber_ < BlockSizes_.size()) {
                    PrepareNextBlock();
                }
            }
        }

        return result;
    }

private:
    void PrepareNextBlock()
    {
        CompletedOffset_ = 0;
        CurrentBlock_ = TSharedRef::Allocate(BlockSizes_[BlockNumber_]);
    }

    int BlockNumber_;
    std::vector<i64> BlockSizes_;

    TSharedRef CurrentBlock_;
    i64 CompletedOffset_;

};

///////////////////////////////////////////////////////////////////////////////

// This reader asynchronously repair blocks of given parts.
// It designed to minimize memory consumption.
//
// We store repaired blocks queue. When RepairNextBlock() called, first
// we check the queue, if it isn't empty then we extract block. Otherwise
// we read window from each part, repair windows of erased parts and add it
// to blocks and add it to RepairPartReaders. All blocks that can be
// reconstructed we add to queue.
class TRepairReader
    : public TRefCounted
{
public:
    struct TBlock
    {
        TBlock()
        { }

        TBlock(TSharedRef data, int index)
            : Data(data), Index(index)
        { }

        TSharedRef Data;
        int Index;
    };

    typedef TValueOrError<TBlock> TReadResult;
    typedef TPromise< TValueOrError<TBlock> > TReadPromise;
    typedef TFuture< TValueOrError<TBlock> > TReadFuture;

    TRepairReader(
        NErasure::ICodec* codec,
        const std::vector<IAsyncReaderPtr>& readers,
        const TBlockIndexList& erasedIndices,
        const TBlockIndexList& repairIndices,
        IInvokerPtr controlInvoker)
            : Codec_(codec)
            , Readers_(readers)
            , ErasedIndices_(erasedIndices)
            , RepairIndices_(repairIndices)
            , Prepared_(false)
            , Finished_(false)
            , WindowIndex_(0)
            , ErasedDataSize_(0)
            , ControlInvoker_(controlInvoker)
    {
        YASSERT(Codec_->GetRepairIndices(ErasedIndices_));
        YASSERT(Codec_->GetRepairIndices(ErasedIndices_)->size() == Readers_.size());
        YASSERT(ControlInvoker_);
    }

    bool HasNextBlock() const
    {
        return !RepairedBlocksQueue_.empty() || !Finished_;
    }

    TReadFuture RepairNextBlock();

    i64 GetErasedDataSize() const;

private:
    NErasure::ICodec* Codec_;
    std::vector<IAsyncReaderPtr> Readers_;

    TBlockIndexList ErasedIndices_;
    TBlockIndexList RepairIndices_;

    std::vector<TWindowReaderPtr> WindowReaders_;
    std::vector<TRepairPartReader> RepairBlockReaders_;

    std::deque<TBlock> RepairedBlocksQueue_;

    bool Prepared_;
    bool Finished_;

    int WindowIndex_;
    int WindowCount_;
    i64 WindowSize_;
    i64 LastWindowSize_;

    i64 ErasedDataSize_;
    
    IInvokerPtr ControlInvoker_;

    TAsyncError PrepareReaders();
    TAsyncError RepairIfNeeded();
    TAsyncError OnReadersPrepared(TError error);
    TAsyncError OnBlockCollected(TValueOrError<std::vector<TSharedRef>> result);
    TAsyncError Repair(const std::vector<TSharedRef>& aliveWindows);
    TError OnGetMeta(IAsyncReader::TGetMetaResult metaOrError);
};

typedef TIntrusivePtr<TRepairReader> TRepairReaderPtr;

///////////////////////////////////////////////////////////////////////////////

TRepairReader::TReadFuture TRepairReader::RepairNextBlock()
{
    auto this_ = MakeStrong(this);
    return RepairIfNeeded()
        .Apply(BIND([this, this_] (TError error) -> TReadFuture {
            RETURN_PROMISE_IF_ERROR(error, TReadResult);

            YCHECK(!RepairedBlocksQueue_.empty());
            auto result = TRepairReader::TReadResult(RepairedBlocksQueue_.front());
            RepairedBlocksQueue_.pop_front();
            return MakePromise(result);
        }).AsyncVia(ControlInvoker_));
}

TAsyncError TRepairReader::Repair(const std::vector<TSharedRef>& aliveWindows)
{
    auto repairedWindows = Codec_->Decode(aliveWindows, ErasedIndices_);
    YCHECK(repairedWindows.size() == ErasedIndices_.size());
    for (int i = 0; i < repairedWindows.size(); ++i) {
        auto repairedWindow = repairedWindows[i];
        FOREACH (auto block, RepairBlockReaders_[i].Add(repairedWindow)) {
            RepairedBlocksQueue_.push_back(TBlock(block, ErasedIndices_[i]));
        }
    }

    if (RepairedBlocksQueue_.empty()) {
        return RepairIfNeeded();
    } else {
        return MakePromise(TError());
    }
}

TAsyncError TRepairReader::OnBlockCollected(TValueOrError<std::vector<TSharedRef>> result)
{
    RETURN_PROMISE_IF_ERROR(result, TError);

    return BIND(&TRepairReader::Repair, MakeStrong(this), result.Value())
        .AsyncVia(TDispatcher::Get()->GetErasureInvoker()).Run();
}

TAsyncError TRepairReader::RepairIfNeeded()
{
    if (!HasNextBlock()) {
        return MakePromise(TError("No blocks to make repair"));
    } else if (RepairedBlocksQueue_.empty()) {
        return PrepareReaders().Apply(
            BIND(&TRepairReader::OnReadersPrepared, MakeStrong(this))
                .AsyncVia(ControlInvoker_));
    } else {
        return MakeFuture(TError());
    }
}

TAsyncError TRepairReader::OnReadersPrepared(TError error)
{
    RETURN_PROMISE_IF_ERROR(error, TError);

    WindowIndex_ += 1;
    if (WindowIndex_ == WindowCount_) {
        Finished_ = true;
    }

    i64 windowSize = (WindowIndex_ == WindowCount_) ? LastWindowSize_ : WindowSize_;

    auto collector = New<TParallelCollector<TSharedRef>>();
    FOREACH (auto windowReader, WindowReaders_) {
        collector->Collect(windowReader->Read(windowSize));
    }

    return collector->Complete().Apply(
            BIND(&TRepairReader::OnBlockCollected, MakeStrong(this))
                .AsyncVia(ControlInvoker_));
}

TError TRepairReader::OnGetMeta(IAsyncReader::TGetMetaResult metaOrError)
{
    RETURN_IF_ERROR(metaOrError);
    auto extension = GetProtoExtension<NProto::TErasurePlacementExt>(
        metaOrError.Value().extensions());

    WindowCount_ = extension.parity_block_count();
    WindowSize_ = extension.parity_block_size();
    LastWindowSize_ = extension.parity_last_block_size();
    auto recoveryIndices = Codec_->GetRepairIndices(ErasedIndices_);
    YCHECK(recoveryIndices);
    YCHECK(recoveryIndices->size() == Readers_.size());
    for (int i = 0; i < Readers_.size(); ++i) {
        int recoveryIndex = (*recoveryIndices)[i];
        int blockCount =
            recoveryIndex < Codec_->GetDataBlockCount()
            ? extension.part_infos().Get((*recoveryIndices)[i]).block_sizes().size()
            : extension.parity_block_count();

        WindowReaders_.push_back(New<TWindowReader>(Readers_[i], blockCount));
    }

    FOREACH (int erasedIndex, ErasedIndices_) {
        std::vector<i64> blockSizes;
        if (erasedIndex < Codec_->GetDataBlockCount()) {
            blockSizes = std::vector<i64>(
                extension.part_infos().Get(erasedIndex).block_sizes().begin(),
                extension.part_infos().Get(erasedIndex).block_sizes().end());
        } else {
            blockSizes = std::vector<i64>(
                extension.parity_block_count(),
                extension.parity_block_size());
            blockSizes.back() = extension.parity_last_block_size();
        }
        ErasedDataSize_ += std::accumulate(blockSizes.begin(), blockSizes.end(), 0);
        RepairBlockReaders_.push_back(TRepairPartReader(blockSizes));
    }
    Prepared_ = true;
    return TError();
}

TAsyncError TRepairReader::PrepareReaders()
{
    YCHECK(!Readers_.empty());

    if (Prepared_) {
        return MakeFuture(TError());
    }

    auto reader = Readers_.front();
    return AsyncGetPlacementMeta(reader).Apply(
        BIND(&TRepairReader::OnGetMeta, MakeStrong(this))
            .AsyncVia(ControlInvoker_));
}

i64 TRepairReader::GetErasedDataSize() const
{
    YCHECK(Prepared_);
    return ErasedDataSize_;
}

///////////////////////////////////////////////////////////////////////////////
// Repair reader of one part

// Repairs blocks of one part.
class TSinglePartRepairSession
    : public TRefCounted
{
public:
    TSinglePartRepairSession(
        NErasure::ICodec* codec,
        const std::vector<IAsyncReaderPtr>& readers,
        const TBlockIndexList& erasedIndices,
        int partIndex,
        const std::vector<int>& blockIndexes)
            : BlockIndexes_(blockIndexes)
            , Reader_(
                New<TRepairReader>(
                    codec,
                    readers,
                    erasedIndices,
                    MakeSingleton(partIndex),
                    TDispatcher::Get()->GetReaderInvoker()))
    { }

    IAsyncReader::TAsyncReadResult Run() {
        return ReadBlock(0, 0);
    }

private:
    std::vector<int> BlockIndexes_;
    TRepairReaderPtr Reader_;
    std::vector<TSharedRef> Result_;

    IAsyncReader::TAsyncReadResult ReadBlock(int pos, int blockIndex)
    {
        if (pos == BlockIndexes_.size()) {
            return MakePromise(IAsyncReader::TReadResult(Result_));
        }

        if (!Reader_->HasNextBlock()) {
            return MakePromise<IAsyncReader::TReadResult>(TError("Block index out of range"));
        }

        auto this_ = MakeStrong(this);
        return Reader_->RepairNextBlock().Apply(
            BIND([this, this_, pos, blockIndex] (TValueOrError<TRepairReader::TBlock> blockOrError) mutable -> IAsyncReader::TAsyncReadResult {
                RETURN_PROMISE_IF_ERROR(blockOrError, IAsyncReader::TReadResult);

                if (BlockIndexes_[pos] == blockIndex) {
                    Result_.push_back(blockOrError.Value().Data);
                    pos += 1;
                }

                return ReadBlock(pos, blockIndex + 1);
            })
        );
    }

};

class TSinglePartRepairReader
    : public IAsyncReader
{
public:
    TSinglePartRepairReader(
        NErasure::ICodec* codec,
        const TBlockIndexList& erasedIndices,
        int partIndex,
        const std::vector<IAsyncReaderPtr>& readers)
            : Codec_(codec)
            , ErasedIndices_(erasedIndices)
            , PartIndex_(partIndex)
            , Readers_(readers)
    { }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes)
    {
        auto session = New<TSinglePartRepairSession>(Codec_, Readers_, ErasedIndices_, PartIndex_, blockIndexes);
        return BIND(&TSinglePartRepairSession::Run, session)
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* tags)
    {
        // TODO(ignat): check that no storage-layer extensions are being requested
        YCHECK(!partitionTag);
        return Readers_.front()->AsyncGetChunkMeta(partitionTag, tags);
    }

    virtual TChunkId GetChunkId() const
    {
        return Readers_.front()->GetChunkId();
    }

private:
    NErasure::ICodec* Codec_;

    TBlockIndexList ErasedIndices_;
    int PartIndex_;

    std::vector<IAsyncReaderPtr> Readers_;

};

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// Repair reader of all parts

class TRepairAllPartsSession
    : public TRefCounted
{
public:
    TRepairAllPartsSession(
        NErasure::ICodec* codec,
        const TBlockIndexList& erasedIndices,
        const std::vector<IAsyncReaderPtr>& readers,
        const std::vector<IAsyncWriterPtr>& writers,
        TCallback<void(double)> onProgress,
        IInvokerPtr controlInvoker)
            : Reader_(
                New<TRepairReader>(
                    codec,
                    readers,
                    erasedIndices,
                    erasedIndices,
                    controlInvoker))
            , Readers_(readers)
            , Writers_(writers)
            , OnProgress_(onProgress)
            , RepairedDataSize_(0)
            , ControlInvoker_(controlInvoker)
    {
        YCHECK(erasedIndices.size() == writers.size());
        FOREACH (auto writer, writers) {
            writer->Open();
        }
        for (int i = 0; i < erasedIndices.size(); ++i) {
            IndexToWriter_[erasedIndices[i]] = writers[i];
        }
    }

    TAsyncError Run()
    {
        if (!Reader_->HasNextBlock()) {
            return Finalize();
        }
        return Reader_->RepairNextBlock().Apply(
            BIND(&TRepairAllPartsSession::OnBlockRepaired, MakeStrong(this))
                .AsyncVia(ControlInvoker_));
    }

private:
    TAsyncError OnBlockRepaired(TValueOrError<TRepairReader::TBlock> blockOrError)
    {
        RETURN_PROMISE_IF_ERROR(blockOrError, TError);

        const auto& block = blockOrError.Value();
        RepairedDataSize_ += block.Data.Size();
        if (!OnProgress_.IsNull()) {
            OnProgress_.Run(static_cast<double>(RepairedDataSize_) / Reader_->GetErasedDataSize());
        }

        YCHECK(IndexToWriter_.find(block.Index) != IndexToWriter_.end());
        auto writer = IndexToWriter_[block.Index];
        bool result = writer->WriteBlock(block.Data);
        if (result) {
            return Run();
        }

        auto this_ = MakeStrong(this);
        return writer->GetReadyEvent().Apply(
            BIND([this, this_] (TError error) -> TAsyncError {
                RETURN_PROMISE_IF_ERROR(error, TError);
                return Run();
            }).AsyncVia(ControlInvoker_)
        );
    }

    TAsyncError Finalize()
    {
        auto this_ = MakeStrong(this);
        return Readers_.front()->AsyncGetChunkMeta().Apply(
            BIND(&TRepairAllPartsSession::OnGotChunkMeta, MakeStrong(this))
                .AsyncVia(ControlInvoker_));
    }

    TAsyncError OnGotChunkMeta(IAsyncReader::TGetMetaResult metaOrError)
    {
        RETURN_PROMISE_IF_ERROR(metaOrError, TError);
        auto collector = New<TParallelCollector<void>>();
        FOREACH (auto writer, Writers_) {
            collector->Collect(writer->AsyncClose(metaOrError.Value()));
        }
        return collector->Complete();
    }

    TRepairReaderPtr Reader_;
    std::vector<IAsyncReaderPtr> Readers_;
    std::vector<IAsyncWriterPtr> Writers_;
    std::map<int, IAsyncWriterPtr> IndexToWriter_;

    TCallback<void(double)> OnProgress_;
    i64 RepairedDataSize_;

    IInvokerPtr ControlInvoker_;
};

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateNonReparingErasureReader(
    const std::vector<IAsyncReaderPtr>& dataBlockReaders)
{
    return New<TNonReparingReader>(dataBlockReaders);
}

IAsyncReaderPtr CreateReparingErasureReader(
    NErasure::ICodec* codec,
    const TBlockIndexList& erasedIndices,
    int partIndex,
    const std::vector<IAsyncReaderPtr>& readers)
{
    return New<TSinglePartRepairReader>(codec, erasedIndices, partIndex, readers);
}

TAsyncError RepairErasedBlocks(
    NErasure::ICodec* codec,
    const TBlockIndexList& erasedIndices,
    const std::vector<IAsyncReaderPtr>& readers,
    const std::vector<IAsyncWriterPtr>& writers,
    TCancelableContextPtr cancelableContext,
    TCallback<void(double)> onProgress)
{
    auto invoker = TDispatcher::Get()->GetReaderInvoker();
    if (cancelableContext) {
        invoker = cancelableContext->CreateInvoker(invoker);
    }
    auto repair = New<TRepairAllPartsSession>(codec, erasedIndices, readers, writers, onProgress, invoker);
    return BIND(&TRepairAllPartsSession::Run, repair)
        .AsyncVia(invoker)
        .Run();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

