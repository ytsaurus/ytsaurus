#include "erasure_reader.h"

#include "async_writer.h"
#include "async_reader.h"
#include "chunk_meta_extensions.h"
#include "dispatcher.h"

#include <ytlib/actions/parallel_awaiter.h>
#include <ytlib/actions/parallel_collector.h>

namespace NYT {
namespace NChunkClient {
namespace NInternal {

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

IAsyncReader::TAsyncGetMetaResult AsyncGetPlacementMeta(IAsyncReader* reader)
{
    std::vector<int> tags;
    tags.push_back(TProtoExtensionTag<NProto::TErasurePlacementExt>::Value);
    return reader->AsyncGetChunkMeta(Null, &tags);
}

} // anonymous namespace

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// Erasure reader without repair

class TErasureReaderSession
    : public TRefCounted
{
public:
    TErasureReaderSession(
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
                std::vector<int>  // indices of blocks in the requested blockIndexes
            >> BlockLocations_(Readers_.size());

        // Fill BlockLocations_ using information about blocks in parts
        int initialPosition = 0;
        FOREACH(int blockIndex, BlockIndexes_) {
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

        auto awaiter = New<TParallelAwaiter>(TDispatcher::Get()->GetReaderInvoker());
        for (int readerIndex = 0; readerIndex < Readers_.size(); ++readerIndex) {
            auto reader = Readers_[readerIndex];
            awaiter->Await(
                reader->AsyncReadBlocks(BlockLocations_[readerIndex].first),
                BIND(
                    &TErasureReaderSession::OnBlocksRead,
                    MakeStrong(this),
                    BlockLocations_[readerIndex].second));
        }

        awaiter->Complete(BIND(&TThis::OnComplete, MakeStrong(this)));

        return ResultPromise_;
    }

    void OnBlocksRead(const std::vector<int>& indicesInPart, IAsyncReader::TReadResult readResult) {
        if (readResult.IsOK()) {
            auto dataRefs = readResult.Value();
            for (int i = 0; i < dataRefs.size(); ++i) {
                Result_[indicesInPart[i]] = dataRefs[i];
            }
        }
        else {
            TGuard<TSpinLock> guard(AddReadErrorLock_);
            ReadErrors_.push_back(readResult);
        }
    }

    void OnComplete() {
        if (ReadErrors_.empty()) {
            ResultPromise_.Set(Result_);
        }
        else {
            auto error = TError("Failed read erasure chunk");
            error.InnerErrors() = ReadErrors_;
            ResultPromise_.Set(error);
        }
    }

private:
    typedef TErasureReaderSession TThis;

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

class TErasureReader
    : public IAsyncReader
{
public:
    explicit TErasureReader(const std::vector<IAsyncReaderPtr>& readers)
        : Readers_(readers)
    {
        YCHECK(!Readers_.empty());
    }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override
    {
        return PreparePartInfos().Apply(BIND([&](TError error) -> TAsyncReadResult {
            RETURN_PROMISE_IF_ERROR(error, TReadResult);
            return New<TErasureReaderSession>(Readers_, PartInfos_, blockIndexes)->Run();
        }));
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = NULL) override
    {
        // TODO(ignat): add check that requests extensions indepent of the chunk part
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

TAsyncError TErasureReader::PreparePartInfos()
{
    if (!PartInfos_.empty()) {
        return MakePromise(TError());
    }
    return AsyncGetPlacementMeta(this).Apply(
        BIND([&](IAsyncReader::TGetMetaResult metaOrError) {
            RETURN_IF_ERROR(metaOrError);

            auto extension = GetProtoExtension<NProto::TErasurePlacementExt>(metaOrError.Value().extensions());
            PartInfos_ = std::vector<NProto::TPartInfo>(extension.part_infos().begin(), extension.part_infos().end());

            // Check that part infos are correct
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
// Repair readers

// Asyncroniously read data by window of size windowSize.
// It is garuanteed that each original block will be read only once.
class TWindowReader
    : public TRefCounted
{
public:
    typedef TValueOrError<TSharedRef> TReadResult;
    typedef TPromise<TReadResult> TReadPromise;
    typedef TFuture<TReadResult> TReadFuture;

    explicit TWindowReader(IAsyncReaderPtr reader, int blockCount)
        : Reader_(reader)
        , BlockCount_(blockCount)
        , BlockIndex_(0)
        , BlocksDataSize_(0)
        , BuildDataSize_(0)
        , FirstBlockOffset_(0)
    { }

    TReadFuture Read(i64 windowSize) {
        if (BlockIndex_ < BlockCount_ &&  BlocksDataSize_ < BuildDataSize_ + windowSize) {
            // Reader one more block if it is necessary
            auto blocksToRead = std::vector<int>(1, BlockIndex_);
            return Reader_->AsyncReadBlocks(blocksToRead)
                .Apply(BIND(&TWindowReader::OnBlockRead, MakeStrong(this), windowSize));
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
            }
            else {
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

// This class is opposite to TWindowReader.
// It consumes windows and returns blocks of the current part that
// can be reconstructed.
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
// we reader window from each part, repair windows of erased parts and add it
// to blocks and add it to RepairPartReaders. All blocks that can be
// reconstructed we add to queue.
class TRepairReader
    : public TRefCounted
{
public:
    struct TBlock {
        TBlock()
        { }

        TBlock(TSharedRef data, int index)
            : Data(data), Index(index)
        { }

        TSharedRef Data;
        int Index;
    };

    typedef TValueOrError<TBlock> TReadResult;
    typedef TPromise<TValueOrError<TBlock>> TReadPromise;
    typedef TFuture<TValueOrError<TBlock>> TReadFuture;

    TRepairReader(
        const NErasure::ICodec* codec,
        const std::vector<IAsyncReaderPtr>& readers,
        const std::vector<int>& erasedIndices,
        const std::vector<int>& repairIndices)
            : Codec_(codec)
            , Readers_(readers)
            , ErasedIndices_(erasedIndices)
            , RepairIndices_(repairIndices)
            , Prepared_(false)
            , Finished_(false)
            , WindowIndex_(0)
    {
        YASSERT(Codec_->GetRecoveryIndices(ErasedIndices_));
        YASSERT(Codec_->GetRecoveryIndices(ErasedIndices_)->size() == Readers_.size());
    }

    bool HasNextBlock() const
    {
        return !RepairedBlocksQueue_.empty() || !Finished_;
    }

    TReadFuture RepairNextBlock();

private:
    const NErasure::ICodec* Codec_;
    std::vector<IAsyncReaderPtr> Readers_;

    std::vector<int> ErasedIndices_;
    std::vector<int> RepairIndices_;

    std::vector<TWindowReaderPtr> WindowReaders_;
    std::vector<TRepairPartReader> RepairBlockReaders_;

    std::deque<TBlock> RepairedBlocksQueue_;

    bool Prepared_;
    bool Finished_;

    int WindowIndex_;
    int WindowCount_;
    i64 WindowSize_;
    i64 LastWindowSize_;

    TAsyncError PrepareReaders();
    TAsyncError RepairIfNeeded();
    TAsyncError OnBlockCollected(TValueOrError<std::vector<TSharedRef>> result);
    TAsyncError Repair(const std::vector<TSharedRef>& aliveWindows);
    TError OnGetMeta(IAsyncReader::TGetMetaResult metaOrError);
};

typedef TIntrusivePtr<TRepairReader> TRepairReaderPtr;

TRepairReader::TReadFuture TRepairReader::RepairNextBlock()
{
    return RepairIfNeeded().Apply(BIND([&](TError error) -> TReadFuture {
        RETURN_PROMISE_IF_ERROR(error, TReadResult);
        
        YCHECK(!RepairedBlocksQueue_.empty());
        auto result = RepairedBlocksQueue_.front();
        RepairedBlocksQueue_.pop_front();
        return MakePromise<TReadResult>(result);
    }));
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
    }
    else {
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
    }
    else if (RepairedBlocksQueue_.empty()) {
        return PrepareReaders()
            .Apply(BIND([&] (TError error) -> TAsyncError { 
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
                return collector->Complete()
                    .Apply(BIND(&TRepairReader::OnBlockCollected, MakeStrong(this)));
            }));
    }
    else {
        return MakePromise(TError());
    }
}

TError TRepairReader::OnGetMeta(IAsyncReader::TGetMetaResult metaOrError)
{
    RETURN_IF_ERROR(metaOrError);
    auto extension = GetProtoExtension<NProto::TErasurePlacementExt>(
        metaOrError.Value().extensions());

    WindowCount_ = extension.parity_block_count();
    WindowSize_ = extension.parity_block_size();
    LastWindowSize_ = extension.parity_last_block_size();
    auto recoveryIndices = Codec_->GetRecoveryIndices(ErasedIndices_);
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
        RepairBlockReaders_.push_back(TRepairPartReader(blockSizes));
    }
    Prepared_ = true;
    return TError();
}

TAsyncError TRepairReader::PrepareReaders()
{
    YCHECK(!Readers_.empty());
    if (Prepared_) {
        return MakePromise(TError());
    }

    return AsyncGetPlacementMeta(Readers_.front().Get())
        .Apply(BIND(&TRepairReader::OnGetMeta, MakeStrong(this)));
}

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// Repiar reader of one part

// Session of repairing block of one part.
class TRepairOnePartSession
    : public TRefCounted
{
public:
    TRepairOnePartSession(
        const NErasure::ICodec* codec,
        const std::vector<IAsyncReaderPtr>& readers,
        const std::vector<int>& erasedIndices,
        int partIndex,
        const std::vector<int>& blockIndexes)
            : BlockIndexes_(blockIndexes)
            , Reader_(
                New<TRepairReader>(
                    codec,
                    readers,
                    erasedIndices,
                    std::vector<int>(1, partIndex)))
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
            return MakePromise<IAsyncReader::TReadResult>(Result_);
        }
        if (!Reader_->HasNextBlock()) {
            return MakePromise<IAsyncReader::TReadResult>(TError("Block index out of range"));
        }

        return Reader_->RepairNextBlock().Apply(
            BIND([&] (TValueOrError<TRepairReader::TBlock> blockOrError) -> IAsyncReader::TAsyncReadResult {
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

class TRepairOnePartReader
    : public IAsyncReader
{
public:
    TRepairOnePartReader(
        const NErasure::ICodec* codec,
        const std::vector<int>& erasedIndices,
        int partIndex,
        const std::vector<IAsyncReaderPtr>& readers)
            : Codec_(codec)
            , ErasedIndices_(erasedIndices)
            , PartIndex_(partIndex)
            , Readers_(readers)
    { }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes)
    {
        auto session = New<TRepairOnePartSession>(Codec_, Readers_, ErasedIndices_, PartIndex_, blockIndexes);
        return BIND(&TRepairOnePartSession::Run, session)
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<int>* tags)
    {
        // TODO(ignat): add check that requests extensions indepent of the chunk part
        YCHECK(!partitionTag);
        return Readers_.front()->AsyncGetChunkMeta(partitionTag, tags);
    }

    virtual TChunkId GetChunkId() const
    {
        return Readers_.front()->GetChunkId();
    }

private:
    const NErasure::ICodec* Codec_;

    std::vector<int> ErasedIndices_;
    int PartIndex_;

    std::vector<IAsyncReaderPtr> Readers_;
};

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
// Repiar reader of all parts

class TRepairAllParts
    : public TRefCounted
{
public:
    TRepairAllParts(
        const NErasure::ICodec* codec,
        const std::vector<int>& erasedIndices,
        const std::vector<IAsyncReaderPtr>& readers,
        const std::vector<IAsyncWriterPtr>& writers)
            : Reader_(
                New<TRepairReader>(
                    codec,
                    readers,
                    erasedIndices,
                    erasedIndices))
            , Readers_(readers)
            , Writers_(writers)
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
        return Reader_->RepairNextBlock()
            .Apply(BIND(&TRepairAllParts::OnBlockRepaired, MakeStrong(this)));
    }
    
private:
    TAsyncError OnBlockRepaired(TValueOrError<TRepairReader::TBlock> blockOrError)
    {
        RETURN_PROMISE_IF_ERROR(blockOrError, TError);
        auto block = blockOrError.Value();
        YCHECK(IndexToWriter_.find(block.Index) != IndexToWriter_.end());
        auto writer = IndexToWriter_[block.Index];
        writer->TryWriteBlock(block.Data);
        
        return writer->GetReadyEvent().Apply(
            BIND([&] (TError error) -> TAsyncError {
                RETURN_PROMISE_IF_ERROR(error, TError);
                return Run();
            })
        );
    }


    TAsyncError Finalize()
    {
        return Readers_.front()->AsyncGetChunkMeta().Apply(
            BIND([&] (IAsyncReader::TGetMetaResult metaOrError) -> TAsyncError {
                RETURN_PROMISE_IF_ERROR(metaOrError, TError);
                auto collector = New<TParallelCollector<void>>();
                FOREACH (auto writer, Writers_) {
                    collector->Collect(writer->AsyncClose(metaOrError.Value()));
                }
                return collector->Complete();
            })
        );
    }

    TRepairReaderPtr Reader_;
    std::vector<IAsyncReaderPtr> Readers_;
    std::vector<IAsyncWriterPtr> Writers_;
    std::map<int, IAsyncWriterPtr> IndexToWriter_;
};

} // namespace NErasureReader


///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateErasureReader(
    const std::vector<IAsyncReaderPtr>& dataBlocksReaders)
{
    return New<NInternal::TErasureReader>(dataBlocksReaders);
}


IAsyncReaderPtr CreateErasureRepairReader(
    const NErasure::ICodec* codec,
    const std::vector<int>& erasedIndices,
    int partIndex,
    const std::vector<IAsyncReaderPtr>& readers)
{
    return New<NInternal::TRepairOnePartReader>(codec, erasedIndices, partIndex, readers);
}

TAsyncError RepairErasedBlocks(
    const NErasure::ICodec* codec,
    const std::vector<int>& erasedIndices,
    const std::vector<IAsyncReaderPtr>& readers,
    const std::vector<IAsyncWriterPtr>& writers)
{
    auto repair = New<NInternal::TRepairAllParts>(codec, erasedIndices, readers, writers);
    return BIND(&NInternal::TRepairAllParts::Run, repair)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

