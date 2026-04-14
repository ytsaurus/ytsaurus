#include "index_builder.h"

#include "format.h"
#include "line_reader.h"
#include "private.h"
#include "reader.h"
#include "helpers.h"

#include <yt/yt/core/profiling/timing.h>

#include <util/generic/algorithm.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TrigrepLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr int PostingsPerBucketExtent = 15;

class TPostingMap
{
public:
    explicit TPostingMap(i64 extentBufferCapacity)
        : BucketIndexes_(TotalTrigramCount, InvalidBucketIndex)
    {
        Extents_.reserve(extentBufferCapacity);
    }

    void AddPosting(TTrigram trigram, ui32 blockIndex, TLineFingerprint lineFingerprint)
    {
        int bucketIndex = BucketIndexes_[trigram.Underlying()];
        if (bucketIndex == InvalidBucketIndex) [[unlikely]] {
            bucketIndex = AllocateBucket(blockIndex);
            BucketIndexes_[trigram.Underlying()] = bucketIndex;
        }

        auto* bucket = &Buckets_[bucketIndex];
        ++bucket->Count;

        auto setBit = [&] {
            bucket->FingerprintBitmap[lineFingerprint >> 6] |= (1ULL << (lineFingerprint & 63));
        };

        ui32 currentBlockIndex = bucket->CurrentBlockIndex;
        if (currentBlockIndex == blockIndex) [[likely]] {
            setBit();
            return;
        }

        int extentIndex = bucket->FirstExtentIndex;
        if (extentIndex == InvalidExtentIndex) [[unlikely]] {
            extentIndex = AllocateBucketExtent();
            bucket->FirstExtentIndex = extentIndex;
        }

        auto* extent = &Extents_[extentIndex];
        auto addPosting = [&] (TLineFingerprint currentLineFingerprint) {
            if (bucket->FirstExtentRemaining == 0) [[unlikely]] {
                auto newExtentIndex = AllocateBucketExtent();
                bucket->FirstExtentIndex = newExtentIndex;
                bucket->FirstExtentRemaining = PostingsPerBucketExtent;
                extent = &Extents_[newExtentIndex];
                extent->NextExtentIndex = extentIndex;
                extentIndex = newExtentIndex;
            }
            extent->Postings[--bucket->FirstExtentRemaining] = MakePosting(currentBlockIndex, currentLineFingerprint);
        };

        IterateBitmap(*bucket, addPosting);

        bucket->CurrentBlockIndex = blockIndex;
        bucket->FingerprintBitmap = {};
        setBit();
    }

    int GetPostingCount(TTrigram trigram) const
    {
        int bucketIndex = BucketIndexes_[trigram.Underlying()];
        return bucketIndex == InvalidBucketIndex
            ? 0
            : Buckets_[bucketIndex].Count;
    }

    TMutableRange<TPosting> GetPostings(TPosting* buffer, TTrigram trigram)
    {
        int bucketIndex = BucketIndexes_[trigram.Underlying()];
        if (bucketIndex == InvalidBucketIndex) [[unlikely]] {
            return {};
        }

        auto* bufferBegin = buffer;
        auto* bufferCurrent = buffer;

        const auto& bucket = Buckets_[bucketIndex];
        ui32 currentBlockIndex = bucket.CurrentBlockIndex;

        IterateBitmap(
            bucket,
            [&] (TLineFingerprint currentLineFingerprint) {
                *bufferCurrent++ = MakePosting(currentBlockIndex, currentLineFingerprint);
            });
        std::reverse(bufferBegin, bufferCurrent);

        int extentIndex = bucket.FirstExtentIndex;
        if (extentIndex != InvalidExtentIndex) {
            {
                const auto& extent = Extents_[extentIndex];
                int count = PostingsPerBucketExtent - bucket.FirstExtentRemaining;
                memcpy(
                    bufferCurrent,
                    extent.Postings.data() + bucket.FirstExtentRemaining,
                    sizeof(TPosting) * count);
                bufferCurrent += count;
                extentIndex = extent.NextExtentIndex;
            }

            while (extentIndex != InvalidExtentIndex) {
                const auto& extent = Extents_[extentIndex];
                memcpy(
                    bufferCurrent,
                    extent.Postings.data(),
                    sizeof(TPosting) * PostingsPerBucketExtent);
                bufferCurrent += PostingsPerBucketExtent;
                extentIndex = extent.NextExtentIndex;
            }
        }

        std::reverse(bufferBegin, bufferCurrent);

        return {bufferBegin, bufferCurrent};
    }

private:
    static constexpr int InvalidBucketIndex = -1;
    static constexpr int InvalidExtentIndex = -1;

    struct TBucket
    {
        int Count = 0;
        int FirstExtentIndex = InvalidExtentIndex;
        int FirstExtentRemaining = PostingsPerBucketExtent;
        ui32 CurrentBlockIndex  = 0;
        std::array<ui64, 4> FingerprintBitmap = {};
    };

    static_assert(sizeof(TBucket) == 48);

    struct TBucketExtent
    {
        int NextExtentIndex = InvalidExtentIndex;
        // Filled from back to front.
        std::array<TPosting, PostingsPerBucketExtent> Postings;
    };

    static_assert(sizeof(TBucketExtent) == 64);

    std::vector<int> BucketIndexes_;
    std::vector<TBucket> Buckets_;
    std::vector<TBucketExtent> Extents_;

    int AllocateBucket(ui32 blockIndex)
    {
        auto index = std::ssize(Buckets_);
        auto& bucket = Buckets_.emplace_back();
        bucket.CurrentBlockIndex = blockIndex;
        return index;
    }

    int AllocateBucketExtent()
    {
        auto index = std::ssize(Extents_);
        Extents_.emplace_back();
        return index;
    }

    template <class F>
    static void IterateBitmap(const TBucket& bucket, F&& func)
    {
        auto iterate = [&] (size_t bitmapIndex) {
            ui64 bitset = bucket.FingerprintBitmap[bitmapIndex];
            while (bitset != 0) {
                auto currentLineFingerprint = TLineFingerprint(__builtin_ctzll(bitset) + bitmapIndex * 64);
                bitset &= bitset - 1;
                func(currentLineFingerprint);
            }
        };
        iterate(0);
        iterate(1);
        iterate(2);
        iterate(3);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIndexSegmentBuilder
{
public:
    explicit TIndexSegmentBuilder(i64 segmentSize)
        // Rough upper estimate:
        // <= segmentSize groups each taking 3 bytes (2 for header + 1 for terminator)
        // <= segmentSize fingerprints each taking 1 byte
        : Buffer_(segmentSize * 4)
    {
        Reset();
    }

    void Reset()
    {
        CurrentPtr_ = Buffer_.data();
        TrigramCount_ = 0;
        PostingCount_ = 0;
    }

    void Add(TRange<TPosting> list)
    {
        int listSize = std::ssize(list);

        TrigramCount_ += 1;
        PostingCount_ += listSize;

        int startIndex = 0;
        while (startIndex < listSize) {
            ui32 blockIndex = BlockIndexFromPosting(list[startIndex]);

            YT_ASSERT(blockIndex < MaxBlocksPerChunk);
            int maxFingerprintDiff = 0;
            int endIndex = startIndex + 1;
            while (endIndex < listSize && BlockIndexFromPosting(list[endIndex]) == blockIndex) {
                maxFingerprintDiff = std::max(
                    maxFingerprintDiff,
                    static_cast<int>(LineFingerprintFromPosting(list[endIndex] - list[endIndex - 1])));
                ++endIndex;
            }

            auto groupSize = static_cast<ui32>(endIndex - startIndex);
            ui16 groupHeader = blockIndex << 6;

            [&] {
                if (groupSize == 1) {
                    // Tag: 61 (== GroupSize1Tag)
                    Write<ui16>(groupHeader | GroupSize1Tag);
                    Write<ui8>(LineFingerprintFromPosting(list[startIndex]));
                    return;
                }

                if (groupSize == 2) {
                    // Tag: 62 (== GroupSize2Tag)
                    Write<ui16>(groupHeader | GroupSize2Tag);
                    Write<ui8>(LineFingerprintFromPosting(list[startIndex]));
                    Write<ui8>(LineFingerprintFromPosting(list[startIndex + 1]));
                    return;
                }

                if (/* groupSize >= 3 && */ groupSize <= 8) {
                    ui32 bitsPerDiff = GetBitsPerDiff(maxFingerprintDiff - 1);
                    auto bitpackedSize = GetBitpackedSize(groupSize - 1, bitsPerDiff);
                    if (bitpackedSize + 1 < LineFingerprintBitmapByteSize) {
                        // Tag: 0..47 (== MaxShortBitpackedTag)
                        Write<ui16>(groupHeader | (bitsPerDiff - 1) | ((groupSize - 3) << 3));
                        Write<ui8>(LineFingerprintFromPosting(list[startIndex]));
                        ui64 packedValue = BitpackShort(&list[startIndex], groupSize, bitsPerDiff);
                        Write(reinterpret_cast<const char*>(&packedValue), bitpackedSize);
                        return;
                    }
                }

                if (groupSize >= 8) {
                    ui32 bitsPerDiff = GetBitsPerDiff(maxFingerprintDiff);
                    int bitpackedSize = GetBitpackedSize(groupSize, bitsPerDiff);
                    if (bitpackedSize + 1 < LineFingerprintBitmapByteSize) {
                        // Tag: 48..55 (== MaxLongBitpackedTag)
                        Write<ui16>(groupHeader | (47 + bitsPerDiff));
                        Write<ui8>(LineFingerprintFromPosting(list[startIndex]));
                        auto packedValue = BitpackLong(&list[startIndex], groupSize, bitsPerDiff);
                        Write(reinterpret_cast<const char*>(packedValue.data()), bitpackedSize);
                        return;
                    }
                }

                {
                    Write<ui16>(groupHeader | BitmapGroupTag);
                    auto bitmap = BuildBitmap(&list[startIndex], groupSize);
                    Write(bitmap);
                }
            }();

            startIndex = endIndex;
        }

        // Tag: 63 (== GroupSize1Tag)
        Write(TerminatorGroupTag);
    }

    TRange<char> GetBuffer() const
    {
        return {Buffer_.data(), CurrentPtr_};
    }

    int GetTrigramCount() const
    {
        return TrigramCount_;
    }

    int GetPostingCount() const
    {
        return PostingCount_;
    }

private:
    std::vector<char> Buffer_;
    char* __restrict__ CurrentPtr_;
    int TrigramCount_ = 0;
    int PostingCount_ = 0;

    template <class T>
    void Write(const T& value)
    {
        Write(reinterpret_cast<const char*>(&value), sizeof(value));
    }

    void Write(const char* __restrict__ data, size_t size)
    {
        memcpy(CurrentPtr_, data, size);
        CurrentPtr_ += size;
    }

    static ui32 GetBitpackedSize(ui32 count, ui32 bitWidth)
    {
        return (count * bitWidth + 7) / 8;
    }

    static ui32 GetBitsPerDiff(int maxDiff)
    {
        if (maxDiff <= 1) {
            return 1;
        } else if (maxDiff <= 3) {
            return 2;
        } else if (maxDiff <= 7) {
            return 3;
        } else if (maxDiff <= 15) {
            return 4;
        } else if (maxDiff <= 31) {
            return 5;
        } else if (maxDiff <= 63) {
            return 6;
        } else if (maxDiff <= 127) {
            return 7;
        } else {
            return 8;
        }
    }

    static ui64 BitpackShort(
        const TPosting* __restrict__ src,
        ui32 groupSize,
        ui32 bitWidth)
    {
        YT_ASSERT(groupSize >= 3 && groupSize <= 8);
        YT_ASSERT((groupSize - 1) * bitWidth <= 64);
        ui64 result = 0;
        ui32 bitOffset = 0;

#define PACK() \
                result |= static_cast<ui64>(LineFingerprintFromPosting(src[1] - src[0]) - 1) << bitOffset; \
                bitOffset += bitWidth; \
                src += 1;
#define PACK_CASE(n) \
                case n: \
                PACK() \
                [[fallthrough]];

        switch (groupSize) {
            PACK_CASE(8)
            PACK_CASE(7)
            PACK_CASE(6)
            PACK_CASE(5)
            PACK_CASE(4)
            default: break;
        }
        PACK()
        PACK()

#undef PACK
#undef PACK_CASE

        return result;
    }

    // BitmapByteSize plus some spare space at the end.
    using TBitpackLongResult = std::array<ui32, 10>;

    static TBitpackLongResult BitpackLong(
        const TPosting* __restrict__ src,
        ui32 groupSize,
        ui32 bitWidth)
    {
        // NB: No need to zero-initialize, see the end.
        TBitpackLongResult result;
        auto* dst = result.data();
        ui64 current = 0;
        ui32 bitOffset = 0;
        for (ui32 index = 0; index < groupSize - 1; ++index) {
            current |= static_cast<ui64>(LineFingerprintFromPosting(src[1] - src[0])) << bitOffset;
            src += 1;
            bitOffset += bitWidth;
            if (bitOffset >= 32) {
                *dst++ = current & 0xffffffff;
                current >>= 32;
                bitOffset -= 32;
            }
        }
        // Flush the tail and also ensure that group zero terminator is written.
        *dst++ = current;
        *dst = 0;
        return result;
    }

    static TLineFingerprintBitmap BuildBitmap(
        const TPosting* __restrict__ src,
        ui32 groupSize)
    {
        // NB: Zero-initialize.
        TLineFingerprintBitmap bitmap{};
        for (ui32 index = 0; index < groupSize; ++index) {
            auto fingerprint = src[index] & LineFingerprintMask;
            bitmap[fingerprint >> 3] |= 1U << (fingerprint & 7);
        }
        return bitmap;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkIndexBuilder
{
public:
    TChunkIndexBuilder(
        ISequentialReader* reader,
        IOutputStream* output,
        const TBuildIndexOptions& options,
        IIndexBuilderCallbacks* callbacks,
        int chunkIndex,
        i64 firstLineIndex)
        : Reader_(reader)
        , Output_(output)
        , Options_(options)
        , Callbacks_(callbacks)
        , ChunkIndex_(chunkIndex)
        , IndexSegmentBuilder_(Options_.IndexSegmentSize)
        , PostingMap_(static_cast<i64>(Options_.ChunkSize / PostingsPerBucketExtent * /*overhead*/ 1.25))
    {
        if (Options_.ChunkSize <= 0) {
            THROW_ERROR_EXCEPTION("ChunkSize must be positive");
        }
        if (Options_.BlockSize <= 0) {
            THROW_ERROR_EXCEPTION("BlockSize must be positive");
        }
        if (Options_.IndexSizeFactor <= 0) {
            THROW_ERROR_EXCEPTION("IndexSizeFactor must be positive");
        }

        ChunkHeader_.InputStartOffset = Reader_->GetCurrentFrameStartOffset();
        ChunkHeader_.FirstLineIndex = firstLineIndex;
    }

    bool Build()
    {
        auto input = Reader_->TryBeginNextFrame();
        if (!input) {
            return false;
        }

        NProfiling::TWallTimer timer;

        YT_LOG_INFO("Started building chunk index (ChunkIndex: %v, StartOffset: %v)",
            ChunkIndex_,
            Reader_->GetCurrentFrameStartOffset());

        ReportProgress();

        ui32 blockIndex = 0;
        i64 blockLineCount = 0;
        i64 blockSize = 0;
        int frameBlockCount = 0;

        auto finishBlock = [&] {
            if (blockSize > 0) {
                auto& blockHeader = BlockHeaders_.emplace_back();
                blockHeader = {};
                blockHeader.LineCount = blockLineCount;

                YT_LOG_DEBUG("Block finished (BlockIndex: %v, BlockLineCount: %v, BlockSize: %v)",
                    blockIndex,
                    blockLineCount,
                    blockSize);

                ++blockIndex;
                ++frameBlockCount;

                blockLineCount = 0;
                blockSize = 0;
            }
        };

        auto startBlock = [&] {
            YT_LOG_DEBUG("Block started (BlockIndex: %v)",
                blockIndex);
        };

        int frameIndex = 0;
        for (;;) {
            auto frameStartOffset = Reader_->GetCurrentFrameStartOffset();

            YT_LOG_DEBUG("Started reading chunk frame (FrameIndex: %v, FrameStartOffset: %v)",
                frameIndex,
                frameStartOffset);

            TLineReader lineReader(input.get());
            ui32 perFrameLineIndex = 0;
            startBlock();
            while (auto line = lineReader.ReadLine()) {
                auto lineFingerprint = GetLineFingerprint(*line);
                IndexLine(blockIndex, lineFingerprint, *line);
                auto lineSize = line->size() + 1;
                blockLineCount += 1;
                blockSize += lineSize;
                UncompressedInputSize_ += lineSize;
                perFrameLineIndex += 1;
                ChunkHeader_.LineCount += 1;
                if (blockSize >= Options_.BlockSize + /*slack*/ 64_KBs && blockIndex + 1 < MaxBlocksPerChunk) {
                    finishBlock();
                    startBlock();
                }
            }
            finishBlock();

            auto frameEndOffset = Reader_->GetCurrentFrameStartOffset();

            auto& frameHeader = FrameHeaders_.emplace_back();
            frameHeader = {};
            frameHeader.InputStartOffset = frameStartOffset;
            frameHeader.InputSize = frameEndOffset - frameStartOffset;
            frameHeader.Checksum = lineReader.GetChecksum();
            frameHeader.LineCount = perFrameLineIndex;
            frameHeader.BlockCount = frameBlockCount;

            YT_LOG_DEBUG(
                "Finished reading chunk frame (FrameIndex: %v, FrameInputSize: %v, "
                "FrameLineCount: %v, FrameBlockCount: %v, FrameChecksum: %x)",
                frameIndex,
                frameHeader.InputSize,
                frameHeader.LineCount,
                frameHeader.BlockCount,
                frameHeader.Checksum);

            if (UncompressedInputSize_ >= Options_.ChunkSize || frameIndex + 1 == MaxBlocksPerChunk) {
                break;
            }

            ++frameIndex;
            frameBlockCount = 0;

            input = Reader_->TryBeginNextFrame();
            if (!input) {
                break;
            }

            ReportProgress();
        }

        ChunkHeader_.InputSize = Reader_->GetCurrentFrameStartOffset() - ChunkHeader_.InputStartOffset;
        ChunkHeader_.FrameCount = std::ssize(FrameHeaders_);
        ChunkHeader_.BlockCount = std::ssize(BlockHeaders_);

        YT_LOG_INFO("Finished indexing chunk frames (LineCount: %v, FrameCount: %v, BlockCount: %v, "
            "InputSize: %v, UncompressedInputSize: %v)",
            ChunkHeader_.LineCount,
            std::ssize(FrameHeaders_),
            std::ssize(BlockHeaders_),
            ChunkHeader_.InputSize,
            UncompressedInputSize_);

        SortTrigrams();

        BuildCompressedPostingLists();

        ChunkHeader_.SegmentCount = std::ssize(SegmentHeaders_);

        ReportProgress();
        WriteIndex();

        YT_LOG_INFO(
            "Finished building chunk index "
            "(ChunkIndex: %v, ElapsedTime: %v, Trigrams: %v, IndexedTrigrams: %v (%v%%), SegmentCount: %v)",
            ChunkIndex_,
            timer.GetElapsedTime(),
            ChunkHeader_.TrigramCount,
            ChunkHeader_.IndexedTrigramCount,
            ChunkHeader_.IndexedTrigramCount * 100 / ChunkHeader_.TrigramCount,
            std::ssize(SegmentHeaders_));

        return true;
    }

    i64 GetChunkLineCount() const
    {
        return ChunkHeader_.LineCount;
    }

private:
    ISequentialReader* const Reader_;
    IOutputStream* const Output_;
    const TBuildIndexOptions Options_;
    IIndexBuilderCallbacks* const Callbacks_;
    const int ChunkIndex_;

    TIndexSegmentBuilder IndexSegmentBuilder_;

    i64 UncompressedInputSize_ = 0;

    TChunkIndexHeader ChunkHeader_ = {};
    std::vector<TFrameHeader> FrameHeaders_;
    std::vector<TBlockHeader> BlockHeaders_;
    std::vector<TIndexSegmentHeader> SegmentHeaders_;

    TPostingMap PostingMap_;

    std::vector<TTrigram> SortedTrigrams_;

    std::vector<char> JoinedCompressedIndexSegments_;

    void IndexLine(ui32 blockIndex, TLineFingerprint lineFingerprint, TStringBuf line)
    {
        const char* __restrict__ currentPtr = line.data();
        const char* __restrict__ endPtr = line.end() - 2;
        while (currentPtr < endPtr) {
            auto trigram = ReadTrigram(currentPtr++);
            PostingMap_.AddPosting(trigram, blockIndex, lineFingerprint);
        }
    }

    void SortTrigrams()
    {
        YT_LOG_INFO("Sorting trigrams");

        SortedTrigrams_.reserve(TotalTrigramCount);
        for (TTrigram trigram = TTrigram(0); trigram.Underlying() < TotalTrigramCount; trigram = TTrigram(trigram.Underlying() + 1)) {
            if (auto count = PostingMap_.GetPostingCount(trigram); count > 0) {
                SortedTrigrams_.push_back(trigram);
            }
        }

        ChunkHeader_.TrigramCount = SortedTrigrams_.size();

        SortBy(
            SortedTrigrams_,
            [&] (TTrigram trigram) { return PostingMap_.GetPostingCount(trigram); });
    }

    void MaybeFlushCurrentIndexSegment(bool force)
    {
        auto buffer = IndexSegmentBuilder_.GetBuffer();
        auto byteSize = std::ssize(buffer);
        if (byteSize == 0 || byteSize < Options_.IndexSegmentSize && !force) {
            return;
        }

        auto& segmentHeader = SegmentHeaders_.emplace_back();
        segmentHeader = {};
        segmentHeader.TrigramCount = IndexSegmentBuilder_.GetTrigramCount();
        segmentHeader.PostingCount = IndexSegmentBuilder_.GetPostingCount();
        segmentHeader.ByteSize = byteSize;

        YT_LOG_DEBUG("Index segment finished (TrigramCount: %v, PostingCount: %v, ByteSize: %v)",
            segmentHeader.TrigramCount,
            segmentHeader.PostingCount,
            segmentHeader.ByteSize);

        JoinedCompressedIndexSegments_.insert(
            JoinedCompressedIndexSegments_.end(),
            buffer.begin(),
            buffer.end());
        IndexSegmentBuilder_.Reset();
    }

    void BuildCompressedPostingLists()
    {
        YT_LOG_INFO("Started building compressed posting lists");

        int maxPostingListSize = SortedTrigrams_.empty() ? 0 : PostingMap_.GetPostingCount(SortedTrigrams_.back());
        auto postingListBuffer = std::make_unique<TPosting[]>(maxPostingListSize);

        auto joinedIndexSegmentsLengthThreshold = static_cast<i64>(UncompressedInputSize_ * Options_.IndexSizeFactor);

        for (auto trigram : SortedTrigrams_) {
            ++ChunkHeader_.IndexedTrigramCount;

            auto postingList = PostingMap_.GetPostings(postingListBuffer.get(), trigram);
            IndexSegmentBuilder_.Add(postingList);

            MaybeFlushCurrentIndexSegment(/*force*/ false);

            if (std::ssize(JoinedCompressedIndexSegments_) > joinedIndexSegmentsLengthThreshold) {
                break;
            }
        }

        MaybeFlushCurrentIndexSegment(/*force*/ true);

        ChunkHeader_.SegmentsSize = std::ssize(JoinedCompressedIndexSegments_);

        YT_LOG_INFO("Finished building compressed posting lists");
    }

    std::vector<char> BuildTrigramBuffer()
    {
        std::vector<char> buffer;
        buffer.reserve(SortedTrigrams_.size() * 3);
        for (auto trigram : SortedTrigrams_) {
            for (char ch : UnpackTrigram(trigram)) {
                buffer.push_back(ch);
            }
        }
        return buffer;
    }

    void WriteBuffer(TRef buffer)
    {
        Output_->Write(buffer.data(), buffer.size());
    }

    void WriteIndex()
    {
        Output_->Write(&ChunkHeader_, sizeof(ChunkHeader_));

        {
            auto trigramBuffer = BuildTrigramBuffer();
            WriteBuffer(TRef(trigramBuffer.data(), trigramBuffer.size()));
        }

        WriteBuffer(TRef(FrameHeaders_.data(), FrameHeaders_.size() * sizeof(TFrameHeader)));

        WriteBuffer(TRef(BlockHeaders_.data(), BlockHeaders_.size() * sizeof(TBlockHeader)));

        WriteBuffer(TRef(SegmentHeaders_.data(), SegmentHeaders_.size() * sizeof(TIndexSegmentHeader)));

        WriteBuffer(TRef(JoinedCompressedIndexSegments_.data(), JoinedCompressedIndexSegments_.size()));
    }

    void ReportProgress()
    {
        Callbacks_->OnProgress(
            Reader_->GetCurrentFrameStartOffset(),
            Reader_->GetTotalInputSize());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TIndexFileBuilder
{
public:
    TIndexFileBuilder(
        ISequentialReader* reader,
        IOutputStream* output,
        const TBuildIndexOptions& options,
        IIndexBuilderCallbacks* callbacks)
        : Reader_(reader)
        , Output_(output)
        , Options_(options)
        , Callbacks_(callbacks)
    { }

    void Run()
    {
        YT_LOG_INFO("Started building file index");

        WriteHeader();

        i64 currentLineIndex = 0;
        for (int chunkIndex = 0; ;++chunkIndex) {
            TChunkIndexBuilder builder(
                Reader_,
                Output_,
                Options_,
                Callbacks_,
                chunkIndex,
                currentLineIndex);
            if (!builder.Build()) {
                break;
            }
            currentLineIndex += builder.GetChunkLineCount();
        }

        YT_LOG_INFO("Finished building file index");
    }

private:
    ISequentialReader* const Reader_;
    IOutputStream* const Output_;
    const TBuildIndexOptions Options_;
    IIndexBuilderCallbacks* const Callbacks_;

    void WriteHeader()
    {
        TIndexFileHeader header{
            .Signature = TIndexFileHeader::V2Signature,
        };
        Output_->Write(&header, sizeof(header));
    }
};

////////////////////////////////////////////////////////////////////////////////

void BuildIndex(
    ISequentialReader* reader,
    IOutputStream* output,
    const TBuildIndexOptions& options,
    IIndexBuilderCallbacks* callbacks)
{
    TIndexFileBuilder(
        reader,
        output,
        options,
        callbacks)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
