#include "index_builder.h"

#include "format.h"
#include "line_reader.h"
#include "posting_codec.h"
#include "private.h"
#include "reader.h"
#include "helpers.h"

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/memory/ref.h>

#include <util/generic/algorithm.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TrigrepLogger;

////////////////////////////////////////////////////////////////////////////////

constexpr int PostingsPerBucketExtent = 15;

////////////////////////////////////////////////////////////////////////////////

class TPostingMap
{
public:
    explicit TPostingMap(i64 extentBufferCapacity)
    {
        // The trigram -> bucket-index map spans 64 MB and is probed randomly
        // once per input character; on 4 KB pages this thrashes the dTLB (the
        // dominant cost on older CPUs). Backing it with 2 MB transparent huge
        // pages cuts the page count from ~16K to ~32, so it fits in the TLB.
        BucketIndexesHolder_ = TSharedMutableRef::AllocateViaMmap(
            TotalTrigramCount * sizeof(int),
            {.InitializeStorage = false, .UseThp = true});
        BucketIndexes_ = reinterpret_cast<int*>(BucketIndexesHolder_.Begin());
        // InvalidBucketIndex (-1) is all-ones, so a byte-wise fill works.
        static_assert(InvalidBucketIndex == -1);
        ::memset(BucketIndexes_, 0xff, TotalTrigramCount * sizeof(int));

        Extents_.reserve(extentBufferCapacity);
    }

    // Adds all trigrams of a single line. Both the BucketIndexes_ lookup and the
    // dependent Buckets_ access are random probes into multi-megabyte arrays, so
    // a straight loop stalls on load-to-use latency for (almost) every trigram.
    // We instead run a three-stage software pipeline that prefetches each probe
    // well ahead of its use:
    //   A: read trigram from text, prefetch BucketIndexes_[trigram]
    //   B: resolve/allocate bucket index, prefetch Buckets_[bucketIndex]
    //   C: apply the posting to the (now cache-hot) bucket
    void AddLinePostings(const char* data, int trigramCount, ui32 blockIndex, TLineFingerprint lineFingerprint)
    {
        // BucketIndexes_ lookup (stage A->B) is the longest-latency probe, so it
        // gets the larger lookahead; the dependent Buckets_ probe (stage B->C)
        // gets the smaller one.
        constexpr int IndexPrefetchAhead = 32;
        constexpr int BucketPrefetchAhead = 8;
        constexpr int BucketRingMask = BucketPrefetchAhead - 1;
        static_assert((BucketPrefetchAhead & BucketRingMask) == 0);

        if (trigramCount <= 0) {
            return;
        }

        if (trigramCount <= IndexPrefetchAhead + BucketPrefetchAhead) [[unlikely]] {
            for (int index = 0; index < trigramCount; ++index) {
                AddPosting(ReadTrigram(data + index), blockIndex, lineFingerprint);
            }
            return;
        }

        int bucketRing[BucketPrefetchAhead];
        const auto* indexes = BucketIndexes_;

        // Stage A: read a trigram and prefetch its (sparse) BucketIndexes_ slot.
        auto stageA = [&] (int i) {
            ui32 trigram = ReadTrigram(data + i).Underlying();
            Y_PREFETCH_READ(indexes + trigram, 0);
        };
        // Stage B: resolve/allocate the bucket (its index slot is hot now) and
        // prefetch the bucket itself.
        auto stageB = [&] (int j) {
            int bucketIndex = LocateBucketIndex(ReadTrigram(data + j), blockIndex);
            bucketRing[j & BucketRingMask] = bucketIndex;
            Y_PREFETCH_WRITE(Buckets_.data() + bucketIndex, 3);
        };

        // Prime the pipeline.
        for (int a = 0; a < IndexPrefetchAhead + BucketPrefetchAhead; ++a) {
            stageA(a);
        }
        for (int b = 0; b < BucketPrefetchAhead; ++b) {
            stageB(b);
        }

        // Steady state: no bounds checks, all three stages fire every iteration.
        int hotCount = trigramCount - IndexPrefetchAhead - BucketPrefetchAhead;
        for (int c = 0; c < hotCount; ++c) {
            DoAddPosting(bucketRing[c & BucketRingMask], blockIndex, lineFingerprint);
            stageB(c + BucketPrefetchAhead);
            stageA(c + IndexPrefetchAhead + BucketPrefetchAhead);
        }

        // Drain.
        for (int c = hotCount; c < trigramCount; ++c) {
            DoAddPosting(bucketRing[c & BucketRingMask], blockIndex, lineFingerprint);
            int b = c + BucketPrefetchAhead;
            if (b < trigramCount) {
                stageB(b);
            }
        }
    }

    // Prefetches the bucket (and its first extent) for a trigram. Used to hide
    // the random Buckets_/Extents_ probes during the compression walk, which
    // visits trigrams in sorted (i.e. random bucket-index) order.
    void PrefetchBucket(TTrigram trigram) const
    {
        int bucketIndex = BucketIndexes_[trigram.Underlying()];
        if (bucketIndex == InvalidBucketIndex) {
            return;
        }
        const auto& bucket = Buckets_[bucketIndex];
        Y_PREFETCH_READ(&bucket, 1);
        if (bucket.FirstExtentIndex != InvalidExtentIndex) {
            Y_PREFETCH_READ(&Extents_[bucket.FirstExtentIndex], 1);
        }
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

    TSharedMutableRef BucketIndexesHolder_;
    int* BucketIndexes_ = nullptr;
    std::vector<TBucket> Buckets_;
    std::vector<TBucketExtent> Extents_;

    // Resolves the bucket for a trigram, allocating one on first occurrence.
    // Split out from DoAddPosting so that the (long-latency) lookup into the
    // sparse 64 MB BucketIndexes_ array can be software-prefetched ahead of the
    // dependent access into Buckets_.
    Y_FORCE_INLINE int LocateBucketIndex(TTrigram trigram, ui32 blockIndex)
    {
        int bucketIndex = BucketIndexes_[trigram.Underlying()];
        if (bucketIndex == InvalidBucketIndex) [[unlikely]] {
            bucketIndex = AllocateBucket(blockIndex);
            BucketIndexes_[trigram.Underlying()] = bucketIndex;
        }
        return bucketIndex;
    }

    Y_FORCE_INLINE void DoAddPosting(int bucketIndex, ui32 blockIndex, TLineFingerprint lineFingerprint)
    {
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

    void AddPosting(TTrigram trigram, ui32 blockIndex, TLineFingerprint lineFingerprint)
    {
        DoAddPosting(LocateBucketIndex(trigram, blockIndex), blockIndex, lineFingerprint);
    }

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
    TIndexSegmentBuilder(i64 segmentSize, IPostingCodec* codec)
        : SegmentSize_(segmentSize)
        , Codec_(codec)
    {
        // A conservative default; PrepareForCompression grows this to fit the
        // largest single posting list once it is known.
        Buffer_.resize(segmentSize * 4);
        Reset();
    }

    // Ensures the buffer can hold a full segment plus one maximal posting list,
    // so a list is always emitted in one piece before the flush check.
    void PrepareForCompression(int maxPostingListSize, ui32 postingUpperBound)
    {
        PostingUpperBound_ = postingUpperBound;
        i64 maxListBytes = Codec_->GetMaxByteSize(maxPostingListSize, postingUpperBound);
        i64 required = SegmentSize_ + maxListBytes;
        if (std::ssize(Buffer_) < required) {
            Buffer_.resize(required);
        }
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
        TrigramCount_ += 1;
        PostingCount_ += std::ssize(list);
        CurrentPtr_ = Codec_->Encode(CurrentPtr_, list, PostingUpperBound_);
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
    const i64 SegmentSize_;
    IPostingCodec* const Codec_;
    ui32 PostingUpperBound_ = 0;
    std::vector<char> Buffer_;
    char* __restrict__ CurrentPtr_;
    int TrigramCount_ = 0;
    int PostingCount_ = 0;
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
        , IndexSegmentBuilder_(Options_.IndexSegmentSize, GetPostingCodec(Options_.Format))
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
        int trigramCount = static_cast<int>(line.size()) - 2;
        PostingMap_.AddLinePostings(line.data(), trigramCount, blockIndex, lineFingerprint);
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

        // Postings are values in [0, BlockCount * 256); this bound is known to the
        // matcher from the chunk header and drives the interpolative coder.
        auto postingUpperBound = static_cast<ui32>(ChunkHeader_.BlockCount) * (LineFingerprintMask + 1) - 1;
        IndexSegmentBuilder_.PrepareForCompression(maxPostingListSize, postingUpperBound);

        auto joinedIndexSegmentsLengthThreshold = static_cast<i64>(UncompressedInputSize_ * Options_.IndexSizeFactor);

        constexpr int BucketPrefetchAhead = 8;
        int trigramCount = std::ssize(SortedTrigrams_);
        for (int index = 0; index < std::min(BucketPrefetchAhead, trigramCount); ++index) {
            PostingMap_.PrefetchBucket(SortedTrigrams_[index]);
        }

        for (int index = 0; index < trigramCount; ++index) {
            auto trigram = SortedTrigrams_[index];
            ++ChunkHeader_.IndexedTrigramCount;

            if (int ahead = index + BucketPrefetchAhead; ahead < trigramCount) {
                PostingMap_.PrefetchBucket(SortedTrigrams_[ahead]);
            }

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
            .Signature = GetIndexFormatSignature(Options_.Format),
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
