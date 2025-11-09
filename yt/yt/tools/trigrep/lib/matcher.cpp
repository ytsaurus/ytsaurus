#include "matcher.h"

#include "format.h"
#include "line_reader.h"
#include "private.h"
#include "reader.h"
#include "helpers.h"

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>

#include <util/system/unaligned_mem.h>

namespace NYT::NTrigrep {

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = TrigrepLogger;

////////////////////////////////////////////////////////////////////////////////

struct TMatcherStatistics
{
    i64 FramesChecked = 0;
    i64 LinesRead = 0;
    i64 LinesChecked = 0;
    i64 LinesMatched = 0;
};

using TTrigramSet = absl::flat_hash_set<TTrigram>;

////////////////////////////////////////////////////////////////////////////////

class TUnpackedIndexSegment
{
public:
    TUnpackedIndexSegment(TRef data, int trigramCount, int postingCount)
        : Postings_(postingCount)
        , PostingListStarts_(trigramCount + 1)
    {
        const char* __restrict__ src = data.begin();
        auto* dst = Postings_.data();
        int trigramIndex = 0;
        PostingListStarts_[0] = dst;

        while (src != data.end()) {
            ui8 groupHeaderLo = *src++;
            if (groupHeaderLo == TerminatorGroupTag) {
                ++trigramIndex;
                PostingListStarts_[trigramIndex] = dst;
                continue;
            }

            ui8 groupHeaderHi = *src++;
            ui16 groupHeader = groupHeaderLo | (groupHeaderHi << 8);
            ui16 blockIndex = groupHeader >> 6;
            ui8 tag = groupHeader & ((1U << 6) - 1);

            if (tag == GroupSize1Tag) {
                TLineFingerprint lineFingerprint = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint);
            } else if (tag == GroupSize2Tag) {
                TLineFingerprint lineFingerprint1 = *src++;
                TLineFingerprint lineFingerprint2 = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint1);
                *dst++ = MakePosting(blockIndex, lineFingerprint2);
            } else if (tag == BitmapGroupTag) {
                auto bitmap = ReadUnaligned<TLineFingerprintBitmap>(src);
                src += LineFingerprintBitmapByteSize;
                for (ui16 lineFingerprint = 0; lineFingerprint <= Max<TLineFingerprint>(); ++lineFingerprint) {
                    if (bitmap[lineFingerprint >> 3] & (1U << (lineFingerprint & 7))) {
                        *dst++ = MakePosting(blockIndex, lineFingerprint);
                    }
                }
            } else if (tag <= MaxShortBitpackedTag) {
                // Tag: 0..47 (== MaxShortBitpackedTag)
                int bitsPerDiff = (tag & 7) + 1;
                int groupSize = (tag >> 3) + 3;

                TLineFingerprint lineFingerprint = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint);

                std::tie(src, dst) = BitunpackShort(
                    dst,
                    MakePosting(blockIndex, lineFingerprint),
                    src,
                    groupSize,
                    bitsPerDiff);
            } else if (tag <= MaxLongBitpackedTag) {
                // Tag: 48..55 (== MaxLongBitpackedTag)
                int bitsPerDiff = tag - MaxShortBitpackedTag;

                TLineFingerprint lineFingerprint = *src++;
                *dst++ = MakePosting(blockIndex, lineFingerprint);

                std::tie(src, dst) = BitunpackLong(
                    dst,
                    MakePosting(blockIndex, lineFingerprint),
                    src,
                    bitsPerDiff);
            } else {
                THROW_ERROR_EXCEPTION("Broken index segment");
            }
        }
        if (trigramIndex != trigramCount) {
            THROW_ERROR_EXCEPTION("Broken index segment");
        }
        PostingListStarts_[trigramIndex++] = dst;
    }

    TRange<TPosting> GetPostingList(int perSegmentIndex) const
    {
        return {PostingListStarts_[perSegmentIndex], PostingListStarts_[perSegmentIndex + 1]};
    }

private:
    std::vector<TPosting> Postings_;
    std::vector<TPosting*> PostingListStarts_;

    static std::pair<const char*, TPosting*> BitunpackShort(
        TPosting* __restrict__ dst,
        TPosting basePosting,
        const char* __restrict__ src,
        ui16 groupSize,
        ui16 bitWidth)
    {
        auto blockIndex = BlockIndexFromPosting(basePosting);
        auto lineFingerprint = LineFingerprintFromPosting(basePosting);
        ui16 bitOffset = 0;
        ui16 bitMask = (1U << bitWidth) - 1;
        for (ui32 index = 1; index < groupSize; ++index) {
            ui16 value = ReadUnaligned<ui16>(src);
            auto diff = ((value >> bitOffset) & bitMask) + 1;

            bitOffset += bitWidth;
            if (bitOffset >= 8) {
                bitOffset -= 8;
                ++src;
            }

            lineFingerprint += diff;
            *dst++ = MakePosting(blockIndex, lineFingerprint);
        }
        if (bitOffset != 0) {
            ++src;
        }
        return {src, dst};
    }

    static std::pair<const char*, TPosting*> BitunpackLong(
        TPosting* __restrict__ dst,
        TPosting basePosting,
        const char* __restrict__ src,
        ui16 bitWidth)
    {
        auto blockIndex = BlockIndexFromPosting(basePosting);
        auto lineFingerprint = LineFingerprintFromPosting(basePosting);
        ui16 bitOffset = 0;
        ui16 bitMask = (1U << bitWidth) - 1;
        for (;;) {
            ui16 value = ReadUnaligned<ui16>(src);
            auto diff = (value >> bitOffset) & bitMask;

            bitOffset += bitWidth;
            if (bitOffset >= 8) {
                bitOffset -= 8;
                ++src;
            }

            if (diff == 0) {
                break;
            }

            lineFingerprint += diff;
            *dst++ = MakePosting(blockIndex, lineFingerprint);
        }
        if (bitOffset != 0) {
            ++src;
        }
        return {src, dst};
    }
};

////////////////////////////////////////////////////////////////////////////////

class TChunkIndexMatcher
{
public:
    TChunkIndexMatcher(
        IRandomReader* reader,
        TFile* indexFile,
        int chunkIndex,
        const std::vector<std::string>& patterns,
        const TTrigramSet& patternTrigrams,
        IMatcherCallbacks* callbacks,
        TMatcherStatistics* statistics)
        : Reader_(reader)
        , IndexFile_(indexFile)
        , ChunkIndex_(chunkIndex)
        , Patterns_(patterns)
        , PatternTrigrams_(patternTrigrams)
        , Callbacks_(callbacks)
        , Statistics_(statistics)
        , ChunkStartOffset_(IndexFile_->GetPosition())
    { }

    bool Run()
    {
        if (ChunkStartOffset_ == IndexFile_->GetLength()) {
            return false;
        }

        YT_LOG_INFO("Matching chunk (ChunkIndex: %v, IndexOffset: %v)",
            ChunkIndex_,
            IndexFile_->GetPosition());

        ReadIndex();

        if (ComputePatternTrigramIndexes()) {
            IntersectPostingLists();
            CheckCandidateFrames();
        }

        IndexFile_->Seek(ChunkEndOffset_, sSet);

        return true;
    }

private:
    IRandomReader* const Reader_;
    TFile* const IndexFile_;
    const int ChunkIndex_;
    const std::vector<std::string>& Patterns_;
    const TTrigramSet& PatternTrigrams_;
    IMatcherCallbacks* const Callbacks_;
    TMatcherStatistics* const Statistics_;

    const i64 ChunkStartOffset_;
    i64 ChunkEndOffset_ = -1;

    TChunkIndexHeader ChunkHeader_{};

    absl::flat_hash_map<TTrigram, int> TrigramsToIndex_;

    std::vector<TBlockHeader> BlockHeaders_;
    std::vector<TFrameHeader> FrameHeaders_;
    std::vector<TIndexSegmentHeader> SegmentHeaders_;

    std::vector<i64> SegmentStartOffsets_;
    std::vector<int> FrameIndexToFirstBlockIndex_;
    std::vector<i64> FrameIndexToFirstLineIndex_;
    std::vector<int> BlockIndexToFrameIndex_;

    struct TTrigramCompositeIndex
    {
        int SegmentIndex;
        int PerSegmentTrigramIndex;

        std::strong_ordering operator<=>(const TTrigramCompositeIndex& other) const = default;
    };

    std::vector<TTrigramCompositeIndex> TrigramIndexToTrigramCompositeIndex_;
    std::vector<TTrigramCompositeIndex> PatternsTrigramCompositeIndexes_;

    // Null indicates "nothing intersected yet".
    std::optional<std::vector<TPosting>> PostingListsIntersection_;

    void ReadIndex()
    {
        IndexFile_->Load(&ChunkHeader_, sizeof(ChunkHeader_));

        {
            std::vector<char> buffer(ChunkHeader_.TrigramCount * 3);
            IndexFile_->Load(buffer.data(), buffer.size());
            for (int index = 0; index < ChunkHeader_.TrigramCount; ++index) {
                auto trigram = PackTrigram(buffer[index * 3], buffer[index * 3 + 1], buffer[index * 3 + 2]);
                TrigramsToIndex_.emplace(trigram, index);
            }
        }

        {
            FrameHeaders_.resize(ChunkHeader_.FrameCount);
            IndexFile_->Load(FrameHeaders_.data(), FrameHeaders_.size() * sizeof(TFrameHeader));
        }

        {
            BlockHeaders_.resize(ChunkHeader_.BlockCount);
            IndexFile_->Load(BlockHeaders_.data(), BlockHeaders_.size() * sizeof(TBlockHeader));
        }

        {
            SegmentHeaders_.resize(ChunkHeader_.SegmentCount);
            IndexFile_->Load(SegmentHeaders_.data(), SegmentHeaders_.size() * sizeof(TIndexSegmentHeader));
        }

        {
            SegmentStartOffsets_.reserve(ChunkHeader_.SegmentCount);
            SegmentStartOffsets_.push_back(
                ChunkStartOffset_ +
                sizeof (TChunkIndexHeader) +
                ChunkHeader_.TrigramCount * 3 +
                ChunkHeader_.FrameCount * sizeof (TFrameHeader) +
                ChunkHeader_.BlockCount * sizeof (TBlockHeader) +
                ChunkHeader_.SegmentCount * sizeof (TIndexSegmentHeader));
            for (int index = 0; index < std::ssize(SegmentHeaders_) - 1; ++index) {
                SegmentStartOffsets_.push_back(SegmentStartOffsets_.back() + SegmentHeaders_[index].ByteSize);
            }
            ChunkEndOffset_ = SegmentStartOffsets_[0] + ChunkHeader_.SegmentsSize;
        }

        {
            FrameIndexToFirstBlockIndex_.reserve(ChunkHeader_.FrameCount);
            FrameIndexToFirstLineIndex_.reserve(ChunkHeader_.FrameCount);
            BlockIndexToFrameIndex_.reserve(ChunkHeader_.BlockCount);
            int blockIndex = 0;
            i64 lineIndex = ChunkHeader_.FirstLineIndex;
            for (int frameIndex = 0; frameIndex < std::ssize(FrameHeaders_); ++frameIndex) {
                FrameIndexToFirstBlockIndex_.push_back(blockIndex);
                FrameIndexToFirstLineIndex_.push_back(lineIndex);
                for (int perFrameBlockIndex = 0; perFrameBlockIndex < FrameHeaders_[frameIndex].BlockCount; ++perFrameBlockIndex) {
                    BlockIndexToFrameIndex_.push_back(frameIndex);
                    lineIndex += BlockHeaders_[blockIndex].LineCount;
                    blockIndex += 1;
                }
            }
        }

        {
            TrigramIndexToTrigramCompositeIndex_.reserve(ChunkHeader_.IndexedTrigramCount);
            int segmentIndex = 0;
            int perSegmentTrigramIndex = 0;
            for (int trigramIndex = 0; trigramIndex < ChunkHeader_.IndexedTrigramCount; ++trigramIndex) {
                TrigramIndexToTrigramCompositeIndex_.emplace_back(segmentIndex, perSegmentTrigramIndex);
                ++perSegmentTrigramIndex;
                if (perSegmentTrigramIndex == SegmentHeaders_[segmentIndex].TrigramCount) {
                    ++segmentIndex;
                    perSegmentTrigramIndex = 0;
                }
            }
        }

        YT_LOG_INFO("Chunk index read (TrigramCount: %v, IndexedTrigramCount: %v, "
            "BlockCount: %v, FrameCount: %v, SegmentCount: %v)",
            ChunkHeader_.TrigramCount,
            ChunkHeader_.IndexedTrigramCount,
            BlockHeaders_.size(),
            FrameHeaders_.size(),
            SegmentHeaders_.size());
    }

    bool ComputePatternTrigramIndexes()
    {
        for (auto trigram : PatternTrigrams_) {
            auto unpackedTrigram = UnpackTrigram(trigram);

            auto it = TrigramsToIndex_.find(trigram);
            if (it == TrigramsToIndex_.end()) {
                YT_LOG_INFO("Trigram is missing, no matches in chunk (Trigram: %v%v%v)",
                    unpackedTrigram[0],
                    unpackedTrigram[1],
                    unpackedTrigram[2]);
                return false;
            }

            auto trigramIndex = it->second;
            if (trigramIndex >= ChunkHeader_.IndexedTrigramCount) {
                YT_LOG_TRACE("Trigram is not indexed (Trigram: %v%v%v)",
                    unpackedTrigram[0],
                    unpackedTrigram[1],
                    unpackedTrigram[2]);
                continue;
            }

            auto trigramCompositeIndex = TrigramIndexToTrigramCompositeIndex_[trigramIndex];
            YT_LOG_TRACE("Trigram is indexed (Trigram: %v%v%v, Index: %v, CompositeIndex: %v:%v)",
                unpackedTrigram[0],
                unpackedTrigram[1],
                unpackedTrigram[2],
                trigramIndex,
                trigramCompositeIndex.SegmentIndex,
                trigramCompositeIndex.PerSegmentTrigramIndex);

            PatternsTrigramCompositeIndexes_.push_back(trigramCompositeIndex);
        }

        SortUnique(PatternsTrigramCompositeIndexes_);

        return true;
    }

    void IntersectPostingLists()
    {
        int startIndex = 0;
        while (startIndex < std::ssize(PatternsTrigramCompositeIndexes_)) {
            auto endIndex = startIndex;
            while (endIndex < std::ssize(PatternsTrigramCompositeIndexes_) &&
                PatternsTrigramCompositeIndexes_[endIndex].SegmentIndex == PatternsTrigramCompositeIndexes_[startIndex].SegmentIndex)
            {
                ++endIndex;
            }

            IntersectPostingListsInSegment(TRange(
                PatternsTrigramCompositeIndexes_.begin() + startIndex,
                PatternsTrigramCompositeIndexes_.begin() + endIndex));

            if (PostingListsIntersection_->empty()) {
                YT_LOG_INFO("Posting lists intersection is empty, no matches in chunk");
                break;
            }

            startIndex = endIndex;
        }
    }

    TUnpackedIndexSegment UnpackIndexSegment(int segmentIndex)
    {
        // NB: TUnpackedIndexSegmnet requires some more space after the end of the buffer
        // to be also accessible for read.
        const auto& segmentHeader = SegmentHeaders_[segmentIndex];
        std::vector<char> buffer(segmentHeader.ByteSize + sizeof(ui64));
        IndexFile_->Seek(SegmentStartOffsets_[segmentIndex], sSet);
        IndexFile_->Load(buffer.data(), segmentHeader.ByteSize);
        return TUnpackedIndexSegment(
            TRef(buffer.data(), segmentHeader.ByteSize),
            segmentHeader.TrigramCount,
            segmentHeader.PostingCount);
    }

    void IntersectPostingListsInSegment(TRange<TTrigramCompositeIndex> trigramCompositeIndexes)
    {
        int segmentIndex = trigramCompositeIndexes.Front().SegmentIndex;

        YT_LOG_DEBUG("Matching index segment (SegmentIndex: %v, TrigramCount: %v, CandidatesRemaining: %v)",
            segmentIndex,
            trigramCompositeIndexes.size(),
            PostingListsIntersection_ ? std::to_string(PostingListsIntersection_->size()) : "INF");

        auto unpackedIndexSegment = UnpackIndexSegment(segmentIndex);
        for (auto [_, perSegmentIndex] : trigramCompositeIndexes) {
            PrunePostingList(unpackedIndexSegment.GetPostingList(perSegmentIndex));
            if (PostingListsIntersection_->empty()) {
                break;
            }
        }
    }

    void PrunePostingList(TRange<TPosting> postingList)
    {
        if (PostingListsIntersection_) {
            std::vector<TPosting> newPostingListsIntersection;
            std::set_intersection(
                PostingListsIntersection_->begin(),
                PostingListsIntersection_->end(),
                postingList.begin(),
                postingList.end(),
                std::back_inserter(newPostingListsIntersection));
            PostingListsIntersection_ = std::move(newPostingListsIntersection);
        } else {
            PostingListsIntersection_ = std::vector<TPosting>(postingList.begin(), postingList.end());
        }
    }

    void CheckCandidateFrames()
    {
        if (PostingListsIntersection_) {
            if (PostingListsIntersection_->empty()) {
                return;
            }

            YT_LOG_INFO("Scanning matching frames");

            int startIndex = 0;
            while (startIndex < std::ssize(*PostingListsIntersection_)) {
                absl::flat_hash_set<TPosting> postingSet;

                auto endIndex = startIndex;
                auto frameIndex = BlockIndexToFrameIndex_[BlockIndexFromPosting((*PostingListsIntersection_)[startIndex])];
                while (endIndex < std::ssize(*PostingListsIntersection_) &&
                    BlockIndexToFrameIndex_[BlockIndexFromPosting((*PostingListsIntersection_)[endIndex])] == frameIndex)
                {
                    postingSet.insert((*PostingListsIntersection_)[endIndex]);
                    ++endIndex;
                }

                CheckCandidateFrame(frameIndex, &postingSet);
                startIndex = endIndex;
            }
        } else {
            YT_LOG_INFO("Falling back to full scan");

            for (int frameIndex = 0; frameIndex < ChunkHeader_.FrameCount; ++frameIndex) {
                CheckCandidateFrame(frameIndex, nullptr);
            }
        }
    }

    void CheckCandidateFrame(int frameIndex, const absl::flat_hash_set<TPosting>* postingSet)
    {
        const auto& frameHeader = FrameHeaders_[frameIndex];
        YT_LOG_DEBUG("Checking frame (FrameIndex: %v, InputStartOffset: %v, InputSize: %v, LineCount: %v)",
            frameIndex,
            frameHeader.InputStartOffset,
            frameHeader.InputSize,
            frameHeader.LineCount);

        auto input = Reader_->CreateFrameStream(
            frameHeader.InputStartOffset,
            frameHeader.InputStartOffset + frameHeader.InputSize);

        TLineReader lineReader(input.get());
        ui32 blockIndex = FrameIndexToFirstBlockIndex_[frameIndex];
        i64 lineIndex = FrameIndexToFirstLineIndex_[frameIndex];
        int perBlockLineIndex = 0;
        while (auto line = lineReader.ReadLine()) {
            auto posting = MakePosting(blockIndex, GetLineFingerprint(*line));
            if (!postingSet || postingSet->contains(posting)) {
                CheckLine(lineIndex, *line);
            }
            ++Statistics_->LinesRead;
            ++lineIndex;
            ++perBlockLineIndex;
            if (perBlockLineIndex == BlockHeaders_[blockIndex].LineCount) {
                ++blockIndex;
                perBlockLineIndex = 0;
            }
        }

        if (frameHeader.Checksum != lineReader.GetChecksum()) {
            THROW_ERROR_EXCEPTION("Checksum mismatch in frame %v:%v; does index file match the input?",
                ChunkIndex_,
                frameIndex);
        }

        auto actualLineCount = lineIndex - FrameIndexToFirstLineIndex_[frameIndex];
        if (frameHeader.LineCount != actualLineCount) {
            THROW_ERROR_EXCEPTION("Line count mismatch in frame %v:%v: expected %v, got %v",
                ChunkIndex_,
                frameIndex,
                frameHeader.LineCount,
                actualLineCount);
        }

        ++Statistics_->FramesChecked;
    }

    void CheckLine(i64 lineIndex, TStringBuf line)
    {
        ++Statistics_->LinesChecked;

        auto matchingRanges = ComputeMatchingRanges(line, Patterns_);
        if (matchingRanges.empty()) {
            YT_LOG_TRACE("Line does not match (LineIndex: %v, Line: %v)",
                lineIndex,
                line);
        } else {
            YT_LOG_TRACE("Line matches (LineIndex: %v, Line: %v)",
                lineIndex,
                line);
            Callbacks_->OnMatch(lineIndex, line, matchingRanges);
            ++Statistics_->LinesMatched;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileIndexMatcher
{
public:
    TFileIndexMatcher(
        IRandomReader* reader,
        TFile* indexFile,
        const std::vector<std::string>& patterns,
        IMatcherCallbacks* callbacks)
        : Reader_(reader)
        , IndexFile_(indexFile)
        , Patterns_(patterns)
        , Callbacks_(callbacks)
    { }

    void Run()
    {
        NProfiling::TWallTimer timer;

        YT_LOG_INFO("Started matching file");

        ReadHeader();

        BuildTrigramSet();

        for (int chunkIndex = 0; ;++chunkIndex) {
            TChunkIndexMatcher chunkMatcher(
                Reader_,
                IndexFile_,
                chunkIndex,
                Patterns_,
                PatternTrigrams_,
                Callbacks_,
                &Statistics_);
            if (!chunkMatcher.Run()) {
                break;
            }
        }

        YT_LOG_INFO("Finished matching file (ElapsedTime: %v, FramesChecked: %v, LinesRead: %v, LinesChecked: %v, LinesMatched: %v)",
            timer.GetElapsedTime(),
            Statistics_.FramesChecked,
            Statistics_.LinesRead,
            Statistics_.LinesChecked,
            Statistics_.LinesMatched);
    }

private:
    IRandomReader* const Reader_;
    TFile* const IndexFile_;
    const std::vector<std::string>& Patterns_;
    IMatcherCallbacks* const Callbacks_;

    TIndexFileHeader Header_;
    TMatcherStatistics Statistics_;
    TTrigramSet PatternTrigrams_;

    void ReadHeader()
    {
        IndexFile_->Load(&Header_, sizeof(Header_));

        if (Header_.Signature != TIndexFileHeader::V2Signature) {
            THROW_ERROR_EXCEPTION("Invalid index file signature: expected %x, got %x",
                TIndexFileHeader::V2Signature,
                Header_.Signature);
        }
    }

    void BuildTrigramSet()
    {
        for (const auto& pattern : Patterns_) {
            for (size_t index = 0; index + 2 < pattern.size(); index++) {
                PatternTrigrams_.insert(ReadTrigram(&pattern[index]));
            }
        }

        YT_LOG_INFO("Pattern trigrams built (TrigramCount: %v)",
            PatternTrigrams_.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunMatcher(
    IRandomReader* reader,
    TFile* indexFile,
    const std::vector<std::string>& patterns,
    IMatcherCallbacks* callbacks)
{
    TFileIndexMatcher(reader, indexFile, patterns, callbacks).Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTrigrep
