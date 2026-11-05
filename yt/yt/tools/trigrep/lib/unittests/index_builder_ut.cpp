#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/tools/trigrep/lib/format.h>
#include <yt/yt/tools/trigrep/lib/index_builder.h>
#include <yt/yt/tools/trigrep/lib/matcher.h>
#include <yt/yt/tools/trigrep/lib/posting_codec.h>
#include <yt/yt/tools/trigrep/lib/uncompressed_reader.h>

#include <util/folder/tempdir.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#include <util/stream/file.h>

#include <util/system/file.h>

#include <random>

namespace NYT::NTrigrep {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TMatch
{
    i64 LineIndex;
    std::string Line;

    bool operator==(const TMatch& other) const = default;
    auto operator<=>(const TMatch& other) const = default;
};

class TNullBuilderCallbacks
    : public IIndexBuilderCallbacks
{
public:
    void OnProgress(i64 /*bytesIndexed*/, i64 /*bytesTotal*/) override
    { }
};

class TCollectingMatcherCallbacks
    : public IMatcherCallbacks
{
public:
    std::vector<TMatch> Matches;

    void OnMatch(
        i64 lineIndex,
        TStringBuf line,
        TRange<std::pair<int, int>> /*matchingRanges*/) override
    {
        Matches.push_back({lineIndex, std::string(line)});
    }
};

////////////////////////////////////////////////////////////////////////////////

// Splits text into lines exactly the way TLineReader does: lines are delimited
// by '\n', a trailing newline does not produce an extra empty line, and an empty
// input yields no lines.
std::vector<std::string> SplitLines(TStringBuf input)
{
    std::vector<std::string> lines;
    size_t position = 0;
    while (position < input.size()) {
        auto newlinePosition = input.find('\n', position);
        if (newlinePosition == TStringBuf::npos) {
            lines.emplace_back(input.substr(position));
            break;
        }
        lines.push_back(std::string(input.substr(position, newlinePosition - position)));
        position = newlinePosition + 1;
    }
    return lines;
}

// Reference implementation: a line matches iff it contains every pattern.
std::vector<TMatch> GrepReference(TStringBuf input, const std::vector<std::string>& patterns)
{
    std::vector<TMatch> result;
    auto lines = SplitLines(input);
    for (i64 lineIndex = 0; lineIndex < std::ssize(lines); ++lineIndex) {
        const auto& line = lines[lineIndex];
        bool matches = true;
        for (const auto& pattern : patterns) {
            if (line.find(pattern) == std::string::npos) {
                matches = false;
                break;
            }
        }
        if (matches) {
            result.push_back({lineIndex, line});
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TIndexBuilderTest
    : public ::testing::TestWithParam<EIndexFormat>
{
protected:
    TTempDir TempDir_;

    std::string InputPath() const
    {
        return std::string(TempDir_.Path().GetPath().c_str()) + "/input.log";
    }

    std::string IndexPath() const
    {
        return InputPath() + ".trindex";
    }

    void WriteInput(TStringBuf input)
    {
        TString path(InputPath());
        TFileOutput output(path);
        output.Write(input.data(), input.size());
        output.Finish();
    }

    void BuildIndexFile(TBuildIndexOptions options, i64 frameSize)
    {
        options.Format = GetParam();
        auto reader = CreateSequentialUncompressedReader(InputPath(), frameSize);
        TString path(IndexPath());
        TFileOutput output(path);
        TNullBuilderCallbacks callbacks;
        BuildIndex(reader.get(), &output, options, &callbacks);
        output.Finish();
    }

    std::vector<TMatch> Match(const std::vector<std::string>& patterns)
    {
        auto reader = CreateRandomUncompressedReader(InputPath());
        TFile indexFile(TString(IndexPath()), OpenExisting | RdOnly);
        TCollectingMatcherCallbacks callbacks;
        RunMatcher(reader.get(), &indexFile, patterns, &callbacks);
        Sort(callbacks.Matches);
        return callbacks.Matches;
    }

    TMatchStatistics MatchStatistics(const std::vector<std::string>& patterns, bool dryRun)
    {
        auto reader = CreateRandomUncompressedReader(InputPath());
        TFile indexFile(TString(IndexPath()), OpenExisting | RdOnly);
        TCollectingMatcherCallbacks callbacks;
        return RunMatcher(reader.get(), &indexFile, patterns, &callbacks, {.DryRun = dryRun});
    }

    // Full round trip: build an index for #input and run #patterns against it.
    std::vector<TMatch> BuildAndMatch(
        TStringBuf input,
        const std::vector<std::string>& patterns,
        TBuildIndexOptions options = GetDefaultOptions(),
        i64 frameSize = 1_MB)
    {
        WriteInput(input);
        BuildIndexFile(options, frameSize);
        return Match(patterns);
    }

    void ExpectMatchesReference(
        TStringBuf input,
        const std::vector<std::string>& patterns,
        TBuildIndexOptions options = GetDefaultOptions(),
        i64 frameSize = 1_MB)
    {
        auto actual = BuildAndMatch(input, patterns, options, frameSize);
        auto expected = GrepReference(input, patterns);
        Sort(expected);
        EXPECT_EQ(actual, expected);
    }

    // Options that fully index even small inputs (IndexSizeFactor is generous).
    static TBuildIndexOptions GetDefaultOptions()
    {
        return TBuildIndexOptions{
            .ChunkSize = 1_GB,
            .BlockSize = 1_MB,
            .IndexSegmentSize = 64_KB,
            .IndexSizeFactor = 100.0,
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_P(TIndexBuilderTest, Smoke)
{
    auto input = TStringBuf(
        "the quick brown fox\n"
        "jumps over the lazy dog\n"
        "the end\n");

    EXPECT_EQ(
        BuildAndMatch(input, {"the"}),
        (std::vector<TMatch>{
            {0, "the quick brown fox"},
            {1, "jumps over the lazy dog"},
            {2, "the end"},
        }));
}

TEST_P(TIndexBuilderTest, MultiplePatternsAreConjunctive)
{
    auto input = TStringBuf(
        "alpha beta gamma\n"
        "alpha gamma\n"
        "beta gamma\n"
        "alpha beta\n");

    EXPECT_EQ(
        BuildAndMatch(input, {"alpha", "beta"}),
        (std::vector<TMatch>{
            {0, "alpha beta gamma"},
            {3, "alpha beta"},
        }));
}

TEST_P(TIndexBuilderTest, AbsentPatternYieldsNoMatches)
{
    auto input = TStringBuf("hello world\nfoo bar baz\n");
    EXPECT_TRUE(BuildAndMatch(input, {"qux"}).empty());
    // Trigram present in the corpus but never on a single line together.
    EXPECT_TRUE(BuildAndMatch(input, {"hello", "baz"}).empty());
}

TEST_P(TIndexBuilderTest, ShortPatterns)
{
    // Patterns shorter than a trigram cannot be filtered by the index and must
    // fall back to scanning candidate frames.
    auto input = TStringBuf(
        "ab cd ef\n"
        "xy\n"
        "a\n"
        "abc\n");
    ExpectMatchesReference(input, {"ab"});
    ExpectMatchesReference(input, {"a"});
    ExpectMatchesReference(input, {"xy"});
    ExpectMatchesReference(input, {"abc"});
}

TEST_P(TIndexBuilderTest, LineEdgeCases)
{
    auto input = TStringBuf(
        "\n"               // empty line
        "a\n"              // 1 char, no trigram
        "ab\n"             // 2 chars, no trigram
        "abc\n"            // exactly one trigram
        "\n"
        "trailing");       // no terminating newline
    ExpectMatchesReference(input, {"abc"});
    ExpectMatchesReference(input, {"trailing"});
    ExpectMatchesReference(input, {"a"});
}

TEST_P(TIndexBuilderTest, NoTrailingNewline)
{
    auto input = TStringBuf("first line\nsecond line");
    EXPECT_EQ(
        BuildAndMatch(input, {"line"}),
        (std::vector<TMatch>{
            {0, "first line"},
            {1, "second line"},
        }));
}

TEST_P(TIndexBuilderTest, DuplicateLinesAllMatch)
{
    // Identical lines share a fingerprint and are deduplicated inside a block,
    // yet every occurrence must still be reported.
    std::string input;
    for (int i = 0; i < 100; ++i) {
        input += "needle in a haystack\n";
    }
    auto matches = BuildAndMatch(input, {"needle"});
    ASSERT_EQ(std::ssize(matches), 100);
    for (int i = 0; i < 100; ++i) {
        EXPECT_EQ(matches[i].LineIndex, i);
    }
}

TEST_P(TIndexBuilderTest, PostingGroupEncodingSizes)
{
    // Drives every posting-list group-encoding branch (size 1, 2, 3..8, >=8,
    // bitmap fallback) by varying how many distinct lines contain one trigram.
    for (int matchingLineCount : {1, 2, 3, 5, 8, 9, 16, 64, 200}) {
        std::string input;
        for (int i = 0; i < matchingLineCount; ++i) {
            // A unique suffix gives each line a distinct fingerprint.
            input += "zzz filler line number " + std::to_string(i) + "\n";
        }
        // Some non-matching lines interleaved.
        input += "unrelated content\n";

        auto matches = BuildAndMatch(input, {"zzz"});
        EXPECT_EQ(std::ssize(matches), matchingLineCount)
            << "matchingLineCount=" << matchingLineCount;
    }
}

TEST_P(TIndexBuilderTest, MultipleSegments)
{
    // Tiny segment size forces the posting lists across many index segments.
    std::string input;
    for (int i = 0; i < 500; ++i) {
        input += "row " + std::to_string(i) + " token" + std::to_string(i % 37) + "\n";
    }
    auto options = GetDefaultOptions();
    options.IndexSegmentSize = 256;
    ExpectMatchesReference(input, {"token13"}, options);
    ExpectMatchesReference(input, {"row"}, options);
}

TEST_P(TIndexBuilderTest, MultipleFrames)
{
    std::string input;
    for (int i = 0; i < 200; ++i) {
        input += "frame line " + std::to_string(i) + "\n";
    }
    // Small frames: many line-aligned frames.
    ExpectMatchesReference(input, {"line"}, GetDefaultOptions(), /*frameSize*/ 64);
    ExpectMatchesReference(input, {"line 137"}, GetDefaultOptions(), /*frameSize*/ 64);
}

TEST_P(TIndexBuilderTest, MultipleBlocks)
{
    // A block only finishes after BlockSize + 64 KB of slack, so emit > 256 KB.
    std::string input;
    for (int i = 0; input.size() < 256_KB; ++i) {
        input += "block payload line " + std::to_string(i) + " marker" + std::to_string(i % 11) + "\n";
    }
    auto options = GetDefaultOptions();
    options.BlockSize = 1;
    ExpectMatchesReference(input, {"marker7"}, options);
    ExpectMatchesReference(input, {"payload"}, options);
}

TEST_P(TIndexBuilderTest, MultipleChunks)
{
    // Each chunk is indexed independently; line numbers must stay global.
    std::string input;
    for (int i = 0; input.size() < 512_KB; ++i) {
        input += "chunk row " + std::to_string(i) + " key" + std::to_string(i % 23) + "\n";
    }
    auto options = GetDefaultOptions();
    options.ChunkSize = 128_KB;
    ExpectMatchesReference(input, {"key19"}, options, /*frameSize*/ 4_KB);
    ExpectMatchesReference(input, {"chunk row 9000"}, options, /*frameSize*/ 4_KB);
}

TEST_P(TIndexBuilderTest, LongLinesAroundPrefetchThreshold)
{
    // The builder switches between scalar and pipelined trigram processing
    // around ~40 trigrams per line; cover lengths straddling the threshold.
    for (int length : {1, 2, 3, 38, 39, 40, 41, 48, 49, 100, 4096, 70000}) {
        std::string line(length, 'a');
        line += "NEEDLE";
        auto input = "prefix\n" + line + "\nsuffix\n";
        auto matches = BuildAndMatch(input, {"NEEDLE"});
        ASSERT_EQ(std::ssize(matches), 1) << "length=" << length;
        EXPECT_EQ(matches[0].LineIndex, 1) << "length=" << length;
    }
}

TEST_P(TIndexBuilderTest, IndexSizeFactorTruncationStillCorrect)
{
    // A tiny IndexSizeFactor leaves most trigrams unindexed; the matcher must
    // fall back to scanning and still return exact results.
    std::string input;
    for (int i = 0; i < 300; ++i) {
        input += "log entry " + std::to_string(i) + " status=" + (i % 5 ? "ok" : "fail") + "\n";
    }
    auto options = GetDefaultOptions();
    options.IndexSizeFactor = 0.001;
    ExpectMatchesReference(input, {"status=fail"}, options);
    ExpectMatchesReference(input, {"entry 250"}, options);
}

TEST_P(TIndexBuilderTest, IndexHeaderSignature)
{
    BuildAndMatch("some indexable content here\n", {"content"});
    TFile indexFile(TString(IndexPath()), OpenExisting | RdOnly);
    TIndexFileHeader header{};
    indexFile.Load(&header, sizeof(header));
    EXPECT_EQ(header.Signature, GetIndexFormatSignature(GetParam()));
}

TEST_P(TIndexBuilderTest, BuildIsDeterministic)
{
    auto input = TStringBuf(
        "deterministic builds produce identical bytes\n"
        "deterministic builds produce identical bytes\n"
        "another line with some other tokens\n");

    auto build = [&] {
        WriteInput(input);
        BuildIndexFile(GetDefaultOptions(), /*frameSize*/ 64);
        return TUnbufferedFileInput(TString(IndexPath())).ReadAll();
    };

    auto first = build();
    auto second = build();
    EXPECT_EQ(first, second);
    EXPECT_FALSE(first.empty());
}

TEST_P(TIndexBuilderTest, DryRunReportsCandidatesWithoutScanning)
{
    // One selective needle buried in a lot of filler, split into many small
    // frames so the index can prune most of them.
    std::string input;
    for (int i = 0; i < 200; ++i) {
        input += "filler line number " + std::to_string(i) + "\n";
    }
    input += "the UNIQUENEEDLE marker line\n";
    for (int i = 0; i < 200; ++i) {
        input += "more filler content " + std::to_string(i) + "\n";
    }
    WriteInput(input);
    BuildIndexFile(GetDefaultOptions(), /*frameSize*/ 64);

    auto dry = MatchStatistics({"UNIQUENEEDLE"}, /*dryRun*/ true);
    auto real = MatchStatistics({"UNIQUENEEDLE"}, /*dryRun*/ false);

    // The corpus spans many frames and the index prunes almost all of them.
    EXPECT_GT(dry.FramesTotal, 1);
    EXPECT_LT(dry.FramesSelected, dry.FramesTotal);
    EXPECT_LE(dry.BytesSelected, dry.BytesTotal);

    // The selection accounting is identical to a real run and depends only on
    // the index, so a dry run computes it without scanning the input.
    EXPECT_EQ(dry.FramesTotal, real.FramesTotal);
    EXPECT_EQ(dry.BytesTotal, real.BytesTotal);
    EXPECT_EQ(dry.FramesSelected, real.FramesSelected);
    EXPECT_EQ(dry.BytesSelected, real.BytesSelected);
    EXPECT_EQ(dry.FramesSelected, real.FramesChecked);
    EXPECT_EQ(dry.FramesChecked, 0);
    EXPECT_EQ(dry.LinesRead, 0);
    EXPECT_GT(real.LinesRead, 0);

    // A short pattern cannot be filtered: every frame is selected.
    auto shortPattern = MatchStatistics({"in"}, /*dryRun*/ true);
    EXPECT_EQ(shortPattern.FramesSelected, shortPattern.FramesTotal);

    // An absent trigram prunes the whole chunk: nothing is selected.
    auto absent = MatchStatistics({"QQQ_NOT_PRESENT_QQQ"}, /*dryRun*/ true);
    EXPECT_EQ(absent.FramesSelected, 0);
}

TEST_P(TIndexBuilderTest, UnknownSignatureThrows)
{
    BuildAndMatch("some indexable content here\n", {"content"});

    // Corrupt the format signature at the head of the index file.
    {
        TFile indexFile(TString(IndexPath()), OpenExisting | WrOnly);
        ui64 badSignature = 0;
        indexFile.Pwrite(&badSignature, sizeof(badSignature), 0);
    }

    auto reader = CreateRandomUncompressedReader(InputPath());
    TFile indexFile(TString(IndexPath()), OpenExisting | RdOnly);
    TCollectingMatcherCallbacks callbacks;
    EXPECT_THROW_WITH_SUBSTRING(
        RunMatcher(reader.get(), &indexFile, {"content"}, &callbacks),
        "Invalid index file signature");
}

////////////////////////////////////////////////////////////////////////////////

std::string MakeRandomLog(ui32 seed)
{
    std::mt19937 rng(seed);
    static constexpr TStringBuf Alphabet = "abcdef .:=/"_sb;
    int lineCount = 5 + rng() % 80;
    std::string result;
    for (int i = 0; i < lineCount; ++i) {
        int length = rng() % 30;
        for (int j = 0; j < length; ++j) {
            result += Alphabet[rng() % Alphabet.size()];
        }
        result += '\n';
    }
    // Occasionally drop the trailing newline.
    if ((seed & 1) && !result.empty()) {
        result.pop_back();
    }
    return result;
}

std::vector<std::string> MakeRandomPatterns(ui32 seed, const std::string& log)
{
    std::mt19937 rng(seed * 2654435761u + 12345u);
    auto lines = SplitLines(log);
    std::vector<std::string> patterns;
    int patternCount = 1 + rng() % 2;
    for (int i = 0; i < patternCount; ++i) {
        // Bias towards substrings actually present in the corpus.
        if (!lines.empty() && rng() % 4 != 0) {
            const auto& line = lines[rng() % lines.size()];
            if (!line.empty()) {
                int start = rng() % line.size();
                int length = 1 + rng() % (line.size() - start);
                patterns.push_back(line.substr(start, length));
                continue;
            }
        }
        static constexpr auto Alphabet = "abcdefg"_sb;
        std::string pattern;
        int length = 1 + rng() % 4;
        for (int j = 0; j < length; ++j) {
            pattern += Alphabet[rng() % Alphabet.size()];
        }
        patterns.push_back(pattern);
    }
    return patterns;
}

TEST_P(TIndexBuilderTest, MatchesReferenceRandomized)
{
    for (ui32 seed = 1; seed <= 100; ++seed) {
        auto log = MakeRandomLog(seed);
        auto patterns = MakeRandomPatterns(seed, log);

        auto options = GetDefaultOptions();
        options.IndexSegmentSize = 256;
        // Exercise both the fully-indexed and the truncated/fallback paths.
        options.IndexSizeFactor = (seed % 3 == 0) ? 0.01 : 100.0;

        auto actual = BuildAndMatch(log, patterns, options, /*frameSize*/ 48);
        auto expected = GrepReference(log, patterns);
        Sort(expected);

        EXPECT_EQ(actual, expected) << "seed=" << seed;
    }
}

INSTANTIATE_TEST_SUITE_P(
    Formats,
    TIndexBuilderTest,
    ::testing::Values(EIndexFormat::V2, EIndexFormat::V3));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTrigrep
