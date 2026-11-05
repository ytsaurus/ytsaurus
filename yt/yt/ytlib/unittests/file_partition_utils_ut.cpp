#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/ytlib/file_client/partition_utils.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFileClient {
namespace {

using NChunkClient::NProto::TChunkSpec;
using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

//! Builds synthetic chunk specs with given uncompressed sizes;
//! range_index is used as a chunk identity marker.
std::vector<TChunkSpec> MakeChunkSpecs(const std::vector<i64>& sizes)
{
    std::vector<TChunkSpec> chunkSpecs;
    chunkSpecs.reserve(sizes.size());
    for (int index = 0; index < std::ssize(sizes); ++index) {
        auto& chunkSpec = chunkSpecs.emplace_back();
        chunkSpec.set_range_index(index);

        TMiscExt miscExt;
        miscExt.set_uncompressed_data_size(sizes[index]);
        SetProtoExtension(chunkSpec.mutable_chunk_meta()->mutable_extensions(), miscExt);
    }
    return chunkSpecs;
}

std::optional<i64> GetLowerOffset(const TChunkSpec& chunkSpec)
{
    if (chunkSpec.has_lower_limit() && chunkSpec.lower_limit().has_offset()) {
        return chunkSpec.lower_limit().offset();
    }
    return std::nullopt;
}

std::optional<i64> GetUpperOffset(const TChunkSpec& chunkSpec)
{
    if (chunkSpec.has_upper_limit() && chunkSpec.upper_limit().has_offset()) {
        return chunkSpec.upper_limit().offset();
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TFilePartitionUtilsTest, CumulativeChunkSizes)
{
    EXPECT_EQ(
        BuildCumulativeChunkSizes(MakeChunkSpecs({40, 40, 40})),
        (std::vector<i64>{0, 40, 80, 120}));
    EXPECT_EQ(
        BuildCumulativeChunkSizes(MakeChunkSpecs({})),
        (std::vector<i64>{0}));
}

TEST(TFilePartitionUtilsTest, WholeFile)
{
    auto chunkSpecs = MakeChunkSpecs({40, 40, 40});
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);

    auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, 0, 120);
    ASSERT_EQ(std::ssize(sliced), 3);
    for (int index = 0; index < 3; ++index) {
        EXPECT_EQ(sliced[index].range_index(), index);
        EXPECT_EQ(GetLowerOffset(sliced[index]), std::nullopt);
        EXPECT_EQ(GetUpperOffset(sliced[index]), std::nullopt);
    }
}

TEST(TFilePartitionUtilsTest, InsideSingleChunk)
{
    auto chunkSpecs = MakeChunkSpecs({40, 40, 40});
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);

    auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, 10, 20);
    ASSERT_EQ(std::ssize(sliced), 1);
    EXPECT_EQ(sliced[0].range_index(), 0);
    EXPECT_EQ(GetLowerOffset(sliced[0]), std::optional<i64>(10));
    EXPECT_EQ(GetUpperOffset(sliced[0]), std::optional<i64>(20));
}

TEST(TFilePartitionUtilsTest, ChunkAligned)
{
    auto chunkSpecs = MakeChunkSpecs({40, 40, 40});
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);

    auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, 40, 80);
    ASSERT_EQ(std::ssize(sliced), 1);
    EXPECT_EQ(sliced[0].range_index(), 1);
    EXPECT_EQ(GetLowerOffset(sliced[0]), std::nullopt);
    EXPECT_EQ(GetUpperOffset(sliced[0]), std::nullopt);
}

TEST(TFilePartitionUtilsTest, CrossingChunkBoundaries)
{
    auto chunkSpecs = MakeChunkSpecs({40, 40, 40});
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);

    auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, 30, 90);
    ASSERT_EQ(std::ssize(sliced), 3);
    EXPECT_EQ(GetLowerOffset(sliced[0]), std::optional<i64>(30));
    EXPECT_EQ(GetUpperOffset(sliced[0]), std::nullopt);
    EXPECT_EQ(GetLowerOffset(sliced[1]), std::nullopt);
    EXPECT_EQ(GetUpperOffset(sliced[1]), std::nullopt);
    EXPECT_EQ(GetLowerOffset(sliced[2]), std::nullopt);
    EXPECT_EQ(GetUpperOffset(sliced[2]), std::optional<i64>(10));
}

TEST(TFilePartitionUtilsTest, EndAtEof)
{
    auto chunkSpecs = MakeChunkSpecs({40, 40, 40});
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);

    auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, 100, 120);
    ASSERT_EQ(std::ssize(sliced), 1);
    EXPECT_EQ(sliced[0].range_index(), 2);
    EXPECT_EQ(GetLowerOffset(sliced[0]), std::optional<i64>(20));
    EXPECT_EQ(GetUpperOffset(sliced[0]), std::nullopt);
}

TEST(TFilePartitionUtilsTest, EmptyRanges)
{
    auto chunkSpecs = MakeChunkSpecs({40, 40, 40});
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);

    for (i64 offset : {0, 20, 40, 120}) {
        auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, offset, offset);
        EXPECT_TRUE(sliced.empty()) << "offset = " << offset;
    }
}

TEST(TFilePartitionUtilsTest, ZeroSizedChunk)
{
    auto chunkSpecs = MakeChunkSpecs({40, 0, 40});
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);
    EXPECT_EQ(cumulativeChunkSizes, (std::vector<i64>{0, 40, 40, 80}));

    // A zero-sized chunk at range begin is skipped entirely.
    {
        auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, 40, 50);
        ASSERT_EQ(std::ssize(sliced), 1);
        EXPECT_EQ(sliced[0].range_index(), 2);
        EXPECT_EQ(GetLowerOffset(sliced[0]), std::nullopt);
        EXPECT_EQ(GetUpperOffset(sliced[0]), std::optional<i64>(10));
    }

    // A zero-sized chunk strictly inside the range is emitted without limits;
    // it is harmless as it contains no blocks.
    {
        auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, 30, 70);
        ASSERT_EQ(std::ssize(sliced), 3);
        EXPECT_EQ(GetLowerOffset(sliced[0]), std::optional<i64>(30));
        EXPECT_EQ(GetLowerOffset(sliced[1]), std::nullopt);
        EXPECT_EQ(GetUpperOffset(sliced[1]), std::nullopt);
        EXPECT_EQ(GetUpperOffset(sliced[2]), std::optional<i64>(30));
    }
}

//! For every possible [begin, end) pair checks that the sliced specs are
//! non-degenerate and cover exactly the requested byte range.
TEST(TFilePartitionUtilsTest, ExhaustiveCoverageOracle)
{
    const std::vector<i64> sizes{3, 5, 7};
    auto chunkSpecs = MakeChunkSpecs(sizes);
    auto cumulativeChunkSizes = BuildCumulativeChunkSizes(chunkSpecs);
    auto fileLength = cumulativeChunkSizes.back();

    for (i64 begin = 0; begin <= fileLength; ++begin) {
        for (i64 end = begin; end <= fileLength; ++end) {
            auto sliced = SliceFileChunkSpecs(chunkSpecs, cumulativeChunkSizes, begin, end);

            if (begin == end) {
                EXPECT_TRUE(sliced.empty());
                continue;
            }

            auto coverageCursor = begin;
            for (const auto& spec : sliced) {
                auto chunkIndex = spec.range_index();
                auto chunkBegin = cumulativeChunkSizes[chunkIndex];

                auto sliceBegin = chunkBegin + GetLowerOffset(spec).value_or(0);
                auto sliceEnd = chunkBegin + GetUpperOffset(spec).value_or(sizes[chunkIndex]);

                // Non-degenerate unless the chunk itself is empty.
                if (sizes[chunkIndex] > 0) {
                    EXPECT_LT(sliceBegin, sliceEnd)
                        << "begin = " << begin << ", end = " << end << ", chunk = " << chunkIndex;
                }

                // Slices are contiguous and in order.
                EXPECT_EQ(sliceBegin, coverageCursor)
                    << "begin = " << begin << ", end = " << end << ", chunk = " << chunkIndex;
                coverageCursor = sliceEnd;
            }
            EXPECT_EQ(coverageCursor, end)
                << "begin = " << begin << ", end = " << end;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFileClient
