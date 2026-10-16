#include <cstddef>
#include <iterator>
#include <vector>
#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/io/io_request_slicer.h>

namespace NYT::NIO {
namespace {

////////////////////////////////////////////////////////////////////////////////

using TBufferSizes = std::vector<i64>;

template <class TRequest>
void SetFairShareContext(TRequest* request)
{
    request->FairShareSlotId = TGuid(1, 2, 3, 4);
    request->FairShareState = TIOFairShareState{
        .IOConsumed = 42,
        .IOFairShareWeight = 0.5,
    };
}

template <class TRequest>
void CheckFairShareContext(const TRequest& slice, const TRequest& request)
{
    EXPECT_EQ(slice.FairShareSlotId, request.FairShareSlotId);
    ASSERT_TRUE(slice.FairShareState);
    ASSERT_TRUE(request.FairShareState);
    EXPECT_EQ(slice.FairShareState->IOConsumed, request.FairShareState->IOConsumed);
    EXPECT_EQ(slice.FairShareState->IOFairShareWeight, request.FairShareState->IOFairShareWeight);
}

void CheckRead(i64 readRequestSize, i64 desiredSize, i64 minimalSize, const TBufferSizes& expectedSizes)
{
    auto buffer = TSharedMutableRef::Allocate(readRequestSize);
    auto request = TReadRequest{
        .Handle = New<TIOEngineHandle>(),
        .Offset = 4096,
        .Size = readRequestSize
    };
    SetFairShareContext(&request);
    auto slices = TIORequestSlicer(desiredSize, minimalSize, /*enableSlicing*/ true).Slice(request, buffer, 4096);

    EXPECT_EQ(expectedSizes.size(), slices.size());

    i64 offset = request.Offset;
    auto outputIterator = buffer.begin();

    for (int i = 0; i < std::ssize(expectedSizes); ++i) {
        const auto& slice = slices[i];

        EXPECT_EQ(slice.Request.Handle.Get(), request.Handle.Get());
        EXPECT_EQ(slice.Request.Offset, offset);
        EXPECT_EQ(slice.Request.Size, expectedSizes[i]);
        CheckFairShareContext(slice.Request, request);
        EXPECT_EQ(slice.OutputBuffer.begin(), outputIterator);
        EXPECT_EQ(slice.OutputBuffer.end(), outputIterator + expectedSizes[i]);

        offset += expectedSizes[i];
        outputIterator += expectedSizes[i];
    }
}

bool CheckSizes(const TBufferSizes& expected, const std::vector<TSharedRef>& buffers) {
    if (expected.size() != buffers.size()) {
        return false;
    }

    for (int i = 0; i < std::ssize(expected); ++i) {
        if (expected[i] != std::ssize(buffers[i])) {
            return false;
        }
    }
    return true;
}

void CheckWrite(
    const TBufferSizes& inputBufferSizes,
    i64 desiredSize,
    i64 minimalSize,
    const std::vector<TBufferSizes>& expectedSizes)
{
    auto request = TWriteRequest{
        .Handle = New<TIOEngineHandle>(),
        .Offset = 4096,
        .Flush = true
    };
    SetFairShareContext(&request);

    for (auto size : inputBufferSizes) {
        request.Buffers.push_back(TSharedMutableRef::Allocate(size));
    }

    auto slices = TIORequestSlicer(desiredSize, minimalSize, /*enableSlicing*/ true).Slice(request, 4096);
    EXPECT_EQ(expectedSizes.size(), slices.size());
    i64 offset = request.Offset;

    for (int i = 0; i < std::ssize(expectedSizes); ++i) {
        const auto& slice = slices[i];
        const auto& expected = expectedSizes[i];

        EXPECT_EQ(slice.Handle.Get(), request.Handle.Get());
        EXPECT_EQ(slice.Flush, request.Flush);
        EXPECT_EQ(slice.Offset, offset);
        CheckFairShareContext(slice, request);
        EXPECT_TRUE(CheckSizes(expected, slice.Buffers));

        offset += std::reduce(expected.begin(), expected.end());
    }
}

void CheckFlushFileRange(
    i64 requestSize,
    i64 desiredSize,
    i64 minimalSize,
    const TBufferSizes& expectedSizes)
{
    auto request = TFlushFileRangeRequest{
        .Handle = New<TIOEngineHandle>(),
        .Offset = 4096,
        .Size = requestSize
    };
    SetFairShareContext(&request);

    auto slices = TIORequestSlicer(desiredSize, minimalSize, /*enableSlicing*/ true).Slice(request, 4096);
    EXPECT_EQ(expectedSizes.size(), slices.size());
    i64 offset = request.Offset;

    for (int i = 0; i < std::ssize(expectedSizes); ++i) {
        const auto& slice = slices[i];

        EXPECT_EQ(slice.Handle.Get(), request.Handle.Get());
        EXPECT_EQ(slice.Offset, offset);
        EXPECT_EQ(slice.Size, expectedSizes[i]);
        CheckFairShareContext(slice, request);

        offset += expectedSizes[i];
    }
}

TEST(TRequestSlicerTest, ReadRequest)
{
    struct TTestCase
    {
        i64 RequestSize = 0;
        i64 DesiredSize = 0;
        i64 MinimalSize = 0;
        TBufferSizes ExpectedSizes;
    };

    const std::vector<TTestCase> TEST_CASES = {
        {
            .RequestSize = 1_MB,
            .DesiredSize = 256_KB,
            .MinimalSize = 64_KB,
            .ExpectedSizes = {256_KB, 256_KB, 256_KB, 256_KB}
        },
        {
            .RequestSize = 1_MB + 32_KB,
            .DesiredSize = 256_KB,
            .MinimalSize = 64_KB,
            .ExpectedSizes = {256_KB, 256_KB, 256_KB, 288_KB}
        },
        {
            .RequestSize = 1_MB,
            .DesiredSize = 4_GB,
            .MinimalSize = 4_GB,
            .ExpectedSizes = {1_MB}
        },
    };

    for (const auto& test : TEST_CASES) {
        CheckRead(test.RequestSize, test.DesiredSize, test.MinimalSize, test.ExpectedSizes);
    }
}

TEST(TRequestSlicerTest, WriteRequest)
{
    struct TTestCase
    {
        TBufferSizes RequestSizes;
        i64 DesiredSize = 0;
        i64 MinimalSize = 0;
        std::vector<TBufferSizes> ExpectedSlices;
    };

    const std::vector<TTestCase> TEST_CASES = {
        {
            .RequestSizes = {1_KB, 1_MB, 3_KB},
            .DesiredSize = 256_KB,
            .MinimalSize = 64_KB,
            .ExpectedSlices = {{1_KB, 255_KB}, {256_KB}, {256_KB}, {257_KB, 3_KB}}
        },
        {
            .RequestSizes = {1_MB},
            .DesiredSize = 256_KB,
            .MinimalSize = 64_KB,
            .ExpectedSlices = {{256_KB}, {256_KB}, {256_KB}, {256_KB}}
        },
        {
            .RequestSizes = {1_MB},
            .DesiredSize = 4_GB,
            .MinimalSize = 4_GB,
            .ExpectedSlices = {{1_MB}}
        },
    };

    for (const auto& test : TEST_CASES) {
        CheckWrite(test.RequestSizes, test.DesiredSize, test.MinimalSize, test.ExpectedSlices);
    }
}

TEST(TRequestSlicerTest, FlushFileRangeRequest)
{
    struct TTestCase
    {
        i64 RequestSize = 0;
        i64 DesiredSize = 0;
        i64 MinimalSize = 0;
        TBufferSizes ExpectedSizes;
    };

    const std::vector<TTestCase> TEST_CASES = {
        {
            .RequestSize = 1_MB,
            .DesiredSize = 256_KB,
            .MinimalSize = 64_KB,
            .ExpectedSizes = {256_KB, 256_KB, 256_KB, 256_KB}
        },
        {
            .RequestSize = 1_MB + 32_KB,
            .DesiredSize = 256_KB,
            .MinimalSize = 64_KB,
            .ExpectedSizes = {256_KB, 256_KB, 256_KB, 288_KB}
        },
        {
            .RequestSize = 1_MB,
            .DesiredSize = 4_GB,
            .MinimalSize = 4_GB,
            .ExpectedSizes = {1_MB}
        },
    };

    for (const auto& test : TEST_CASES) {
        CheckFlushFileRange(test.RequestSize, test.DesiredSize, test.MinimalSize, test.ExpectedSizes);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
