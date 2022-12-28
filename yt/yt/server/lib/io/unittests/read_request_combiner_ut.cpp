#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/read_request_combiner.h>

#include <util/system/fs.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

using TCombinedRequest = IReadRequestCombiner::TCombinedRequest;

////////////////////////////////////////////////////////////////////////////////

static bool operator==(const IIOEngine::TReadRequest& lhs, const IIOEngine::TReadRequest& rhs)
{
    return lhs.Offset == rhs.Offset && lhs.Size == rhs.Size &&
        lhs.Handle == rhs.Handle;
}

static bool operator==(const TCombinedRequest& lhs, const TCombinedRequest& rhs)
{
    return lhs.ReadRequest == rhs.ReadRequest;
}

static std::ostream& operator<<(std::ostream& stream, const TCombinedRequest& value)
{
    return stream << "[request: "
        << value.ReadRequest.Handle.Get()
        << " " << value.ReadRequest.Offset
        << "@" << value.ReadRequest.Size << "]";
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

template <bool Dummy>
class TReadRequestCombinerTestBase
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        FileName = GenerateRandomFileName("ReadRequestCombiner");

        TestHandles.assign({
            New<TIOEngineHandle>(FileName, WrOnly | OpenAlways),
            New<TIOEngineHandle>(FileName, WrOnly | OpenAlways),
            New<TIOEngineHandle>(FileName, WrOnly | OpenAlways)
        });
        TestHandlesDirect.assign({
            New<TIOEngineHandle>(FileName, WrOnly | OpenAlways | DirectAligned),
            New<TIOEngineHandle>(FileName, WrOnly | OpenAlways | DirectAligned)
        });

        std::sort(TestHandles.begin(), TestHandles.end());
        std::sort(TestHandlesDirect.begin(), TestHandlesDirect.end());

        NFs::Remove(FileName);
    }

    std::tuple<IReadRequestCombinerPtr, std::vector<TCombinedRequest>>
    Combine(const std::vector<IIOEngine::TReadRequest>& input)
    {
        auto combiner = Dummy ? CreateDummyReadRequestCombiner() : CreateReadRequestCombiner();
        auto combineResult = combiner->Combine(
            input,
            PageSize,
            GetRefCountedTypeCookie<IIOEngine::TDefaultReadTag>());

        return {
            std::move(combiner),
            std::move(combineResult),
        };
    }

    void RunTest(
        const std::vector<IIOEngine::TReadRequest>& input,
        const std::vector<TCombinedRequest>& output)
    {
        auto [combiner, combineResult] = Combine(input);

        EXPECT_EQ(combineResult, output);

        for (const auto& request : combineResult) {
            EXPECT_EQ(std::ssize(request.ResultBuffer), request.ReadRequest.Size);
        }

        auto outputBuffers = combiner->ReleaseOutputBuffers();
        EXPECT_EQ(outputBuffers.size(), input.size());

        for (int index = 0; index < std::ssize(outputBuffers); index++) {
            EXPECT_EQ(std::ssize(outputBuffers[index]), input[index].Size);
        }
    }

    const int PageSize = 4096;

    std::vector<TIOEngineHandlePtr> TestHandles;
    std::vector<TIOEngineHandlePtr> TestHandlesDirect;

    TString FileName;
};

using TReadRequestCombinerTest = TReadRequestCombinerTestBase<false>;

TEST_F(TReadRequestCombinerTest, CombineEmpty)
{
    RunTest({}, {});
}

TEST_F(TReadRequestCombinerTest, CombineOneHandleNoDirect)
{
    RunTest(
        {
            { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
            { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
            { .Handle = TestHandles[0], .Offset = 3000, .Size = 150 },
            { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 },
            { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 },
        },
        {
            {.ReadRequest = { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 }},
            {.ReadRequest = { .Handle = TestHandles[0], .Offset = 2048, .Size = 1124 }},
            {.ReadRequest = { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 }},
        });
}

TEST_F(TReadRequestCombinerTest, CombineOneHandleDirect)
{
    RunTest(
    {
        { .Handle = TestHandlesDirect[0], .Offset = 16000, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandlesDirect[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandlesDirect[0], .Offset = 2048, .Size = 1024 },
    },
    {
        {.ReadRequest = { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 4096 }},
        {.ReadRequest = { .Handle = TestHandlesDirect[0], .Offset = 12288, .Size = 8192 }}
    });
}

TEST_F(TReadRequestCombinerTest, CombineMultiHandlesNoDirect)
{
    RunTest(
    {
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandles[1], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 },
        { .Handle = TestHandles[2], .Offset = 3072, .Size = 100 },
    },
    {
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 }},
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 2048, .Size = 1124 }},
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 }},
        {.ReadRequest = { .Handle = TestHandles[1], .Offset = 3000, .Size = 150 }},
        {.ReadRequest = { .Handle = TestHandles[2], .Offset = 3072, .Size = 100 }},
    });
}

TEST_F(TReadRequestCombinerTest, CombineMultiHandlesDirect)
{
    RunTest(
    {
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandlesDirect[1], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 },
        { .Handle = TestHandlesDirect[0], .Offset = 16000, .Size = 500 },
        { .Handle = TestHandles[1], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandlesDirect[0], .Offset = 15000, .Size = 150 },
    },
    {
        {.ReadRequest = { .Handle = TestHandlesDirect[0], .Offset = 12288, .Size = 8192 }},
        {.ReadRequest = { .Handle = TestHandlesDirect[1], .Offset = 0, .Size = 4096 }},
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 }},
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 2048, .Size = 1124 }},
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 }},
        {.ReadRequest = { .Handle = TestHandles[1], .Offset = 3072, .Size = 100 }},
    });
}

TMutableRef SliceTail(TMutableRef buffer, int size)
{
    EXPECT_GT(std::ssize(buffer), size);
    return buffer.Slice(buffer.Size() - size, buffer.Size());
}

TEST_F(TReadRequestCombinerTest, CombineEOFOneHandle)
{
    auto [combiner, combineResult] = Combine({
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 12000, .Size = 512 },
    });

    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 2000)).IsOK() );
    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 3872)).IsOK() );

    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 4000)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[0].ResultBuffer, 4000)).IsOK() );
}

TEST_F(TReadRequestCombinerTest, CombineEOFMultiHandles)
{
    auto [combiner, combineResult] = Combine({
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 4096 },
        { .Handle = TestHandlesDirect[0], .Offset = 12000, .Size = 4096 },
        { .Handle = TestHandlesDirect[1], .Offset = 0, .Size = 512 },
        { .Handle = TestHandlesDirect[1], .Offset = 12000, .Size = 512 },
    });

    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 1)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 2000)).IsOK() );

    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[3].ResultBuffer, 2000)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[3].ResultBuffer, 4000)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[2].ResultBuffer, 4000)).IsOK() );
}

// ////////////////////////////////////////////////////////////////////////////////

using TDummyReadRequestCombinerTest = TReadRequestCombinerTestBase<true>;

TEST_F(TDummyReadRequestCombinerTest, CombineOneHandleNoDirect)
{
    RunTest(
    {
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 },
    },
    {
        { .ReadRequest = { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 }},
        { .ReadRequest = { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 }},
        { .ReadRequest = { .Handle = TestHandles[0], .Offset = 3000, .Size = 150 }},
        { .ReadRequest = { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 }},
        { .ReadRequest = { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 }}
    });
}

TEST_F(TDummyReadRequestCombinerTest, CombineOneHandleDirect)
{
    RunTest(
    {
        { .Handle = TestHandlesDirect[0], .Offset = 16000, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 100 },
    },
    {
        { .ReadRequest = { .Handle = TestHandlesDirect[0], .Offset = 12288, .Size = 8192 }},
        { .ReadRequest = { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 4096 }},
        { .ReadRequest = { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 4096 }},
    });
}

TEST_F(TDummyReadRequestCombinerTest, CombineEOFOneHandle)
{
    auto [combiner, combineResult] = Combine({
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 12000, .Size = 512 },
    });

    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 2000)).IsOK() );
    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 3872)).IsOK() );

    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 4000)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[0].ResultBuffer, 4000)).IsOK() );
}

TEST_F(TDummyReadRequestCombinerTest, CombineMultiHandlesDirect)
{
    RunTest(
    {
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandlesDirect[1], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandlesDirect[0], .Offset = 16000, .Size = 500 },
    },
    {
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 }},
        {.ReadRequest = { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 }},
        {.ReadRequest = { .Handle = TestHandlesDirect[1], .Offset = 0, .Size = 4096 }},
        {.ReadRequest = { .Handle = TestHandlesDirect[0], .Offset = 12288, .Size = 8192 }},
    });
}

TEST_F(TDummyReadRequestCombinerTest, CombineEOFMultiHandles)
{
    auto [combiner, combineResult] = Combine({
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 4096 },
        { .Handle = TestHandlesDirect[0], .Offset = 12000, .Size = 4096 },
        { .Handle = TestHandlesDirect[1], .Offset = 0, .Size = 512 },
        { .Handle = TestHandlesDirect[1], .Offset = 12000, .Size = 512 },
    });

    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 1)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[1].ResultBuffer, 2000)).IsOK() );

    EXPECT_TRUE( combiner->CheckEof(SliceTail(combineResult[3].ResultBuffer, 2000)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[3].ResultBuffer, 4000)).IsOK() );
    EXPECT_FALSE( combiner->CheckEof(SliceTail(combineResult[2].ResultBuffer, 4000)).IsOK() );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
