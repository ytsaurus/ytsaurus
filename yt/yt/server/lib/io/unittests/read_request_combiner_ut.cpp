#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/read_request_combiner.h>

#include <util/system/fs.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

static bool operator==(const TReadRequestCombiner::TIORequest& lhs, const TReadRequestCombiner::TIORequest& rhs)
{
    return lhs.Handle == rhs.Handle && lhs.Offset == rhs.Offset && lhs.Size == rhs.Size;
}

static std::ostream& operator<<(std::ostream& stream, const TReadRequestCombiner::TIORequest& value)
{
    if (value.Handle) {
        return stream << "[request: " << static_cast<FHANDLE>(*value.Handle) << " "
            << value.Offset << "@" << value.Size << "]";
    } else {
        return stream << "[request: <null> " << value.Offset << "@" << value.Size << "]";
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadRequestCombinerTest
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

    TReadRequestCombiner Combine(const std::vector<IIOEngine::TReadRequest>& input)
    {
        TReadRequestCombiner combiner;
        combiner.Combine(
            input,
            PageSize,
            NYTAlloc::EMemoryZone::Normal,
            GetRefCountedTypeCookie<IIOEngine::TDefaultReadTag>());

        return std::move(combiner);
    }

    void RunTest(
        const std::vector<IIOEngine::TReadRequest>& input,
        const std::vector<TReadRequestCombiner::TIORequest>& output)
    {
        auto combiner = Combine(input);

        auto ioRequests = combiner.GetIORequests();
        EXPECT_EQ(ioRequests, output);

        for (const auto& request: ioRequests) {
            EXPECT_EQ(std::ssize(request.ResultBuffer), request.Size);
        }

        auto outputBuffers = combiner.GetOutputBuffers();
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

TEST_F(TReadRequestCombinerTest, CombineEmpty)
{
    RunTest({}, {});
}

TEST_F(TReadRequestCombinerTest, CombineOneHandleNoDirect)
{
    RunTest({
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 },
    }, {
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1124 },
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
    });
}

TEST_F(TReadRequestCombinerTest, CombineOneHandleDirect)
{
    RunTest({
        { .Handle = TestHandlesDirect[0], .Offset = 16000, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandlesDirect[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandlesDirect[0], .Offset = 2048, .Size = 1024 },
    }, {
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 4096 },
        { .Handle = TestHandlesDirect[0], .Offset = 12288, .Size = 8192 },
    });
}

TEST_F(TReadRequestCombinerTest, CombineMultiHandlesNoDirect)
{
    RunTest({
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandles[1], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 },
        { .Handle = TestHandles[2], .Offset = 3072, .Size = 100 },
    }, {
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1124 },
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[1], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandles[2], .Offset = 3072, .Size = 100 },
    });
}

TEST_F(TReadRequestCombinerTest, CombineMultiHandlesDirect)
{
    RunTest({
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandlesDirect[1], .Offset = 3000, .Size = 150 },
        { .Handle = TestHandles[0], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1024 },
        { .Handle = TestHandlesDirect[0], .Offset = 16000, .Size = 500 },
        { .Handle = TestHandles[1], .Offset = 3072, .Size = 100 },
        { .Handle = TestHandlesDirect[0], .Offset = 15000, .Size = 150 },
    }, {
        { .Handle = TestHandlesDirect[0], .Offset = 12288, .Size = 8192 },
        { .Handle = TestHandlesDirect[1], .Offset = 0, .Size = 4096 },
        { .Handle = TestHandles[0], .Offset = 1024, .Size = 512 },
        { .Handle = TestHandles[0], .Offset = 2048, .Size = 1124 },
        { .Handle = TestHandles[0], .Offset = 9000, .Size = 512 },
        { .Handle = TestHandles[1], .Offset = 3072, .Size = 100 },
    });
}

TMutableRef SliceTail(TMutableRef buffer, int size)
{
    EXPECT_GT(std::ssize(buffer), size);
    return buffer.Slice(buffer.Size() - size, buffer.Size());
}

TEST_F(TReadRequestCombinerTest, CombineEOFOneHandle)
{
    auto combiner = Combine({
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 512 },
        { .Handle = TestHandlesDirect[0], .Offset = 12000, .Size = 512 },
    });

    auto requests = combiner.GetIORequests();

    EXPECT_TRUE( combiner.CheckEOF(SliceTail(requests[1].ResultBuffer, 2000)).IsOK() );
    EXPECT_TRUE( combiner.CheckEOF(SliceTail(requests[1].ResultBuffer, 3872)).IsOK() );

    EXPECT_FALSE( combiner.CheckEOF(SliceTail(requests[1].ResultBuffer, 4000)).IsOK() );
    EXPECT_FALSE( combiner.CheckEOF(SliceTail(requests[0].ResultBuffer, 4000)).IsOK() );
}

TEST_F(TReadRequestCombinerTest, CombineEOFMultiHandles)
{
    auto combiner = Combine({
        { .Handle = TestHandlesDirect[0], .Offset = 0, .Size = 4096 },
        { .Handle = TestHandlesDirect[0], .Offset = 12000, .Size = 4096 },
        { .Handle = TestHandlesDirect[1], .Offset = 0, .Size = 512 },
        { .Handle = TestHandlesDirect[1], .Offset = 12000, .Size = 512 },
    });

    auto requests = combiner.GetIORequests();

    EXPECT_TRUE( combiner.CheckEOF(SliceTail(requests[1].ResultBuffer, 1)).IsOK() );
    EXPECT_FALSE( combiner.CheckEOF(SliceTail(requests[1].ResultBuffer, 2000)).IsOK() );

    EXPECT_TRUE( combiner.CheckEOF(SliceTail(requests[3].ResultBuffer, 2000)).IsOK() );
    EXPECT_FALSE( combiner.CheckEOF(SliceTail(requests[3].ResultBuffer, 4000)).IsOK() );
    EXPECT_FALSE( combiner.CheckEOF(SliceTail(requests[2].ResultBuffer, 4000)).IsOK() );
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIO
