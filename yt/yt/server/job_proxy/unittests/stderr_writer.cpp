#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/server/job_proxy/stderr_writer.h>

namespace NYT::NJobProxy {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TStderrWriterTest, TestPagedLog)
{
    TStderrWriter writer(1000);
    TStringStream reference;

    for (size_t i = 0; i <= 100; ++i) {
        const auto str = ToString(i) + "\n";
        writer << str;
        reference << str;
    }

    {
        const auto lastByte = static_cast<decltype(NApi::TPagedLog::TotalSize)>(reference.Str().size());
        {
            const auto data = writer.GetCurrentData({});
            ASSERT_EQ(data.Data, reference.Str());
            ASSERT_EQ(data.TotalSize, static_cast<decltype(data.TotalSize)>(reference.Str().size()));
        }

        {
            const auto data = writer.GetCurrentData({.Limit = 123});
            ASSERT_EQ(data.Data, reference.Str().substr(0, 123));
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            const auto data = writer.GetCurrentData({.Offset = -50});
            ASSERT_EQ(data.Data, reference.Str().substr(reference.Str().size() - 50, 50));
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            // before start
            const auto data = writer.GetCurrentData({.Offset = -50000});
            ASSERT_EQ(data.Data, reference.Str());
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            // Requested more than have
            const auto data = writer.GetCurrentData({.Limit = 100, .Offset = 250});
            ASSERT_EQ(data.Data.size(), size_t(44));
            ASSERT_TRUE(data.Data.EndsWith("100\n"));
            ASSERT_EQ(data.EndOffset, lastByte);
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            // Range after end
            const auto data = writer.GetCurrentData({.Limit = 123, .Offset = 300});
            ASSERT_EQ(data.Data, "");
            ASSERT_EQ(data.EndOffset, 0);
            ASSERT_EQ(data.TotalSize, lastByte);
        }
    }

    // Reach ..skipped..
    for (size_t i = 100; i <= 1000; ++i) {
        const auto str = ToString(i) + "\n";
        writer << str;
        reference << str;
    }

    {
        const auto data = writer.GetCurrentData({});
        ASSERT_EQ(data.TotalSize, static_cast<decltype(data.TotalSize)>(reference.Str().size()));
    }

    for (size_t i = 1000; i <= 2000; ++i) {
        const auto str = ToString(i) + "\n";
        writer << str;
        reference << str;
    }

    const auto lastByte = static_cast<decltype(NApi::TPagedLog::TotalSize)>(reference.Str().size());

    {
        // Full log requested
        const auto data = writer.GetCurrentData({});
        ASSERT_EQ(data.TotalSize, lastByte);
        ASSERT_EQ(data.EndOffset, lastByte);
    }

    {
        // 0..123 requested
        // BUG/TODO to simplify logic we use only tail - but tail does not contain requested data
        const auto data = writer.GetCurrentData({.Limit = 123});
        ASSERT_EQ(data.Data, "");
        ASSERT_EQ(data.EndOffset, 123);
        ASSERT_EQ(data.TotalSize, lastByte);
    }

    {
        // last 50 bytes requested
        const auto data = writer.GetCurrentData({.Offset = -50});
        ASSERT_EQ(data.Data.size(), size_t(50));
        ASSERT_TRUE(data.Data.EndsWith("2000\n"));
        ASSERT_EQ(data.EndOffset, lastByte);
        ASSERT_EQ(data.TotalSize, lastByte);
    }

    {
        // before start
        const auto data = writer.GetCurrentData({.Offset = -50000});
        ASSERT_TRUE(data.Data.StartsWith("1901\n"));
        ASSERT_TRUE(data.Data.EndsWith("2000\n"));
        ASSERT_EQ(data.TotalSize, lastByte);
    }

    {
        const auto data = writer.GetCurrentData({.Limit = 50, .Offset = 8850});
        // DUMP("d", data.Data, data.EndOffset, data.TotalSize);
        ASSERT_EQ(data.Data.size(), size_t(50));
        ASSERT_TRUE(data.Data.EndsWith("1999\n2"));
        ASSERT_EQ(data.EndOffset, 8900);
        ASSERT_EQ(data.TotalSize, lastByte);
    }

    {
        // Requested more than have
        const auto data = writer.GetCurrentData({.Limit = 100, .Offset = 8850});
        ASSERT_EQ(data.Data.size(), size_t(54));
        ASSERT_TRUE(data.Data.EndsWith("2000\n"));
        ASSERT_EQ(data.EndOffset, lastByte);
        ASSERT_EQ(data.TotalSize, lastByte);
    }

    {
        // Requested pos before tail start (8404)
        const auto data = writer.GetCurrentData({.Limit = 100, .Offset = 8350});
        ASSERT_EQ(data.Data.size(), size_t(46));
        ASSERT_TRUE(data.Data.EndsWith("1909\n1"));
        ASSERT_TRUE(data.Data.StartsWith("1901\n"));
        ASSERT_EQ(data.EndOffset, 8450);
        ASSERT_EQ(data.TotalSize, lastByte);
    }
}

TEST(TStderrWriterTest, TestPagedLogOneBuffer)
{
    TStringStream reference;

    for (size_t i = 0; i <= 100; ++i) {
        const auto str = ToString(i) + "\n";
        reference << str;
    }

    {
        const auto lastByte = static_cast<decltype(NApi::TPagedLog::TotalSize)>(reference.Str().size());
        {
            const auto data = NApi::TPagedLog::PagedLogFromReq({}, reference.Str());
            ASSERT_EQ(data.Data, reference.Str());
            ASSERT_EQ(data.TotalSize, static_cast<decltype(data.TotalSize)>(reference.Str().size()));
        }

        {
            const auto data = NApi::TPagedLog::PagedLogFromReq({.Limit = 123}, reference.Str());
            ASSERT_EQ(data.Data, reference.Str().substr(0, 123));
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            const auto data = NApi::TPagedLog::PagedLogFromReq({.Offset = -50}, reference.Str());
            ASSERT_EQ(data.Data, reference.Str().substr(reference.Str().size() - 50, 50));
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            // before start
            const auto data = NApi::TPagedLog::PagedLogFromReq({.Offset = -50000}, reference.Str());
            ASSERT_EQ(data.Data, reference.Str());
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            // Requested more than have
            const auto data = NApi::TPagedLog::PagedLogFromReq({.Limit = 100, .Offset = 250}, reference.Str());
            ASSERT_EQ(data.Data.size(), size_t(44));
            ASSERT_TRUE(data.Data.EndsWith("100\n"));
            ASSERT_EQ(data.EndOffset, lastByte);
            ASSERT_EQ(data.TotalSize, lastByte);
        }

        {
            // Range after end
            const auto data = NApi::TPagedLog::PagedLogFromReq({.Limit = 123, .Offset = 300}, reference.Str());
            ASSERT_EQ(data.Data, "");
            ASSERT_EQ(data.EndOffset, 0);
            ASSERT_EQ(data.TotalSize, lastByte);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NJobProxy
