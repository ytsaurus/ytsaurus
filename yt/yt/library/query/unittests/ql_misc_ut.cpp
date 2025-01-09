#include "ql_helpers.h"

#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>

#include <yt/yt/library/query/engine_api/top_collector.h>

#include <util/random/shuffle.h>

// Tests:
// TSelfifyEvaluatedExpressionTest
// TTopCollectorTest

namespace NYT::NQueryClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

char CompareI64(const TPIValue* first, const TPIValue* second)
{
    return first[0].Data.Int64 < second[0].Data.Int64;
}

char CompareString(const TPIValue* first, const TPIValue* second)
{
    return first[0].AsStringBuf() < second[0].AsStringBuf();
}

TEST(TTopCollectorTest, Simple)
{
    const int dataLength = 100;
    const int resultLength = 20;

    auto data = std::vector<TPIValue>(dataLength);
    for (int i = 0; i < dataLength; ++i) {
        data[i].Type = EValueType::Int64;
        data[i].Data.Int64 = dataLength - i - 1;
    }

    auto topCollector = TTopCollector(
        resultLength,
        NWebAssembly::PrepareFunction(&CompareI64),
        /*rowSize*/ 1,
        GetDefaultMemoryChunkProvider());

    for (int i = 0; i < dataLength; ++i) {
        topCollector.AddRow(&data[i]);
    }

    auto sortResult = topCollector.GetRows();
    for (int i = 0; i < resultLength; ++i) {
        EXPECT_EQ(sortResult[i][0].Type, EValueType::Int64);
        EXPECT_EQ(sortResult[i][0].Data.Int64, i);
    }
}

TEST(TTopCollectorTest, Shuffle)
{
    const int dataLength = 2000;
    const int resultLength = 100;

    auto data = std::vector<TValue>(dataLength);
    for (int i = 0; i < dataLength; i += 2) {
        data[i].Type = EValueType::Int64;
        data[i].Data.Int64 = i;
        data[i+1].Type = EValueType::Int64;
        data[i+1].Data.Int64 = i;
    }

    for (int i = 0; i < 1000; ++i) {
        Shuffle(data.begin(), data.end());

        auto topCollector = TTopCollector(
            resultLength,
            NWebAssembly::PrepareFunction(&CompareI64),
            /*rowSize*/ 1,
            GetDefaultMemoryChunkProvider());

        for (int i = 0; i < dataLength; ++i) {
            topCollector.AddRow(std::bit_cast<TPIValue*>(&data[i]));
        }

        auto sortResult = topCollector.GetRows();
        for (int i = 0; i < resultLength; i += 2) {
            EXPECT_EQ(sortResult[i][0].Type, EValueType::Int64);
            EXPECT_EQ(sortResult[i][0].Data.Int64, i);
            EXPECT_EQ(sortResult[i+1][0].Type, EValueType::Int64);
            EXPECT_EQ(sortResult[i+1][0].Data.Int64, i);
        }
    }
}

TEST(TTopCollectorTest, LargeRow)
{
    i64 dataLength = 1;

    auto topCollector = TTopCollector(
        2,
        NWebAssembly::PrepareFunction(&CompareString),
        /*rowSize*/ 1,
        GetDefaultMemoryChunkProvider());

    auto largeString = TString();
    largeString.resize(600_KB);

    auto data = std::vector<TPIValue>(dataLength);
    MakePositionIndependentStringValue(&data[0], largeString);

    ::memset(std::bit_cast<void*>(largeString.begin()), 'c', 600_KB);
    topCollector.AddRow(&data[0]);

    ::memset(std::bit_cast<void*>(largeString.begin()), 'b', 600_KB);
    topCollector.AddRow(&data[0]);

    ::memset(std::bit_cast<void*>(largeString.begin()), 'a', 600_KB);
    topCollector.AddRow(&data[0]);

    auto sortResult = topCollector.GetRows();

    EXPECT_EQ(sortResult[0][0].AsStringBuf()[0], 'a');
    EXPECT_EQ(sortResult[1][0].AsStringBuf()[0], 'b');
}

////////////////////////////////////////////////////////////////////////////////

struct TVectorOverMemoryChunkProviderTestBufferTag
{ };

TEST(TVectorOverMemoryChunkProviderTest, Simple)
{
    const int length = 100'000;

    auto vector = TVectorOverMemoryChunkProvider<int>(
        GetRefCountedTypeCookie<TVectorOverMemoryChunkProviderTestBufferTag>(),
        GetDefaultMemoryChunkProvider());

    for (int i = 0; i < length; ++i) {
        vector.PushBack(i);
    }

    for (int i = 0; i < length; ++i) {
        EXPECT_EQ(i, vector[i]);
    }
}

TEST(TVectorOverMemoryChunkProviderTest, Copy)
{
    const int length = 100'000;

    auto vector = TVectorOverMemoryChunkProvider<int>(
        GetRefCountedTypeCookie<TVectorOverMemoryChunkProviderTestBufferTag>(),
        GetDefaultMemoryChunkProvider());

    for (int i = 0; i < length; ++i) {
        vector.PushBack(i);
    }

    for (int i = 0; i < length; ++i) {
        EXPECT_EQ(i, vector[i]);
    }

    auto copy = vector;

    for (int i = 0; i < length; ++i) {
        EXPECT_EQ(i, copy[i]);
    }
}

TEST(TVectorOverMemoryChunkProviderTest, Empty)
{
    auto vector = TVectorOverMemoryChunkProvider<int>(
        GetRefCountedTypeCookie<TVectorOverMemoryChunkProviderTestBufferTag>(),
        GetDefaultMemoryChunkProvider());

    EXPECT_TRUE(vector.Empty());
}

TEST(TVectorOverMemoryChunkProviderTest, CopyEmpty)
{
    auto vector = TVectorOverMemoryChunkProvider<int>(
        GetRefCountedTypeCookie<TVectorOverMemoryChunkProviderTestBufferTag>(),
        GetDefaultMemoryChunkProvider());

    auto copy = vector;

    EXPECT_TRUE(copy.Empty());
}

TEST(TVectorOverMemoryChunkProviderTest, Append)
{
    const int appends = 61;
    for (int length = 0; length < 10'000; length += 61) {
        auto vector = TVectorOverMemoryChunkProvider<int>(
            GetRefCountedTypeCookie<TVectorOverMemoryChunkProviderTestBufferTag>(),
            GetDefaultMemoryChunkProvider());

        for (int i = 0; i < length; ++i) {
            vector.PushBack(i);
        }

        for (int i = 0; i < appends; ++i) {
            vector.Append({0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        }

        for (int i = 0; i < length; ++i) {
            EXPECT_EQ(i, vector[i]);
        }

        for (int i = 0; i < appends; ++i) {
            for (int j = 0; j < 10; ++j) {
                EXPECT_EQ(j, vector[length + (i * 10) + j]);
            }
        }
    }
}

TEST(TVectorOverMemoryChunkProviderTest, AppendHuge)
{
    const int appends = 2;
    const int appendLength = 10'000;
    auto append = std::vector<int>();
    for (int i = 0; i < appendLength; ++i) {
        append.push_back(i);
    }

    for (int length = 0; length < 10'000; length += 1021) {
        auto vector = TVectorOverMemoryChunkProvider<int>(
            GetRefCountedTypeCookie<TVectorOverMemoryChunkProviderTestBufferTag>(),
            GetDefaultMemoryChunkProvider());

        for (int i = 0; i < length; ++i) {
            vector.PushBack(i);
        }

        for (int i = 0; i < appends; ++i) {
            vector.Append(append);
        }

        for (int i = 0; i < length; ++i) {
            EXPECT_EQ(i, vector[i]);
        }

        for (int i = 0; i < appends; ++i) {
            for (int j = 0; j < appendLength; ++j) {
                EXPECT_EQ(j, vector[length + (i * appendLength) + j]);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NQueryClient
