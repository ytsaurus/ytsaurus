#include <yt/cpp/mapreduce/tests/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/maybe.h>

using namespace NYT;
using namespace NYT::NTesting;

////////////////////////////////////////////////////////////////////

class TTestRawReaderFixture
    : public TTestFixture
{
public:
    TTestRawReaderFixture(size_t recordCount)
    {
        GetClient()->Create(GetWorkingDir() + "/table", ENodeType::NT_TABLE);
        auto writer = GetClient()->CreateTableWriter<TNode>(
            TRichYPath(GetWorkingDir() + "/table").SortedBy("key"));
        for (size_t i = 0; i < recordCount; ++i) {
            auto r = TNode()("key", i);
            writer->AddRow(r);
            Data_.push_back(std::move(r));
        }
        writer->Finish();
    }

    const TNode::TListType& GetData() const {
        return Data_;
    }

    // Scan first occurrences of rangeIndex / rowIndex
    // Removes all control entities from the list
    static void FilterControlNodes(TNode::TListType& list, TMaybe<ui32>& rangeIndex, TMaybe<ui64>& rowIndex) {
        TNode::TListType filteredList;
        for (auto& node: list) {
            if (node.IsEntity()) {
                auto& attrs = node.GetAttributes().AsMap();
                if (!rowIndex.Defined()) {
                    if (auto p = attrs.FindPtr("row_index")) {
                        rowIndex = ui64(p->AsInt64());
                    }
                }
                if (!rangeIndex.Defined()) {
                    if (auto p = attrs.FindPtr("range_index")) {
                        rangeIndex = ui32(p->AsInt64());
                    }
                }
            } else {
                filteredList.push_back(std::move(node));
            }
        }
        list.swap(filteredList);
    }

private:
    TNode::TListType Data_;
};

////////////////////////////////////////////////////////////////////////////////

TEST(RawIo, Read)
{
    TTestRawReaderFixture testFixture(10);

    auto client = testFixture.GetClient();
    auto reader = client->CreateRawReader(testFixture.GetWorkingDir() + "/table", TFormat::YsonBinary(), TTableReaderOptions());
    auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

    TMaybe<ui32> rangeIndex;
    TMaybe<ui64> rowIndex;
    TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

    EXPECT_EQ(rangeIndex, 0u);
    EXPECT_EQ(rowIndex, 0ull);
    EXPECT_EQ(res.AsList(), testFixture.GetData());
}

TEST(RawIo, RetryBeforeRead)
{
    TTestRawReaderFixture testFixture(10);
    auto error = std::make_exception_ptr(std::runtime_error("some_error"));

    auto client = testFixture.GetClient();
    auto reader = client->CreateRawReader(testFixture.GetWorkingDir() + "/table", TFormat::YsonBinary(), TTableReaderOptions());
    {
        reader->Retry(Nothing(), Nothing(), error);
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        EXPECT_EQ(rangeIndex, 0u);
        EXPECT_EQ(rowIndex, 0ull);
        EXPECT_EQ(res.AsList(), testFixture.GetData());
    }
    {
        reader->Retry(Nothing(), 0ull, error);
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        EXPECT_EQ(rangeIndex, 0u);
        EXPECT_EQ(rowIndex, 0ull);
        EXPECT_EQ(res.AsList(), testFixture.GetData());
    }
    {
        reader->Retry(Nothing(), 5ull, error);
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        EXPECT_EQ(rangeIndex, 0u);
        EXPECT_EQ(rowIndex, 5ull);
        EXPECT_EQ(res.AsList(),
            TNode::TListType(testFixture.GetData().begin() + 5, testFixture.GetData().end()));
    }
}

TEST(RawIo, RetryAfterRead)
{
    auto error = std::make_exception_ptr(std::runtime_error("some_error"));
    TTestRawReaderFixture testFixture(10);

    auto client = testFixture.GetClient();
    auto reader = client->CreateRawReader(testFixture.GetWorkingDir() + "/table", TFormat::YsonBinary(), TTableReaderOptions());
    reader->ReadAll();
    reader->Retry(Nothing(), 9ull, error);
    auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

    TMaybe<ui32> rangeIndex;
    TMaybe<ui64> rowIndex;
    TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

    EXPECT_EQ(rangeIndex, 0u);
    EXPECT_EQ(rowIndex, 9ull);
    EXPECT_EQ(res.AsList(),
        TNode::TListType(testFixture.GetData().begin() + 9, testFixture.GetData().end()));
}

TEST(RawIo, ReadRange)
{
    TTestRawReaderFixture testFixture(10);

    auto client = testFixture.GetClient();

    TRichYPath path(testFixture.GetWorkingDir() + "/table");
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(1))
        .UpperLimit(TReadLimit().RowIndex(5)));

    auto reader = client->CreateRawReader(path, TFormat::YsonBinary(), TTableReaderOptions());
    auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

    TMaybe<ui32> rangeIndex;
    TMaybe<ui64> rowIndex;
    TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

    EXPECT_EQ(rangeIndex, 0u);
    EXPECT_EQ(rowIndex, 1ull);
    EXPECT_EQ(res.AsList(),
        TNode::TListType(testFixture.GetData().begin() + 1, testFixture.GetData().begin() + 5));
}

TEST(RawIo, RetryReadRange)
{
    TTestRawReaderFixture testFixture(20);

    auto error = std::make_exception_ptr(std::runtime_error("some_error"));

    auto client = testFixture.GetClient();

    TRichYPath path(testFixture.GetWorkingDir() + "/table");
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(1))
        .UpperLimit(TReadLimit().RowIndex(5)));
    path.AddRange(TReadRange()
        .LowerLimit(TReadLimit().RowIndex(10))
        .UpperLimit(TReadLimit().RowIndex(14)));

    auto reader = client->CreateRawReader(path, TFormat::YsonBinary(), TTableReaderOptions());
    reader->ReadAll();

    {
        reader->Retry(Nothing(), Nothing(), error);
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        EXPECT_EQ(rangeIndex, 0u);
        EXPECT_EQ(rowIndex, 1ull);
        TNode::TListType expected(testFixture.GetData().begin() + 1, testFixture.GetData().begin() + 5);
        expected.insert(expected.end(), testFixture.GetData().begin() + 10, testFixture.GetData().begin() + 14);
        EXPECT_EQ(res.AsList(), expected);
    }
    {
        reader->Retry(0, 3, error);
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        EXPECT_EQ(rangeIndex, 0u);
        EXPECT_EQ(rowIndex, 3ull);
        TNode::TListType expected(testFixture.GetData().begin() + 3, testFixture.GetData().begin() + 5);
        expected.insert(expected.end(), testFixture.GetData().begin() + 10, testFixture.GetData().begin() + 14);
        EXPECT_EQ(res.AsList(), expected);
    }
    {
        reader->Retry(1, 12, error);
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        EXPECT_EQ(rangeIndex, 0u); // Range with 1 index becames 0 after retrying
        EXPECT_EQ(rowIndex, 12ull);
        EXPECT_EQ(res.AsList(), TNode::TListType(testFixture.GetData().begin() + 12, testFixture.GetData().begin() + 14));
    }
}

TEST(RawIo, ControlAttributes)
{
    TTestRawReaderFixture testFixture(10);

    auto client = testFixture.GetClient();
    auto options = TTableReaderOptions().ControlAttributes(TControlAttributes()
        .EnableRangeIndex(false)
        .EnableRowIndex(false));
    auto reader = client->CreateRawReader(testFixture.GetWorkingDir() + "/table", TFormat::YsonBinary(), options);
    auto list = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment).AsList();
    for (auto row : list) {
        if (row.IsEntity()) {
            auto& attrs = row.GetAttributes().AsMap();
            EXPECT_TRUE(attrs.empty());
        }
    }

    TMaybe<ui32> rangeIndex;
    TMaybe<ui64> rowIndex;
    TTestRawReaderFixture::FilterControlNodes(list, rangeIndex, rowIndex);

    EXPECT_EQ(rangeIndex, TMaybe<ui32>());
    EXPECT_EQ(rowIndex, TMaybe<ui64>());
    EXPECT_EQ(list, testFixture.GetData());
}

////////////////////////////////////////////////////////////////////
