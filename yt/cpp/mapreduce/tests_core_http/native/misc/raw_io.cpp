#include <yt/cpp/mapreduce/tests_core_http/yt_unittest_lib/yt_unittest_lib.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/common/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

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

Y_UNIT_TEST_SUITE(RawIo)
{
    Y_UNIT_TEST(Read)
    {
        TTestRawReaderFixture testFixture(10);

        auto client = testFixture.GetClient();
        auto reader = client->CreateRawReader(testFixture.GetWorkingDir() + "/table", TFormat::YsonBinary(), TTableReaderOptions());
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(rowIndex, 0ull);
        UNIT_ASSERT_VALUES_EQUAL(res.AsList(), testFixture.GetData());
    }

    Y_UNIT_TEST(RetryBeforeRead)
    {
        TTestRawReaderFixture testFixture(10);

        auto client = testFixture.GetClient();
        auto reader = client->CreateRawReader(testFixture.GetWorkingDir() + "/table", TFormat::YsonBinary(), TTableReaderOptions());
        {
            reader->Retry(Nothing(), Nothing());
            auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

            TMaybe<ui32> rangeIndex;
            TMaybe<ui64> rowIndex;
            TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

            UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
            UNIT_ASSERT_VALUES_EQUAL(rowIndex, 0ull);
            UNIT_ASSERT_VALUES_EQUAL(res.AsList(), testFixture.GetData());
        }
        {
            reader->Retry(Nothing(), 0ull);
            auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

            TMaybe<ui32> rangeIndex;
            TMaybe<ui64> rowIndex;
            TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

            UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
            UNIT_ASSERT_VALUES_EQUAL(rowIndex, 0ull);
            UNIT_ASSERT_VALUES_EQUAL(res.AsList(), testFixture.GetData());
        }
        {
            reader->Retry(Nothing(), 5ull);
            auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

            TMaybe<ui32> rangeIndex;
            TMaybe<ui64> rowIndex;
            TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

            UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
            UNIT_ASSERT_VALUES_EQUAL(rowIndex, 5ull);
            UNIT_ASSERT_VALUES_EQUAL(res.AsList(),
                TNode::TListType(testFixture.GetData().begin() + 5, testFixture.GetData().end()));
        }
    }

    Y_UNIT_TEST(RetryAfterRead)
    {
        TTestRawReaderFixture testFixture(10);

        auto client = testFixture.GetClient();
        auto reader = client->CreateRawReader(testFixture.GetWorkingDir() + "/table", TFormat::YsonBinary(), TTableReaderOptions());
        reader->ReadAll();
        reader->Retry(Nothing(), 9ull);
        auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

        TMaybe<ui32> rangeIndex;
        TMaybe<ui64> rowIndex;
        TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

        UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(rowIndex, 9ull);
        UNIT_ASSERT_VALUES_EQUAL(res.AsList(),
            TNode::TListType(testFixture.GetData().begin() + 9, testFixture.GetData().end()));
    }

    Y_UNIT_TEST(ReadRange)
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

        UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
        UNIT_ASSERT_VALUES_EQUAL(rowIndex, 1ull);
        UNIT_ASSERT_VALUES_EQUAL(res.AsList(),
            TNode::TListType(testFixture.GetData().begin() + 1, testFixture.GetData().begin() + 5));
    }

    Y_UNIT_TEST(RetryReadRange)
    {
        TTestRawReaderFixture testFixture(20);

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
            reader->Retry(Nothing(), Nothing());
            auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

            TMaybe<ui32> rangeIndex;
            TMaybe<ui64> rowIndex;
            TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

            UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
            UNIT_ASSERT_VALUES_EQUAL(rowIndex, 1ull);
            TNode::TListType expected(testFixture.GetData().begin() + 1, testFixture.GetData().begin() + 5);
            expected.insert(expected.end(), testFixture.GetData().begin() + 10, testFixture.GetData().begin() + 14);
            UNIT_ASSERT_VALUES_EQUAL(res.AsList(), expected);
        }
        {
            reader->Retry(0, 3);
            auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

            TMaybe<ui32> rangeIndex;
            TMaybe<ui64> rowIndex;
            TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

            UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u);
            UNIT_ASSERT_VALUES_EQUAL(rowIndex, 3ull);
            TNode::TListType expected(testFixture.GetData().begin() + 3, testFixture.GetData().begin() + 5);
            expected.insert(expected.end(), testFixture.GetData().begin() + 10, testFixture.GetData().begin() + 14);
            UNIT_ASSERT_VALUES_EQUAL(res.AsList(), expected);
        }
        {
            reader->Retry(1, 12);
            auto res = NodeFromYsonString(reader->ReadAll(), ::NYson::EYsonType::ListFragment);

            TMaybe<ui32> rangeIndex;
            TMaybe<ui64> rowIndex;
            TTestRawReaderFixture::FilterControlNodes(res.AsList(), rangeIndex, rowIndex);

            UNIT_ASSERT_VALUES_EQUAL(rangeIndex, 0u); // Range with 1 index becames 0 after retrying
            UNIT_ASSERT_VALUES_EQUAL(rowIndex, 12ull);
            UNIT_ASSERT_VALUES_EQUAL(res.AsList(), TNode::TListType(testFixture.GetData().begin() + 12, testFixture.GetData().begin() + 14));
        }
    }
}

////////////////////////////////////////////////////////////////////
