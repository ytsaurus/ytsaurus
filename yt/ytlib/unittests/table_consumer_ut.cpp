#include <yt/core/test_framework/framework.h>

#include "table_value_consumer_mock.h"

#include <yt/client/table_client/table_consumer.h>
#include <yt/client/table_client/unversioned_row.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/yson/parser.h>

namespace NYT::NTableClient {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TEmptyValueConsumer
    : public IValueConsumer
{
    virtual const TNameTablePtr& GetNameTable() const
    {
        return NameTable;
    }

    virtual bool GetAllowUnknownColumns() const
    {
        return true;
    }

    virtual void OnBeginRow()
    { }

    virtual void OnValue(const TUnversionedValue& /*value*/)
    { }

    virtual void OnEndRow()
    { }

private:
    TNameTablePtr NameTable = New<TNameTable>();
};

////////////////////////////////////////////////////////////////////////////////

TEST(TTableConsumer, EntityAsNull)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);
    EXPECT_CALL(mock, OnBeginRow());
    EXPECT_CALL(mock, OnMyValue(MakeUnversionedSentinelValue(EValueType::Null, 0)));
    EXPECT_CALL(mock, OnEndRow());

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(&mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        consumer->OnEntity();
    consumer->OnEndMap();
}

TEST(TTableConsumer, TopLevelAttributes)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);
    EXPECT_CALL(mock, OnBeginRow());

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(&mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        EXPECT_THROW(consumer->OnBeginAttributes(), std::exception);
}

TEST(TTableConsumer, RowAttributes)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(&mock));
    consumer->OnBeginAttributes();
    consumer->OnKeyedItem("table_index");
    consumer->OnInt64Scalar(0);
    consumer->OnEndAttributes();
    EXPECT_THROW(consumer->OnBeginMap(), std::exception);
}

TEST(TYsonParserTest, ContextInExceptions_TableConsumer)
{
    try {
        TEmptyValueConsumer emptyValueConsumer;
        TTableConsumer consumer(&emptyValueConsumer);
        TYsonParser parser(&consumer, EYsonType::ListFragment);
        parser.Read("{foo=bar};");
        parser.Read("{bar=baz};LOG_IN");
        parser.Read("FO something happened");
        parser.Finish();
        GTEST_FAIL() << "Expected exception to be thrown";
    } catch (const std::exception& ex) {
        EXPECT_THAT(ex.what(), testing::HasSubstr("YT_LOG_INFO something happened"));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NVersionedTableClient

