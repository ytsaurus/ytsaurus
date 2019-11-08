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
using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

struct TEmptyValueConsumer
    : public IValueConsumer
{
    virtual const TNameTablePtr& GetNameTable() const override
    {
        return NameTable;
    }

    virtual const TTableSchema& GetSchema() const override
    {
        return Schema_;
    }

    virtual bool GetAllowUnknownColumns() const override
    {
        return true;
    }

    virtual void OnBeginRow() override
    { }

    virtual void OnValue(const TUnversionedValue& /*value*/) override
    { }

    virtual void OnEndRow() override
    { }

private:
    const TTableSchema Schema_ = TTableSchema();
    const TNameTablePtr NameTable = New<TNameTable>();
};

////////////////////////////////////////////////////////////////////////////////

TEST(TTableConsumer, EntityAsNull)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);
    EXPECT_CALL(mock, OnBeginRow());
    EXPECT_CALL(mock, OnMyValue(MakeUnversionedSentinelValue(EValueType::Null, 0)));
    EXPECT_CALL(mock, OnEndRow());

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(EComplexTypeMode::Positional, &mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        consumer->OnEntity();
    consumer->OnEndMap();
}

TEST(TTableConsumer, TopLevelAttributes)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);
    EXPECT_CALL(mock, OnBeginRow());

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(EComplexTypeMode::Positional, &mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        EXPECT_THROW(consumer->OnBeginAttributes(), std::exception);
}

TEST(TTableConsumer, RowAttributes)
{
    StrictMock<TMockValueConsumer> mock(New<TNameTable>(), true);

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(EComplexTypeMode::Positional, &mock));
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
        TTableConsumer consumer(EComplexTypeMode::Positional, &emptyValueConsumer);
        TYsonParser parser(&consumer, EYsonType::ListFragment);
        parser.Read("{foo=bar};");
        parser.Read("{bar=baz};YT_LOG_IN");
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

