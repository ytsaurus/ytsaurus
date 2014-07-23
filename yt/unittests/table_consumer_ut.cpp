#include "stdafx.h"
#include "framework.h"

#include <core/ytree/convert.h>
#include <core/ytree/yson_consumer-mock.h>

#include <ytlib/table_client/async_writer.h>
#include <ytlib/table_client/table_consumer.h>

namespace NYT {
namespace NTableClient {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

typedef std::vector< std::pair<Stroka, Stroka> > TOwnedRow;

TOwnedRow ToRow(const TRow& row)
{
    TOwnedRow ownedRow;
    for (const auto& keyValue: row) {
        ownedRow.push_back(std::make_pair(Stroka(keyValue.first), Stroka(keyValue.second)));
    }
    return ownedRow;
}

TOwnedRow ToRow(const std::vector<Stroka>& items)
{
    TOwnedRow ownedRow;
    for (int i = 0; i < items.size(); i += 2) {
        ownedRow.push_back(std::make_pair(items[i * 2], items[i * 2 + 1]));
    }
    return ownedRow;
}

TOwnedRow ToRow(const Stroka& key, const Stroka& value)
{
    TOwnedRow ownedRow;
    ownedRow.push_back(std::make_pair(key, value));
    return ownedRow;
}


class TDummyTableWriter
    : public IWriterBase
{
public:
    virtual void WriteRow(const TRow& row) override
    {
        Rows_.push_back(ToRow(row));
    }

    virtual i64 GetRowCount() const  override
    {
        return Rows_.size();
    }

    virtual NChunkClient::NProto::TDataStatistics GetDataStatistics() const override
    {
        YUNREACHABLE();
    }

    void CheckRows(const std::vector<TOwnedRow>& correctRows) const
    {
        EXPECT_EQ(correctRows, Rows_);
    }

private:
    std::vector<TOwnedRow> Rows_;
};

TEST(TTableConsumerTest, Simple)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("k1");
        consumer.OnInt64Scalar(1);
    consumer.OnEndMap();
    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("k2");
        consumer.OnStringScalar("abc");
    consumer.OnEndMap();

    std::vector<TOwnedRow> correctResult;
    correctResult.push_back(ToRow("k1", NYTree::ConvertToYsonString(1).Data()));
    correctResult.push_back(ToRow("k2", NYTree::ConvertToYsonString("abc").Data()));

    writer->CheckRows(correctResult);
}

TEST(TTableConsumerTest, TableIndex)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        consumer.OnInt64Scalar(0);
    consumer.OnEndAttributes();
    consumer.OnEntity();

    consumer.OnListItem();
    consumer.OnBeginMap();
        consumer.OnKeyedItem("k1");
        consumer.OnStringScalar("abc");
    consumer.OnEndMap();

    std::vector<TOwnedRow> correctResult;
    correctResult.push_back(ToRow("k1", NYTree::ConvertToYsonString("abc").Data()));

    writer->CheckRows(correctResult);
}

TEST(TTableConsumerTest, InvalidControlAttribute)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
    EXPECT_THROW(consumer.OnKeyedItem("invalid_attribute"), std::exception);
}

TEST(TTableConsumerTest, InvalidControlAttributeValue)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        EXPECT_THROW(consumer.OnStringScalar("0"), std::exception);
}

TEST(TTableConsumerTest, EmptyAttribute)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
    EXPECT_THROW(consumer.OnEndAttributes(), std::exception);
}

TEST(TTableConsumerTest, RowWithAttributes)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    consumer.OnBeginAttributes();
        consumer.OnKeyedItem("table_index");
        consumer.OnInt64Scalar(0);
    consumer.OnEndAttributes();
    EXPECT_THROW(consumer.OnBeginMap(), std::exception);
}

TEST(TTableConsumerTest, IntegerRow)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    EXPECT_THROW(consumer.OnInt64Scalar(10), std::exception);
}

TEST(TTableConsumerTest, EntityRow)
{
    auto writer = New<TDummyTableWriter>();
    TTableConsumer consumer(writer);

    consumer.OnListItem();
    EXPECT_THROW(consumer.OnEntity(), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
