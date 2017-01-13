#include "framework.h"

#include "table_value_consumer_mock.h"

#include <yt/ytlib/table_client/table_consumer.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTableClient {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

using namespace NYTree;
using namespace NYson;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT

