#include "stdafx.h"
#include "framework.h"

#include <ytlib/new_table_client/value_consumer-mock.h>
#include <ytlib/new_table_client/table_consumer.h>
#include <ytlib/new_table_client/unversioned_row.h>

#include <core/ytree/fluent.h>

namespace NYT {
namespace NVersionedTableClient {
namespace {

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TTableConsumer, EntityAsNull)
{
    auto mock = New<StrictMock<TMockValueConsumer>>(New<TNameTable>(), true);
    EXPECT_CALL(*mock, OnBeginRow());
    EXPECT_CALL(*mock, OnValue(MakeUnversionedSentinelValue(EValueType::Null, 0)));
    EXPECT_CALL(*mock, OnEndRow());

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        consumer->OnEntity();
    consumer->OnEndMap();
}

TEST(TTableConsumer, TopLevelAttributes)
{
    auto mock = New<StrictMock<TMockValueConsumer>>(New<TNameTable>(), true);
    EXPECT_CALL(*mock, OnBeginRow());

    std::unique_ptr<IYsonConsumer> consumer(new TTableConsumer(mock));
    consumer->OnBeginMap();
        consumer->OnKeyedItem("a");
        EXPECT_THROW(consumer->OnBeginAttributes(), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NVersionedTableClient
} // namespace NYT

