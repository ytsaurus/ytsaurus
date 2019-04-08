#include <yt/core/test_framework/framework.h>

#include <yt/core/test_framework/yson_consumer_mock.h>

#include <yt/client/table_client/schema.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;

using ::testing::InSequence;
using ::testing::StrictMock;
using ::testing::NiceMock;

////////////////////////////////////////////////////////////////////////////////

TEST(TTableSchemaTest, TestEqualIgnoringRequiredness)
{
    TTableSchema schema1 = TTableSchema({
        TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::Int64, true)),
    });

    TTableSchema schema2 = TTableSchema({
        TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::Int64, false)),
    });

    TTableSchema schema3 = TTableSchema({
        TColumnSchema("foo", SimpleLogicalType(ESimpleLogicalValueType::String, true)),
    });

    EXPECT_TRUE(schema1 != schema2);
    EXPECT_TRUE(IsEqualIgnoringRequiredness(schema1, schema2));
    EXPECT_FALSE(IsEqualIgnoringRequiredness(schema1, schema3));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
