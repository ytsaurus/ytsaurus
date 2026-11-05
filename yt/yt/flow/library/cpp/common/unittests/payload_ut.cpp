#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/payload.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TPayloadTest, BuildSimple)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {"name" = "key"; "type" = "uint64";};
            {"name" = "value"; "type" = "string";};
            {"name" = "second_value"; "type" = "int64";};
        ]
    )""")));
    YT_VERIFY(schema);
    TPayloadBuilder builder(schema);
    builder.SetValue(NTableClient::MakeUnversionedStringValue("abc"), 1);
    builder.SetValue(NTableClient::MakeUnversionedUint64Value(5), 0);
    builder.SetValue(NTableClient::MakeUnversionedInt64Value(34), 2);
    auto result = builder.Finish();
    auto expected = TPayload(MakeCompactUnversionedOwningRow<ui64, std::string, i64>(5u, "abc", 34));
    ASSERT_EQ(expected, result);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPayloadTest, WrongType)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {"name" = "key"; "type" = "uint64";};
            {"name" = "value"; "type" = "string";};
        ]
    )""")));
    TPayloadBuilder builder(schema);
    EXPECT_THROW_WITH_SUBSTRING(
        { builder.SetValue(NTableClient::MakeUnversionedStringValue("abc"), 0); },
        "Error validating column \"key\"");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
