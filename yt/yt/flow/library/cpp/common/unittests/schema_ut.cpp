#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/schema.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemaTest, ConvertMessage)
{
    auto sourceSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="banner_id"; type="uint64";};
            {name="order_id"; type="uint64";};
            {name="is_click"; type="boolean";};
        ]
    )"""")));

    auto targetSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="hash"; type="uint64"; expression="farm_hash(order_id)"; sort_order="ascending";};
            {name="order_id"; type="uint64"; sort_order="ascending";};
            {name="is_click"; type="boolean";};
            {name="banner_id"; type="uint64";};
        ]
    )""")));

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(123, 0));
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(456, 1));
    builder.Payload().SetValue(NTableClient::MakeUnversionedBooleanValue(true, 2));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    auto sourceMessage = builder.Finish();

    TPayloadBuilder expectedBuilder(targetSchema);
    expectedBuilder.SetValue(NTableClient::MakeUnversionedUint64Value(2044001940267648219ull, 0));
    expectedBuilder.SetValue(NTableClient::MakeUnversionedUint64Value(456, 1));
    expectedBuilder.SetValue(NTableClient::MakeUnversionedBooleanValue(true, 2));
    expectedBuilder.SetValue(NTableClient::MakeUnversionedUint64Value(123, 3));
    auto expectedPayload = expectedBuilder.Finish();

    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    auto newMessage = ConvertMessageToNewSchema(sourceMessage, targetSchema, converterCache);
    ASSERT_EQ(sourceMessage.MessageId, newMessage.MessageId);
    ASSERT_EQ(sourceMessage.SystemTimestamp, newMessage.SystemTimestamp);
    ASSERT_EQ(sourceMessage.StreamId, newMessage.StreamId);
    ASSERT_EQ(targetSchema, newMessage.PayloadSchema);
    ASSERT_EQ(expectedPayload, newMessage.Payload);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemaTest, ValidateSchemaExpressionsValid)
{
    EXPECT_NO_THROW(ValidateSchemaExpressions(
        ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
            [
                {name="hash"; type="uint64"; required=%true; expression="farm_hash(order_id)"; sort_order="ascending";};
                {name="order_id"; type="uint64"; sort_order="ascending";};
            ]
        )""")))));
}

TEST(TSchemaTest, ValidateSchemaExpressionsColumnNotInSchema)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateSchemaExpressions(ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
            [
                {name="hash"; type="uint64"; required=%true; expression="farm_hash(uuid, message_id)"; sort_order="ascending";};
                {name="uuid"; type="string"; sort_order="ascending";};
            ]
        )""")))),
        "Invalid expression in schema");
}

TEST(TSchemaTest, ValidateSchemaExpressionsNonUint64ExpressionColumn)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateSchemaExpressions(ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
            [
                {name="hash"; type="string"; expression="farm_hash(order_id)"; sort_order="ascending";};
                {name="order_id"; type="uint64"; sort_order="ascending";};
            ]
        )""")))),
        "must have type uint64");
}

TEST(TSchemaTest, ValidateSchemaExpressionsExpressionColumnNotRequired)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateSchemaExpressions(ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
            [
                {name="hash"; type="uint64"; expression="farm_hash(order_id)"; sort_order="ascending";};
                {name="order_id"; type="uint64"; required=%true; sort_order="ascending";};
            ]
        )""")))),
        "must be required");
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStripExpressionColumnsTest, SchemaWithoutComputedColumns)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="word"; type="string"; sort_order="ascending";};
            {name="count"; type="uint64";};
        ]
    )""")));

    ASSERT_EQ(schema, StripExpressionColumns(schema));
}

TEST(TStripExpressionColumnsTest, LeadingComputedColumn)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="hash"; type="uint64"; required=%true; expression="farm_hash(word)"; sort_order="ascending";};
            {name="word"; type="string"; sort_order="ascending";};
            {name="count"; type="uint64";};
        ]
    )""")));

    auto stripped = StripExpressionColumns(schema);

    ASSERT_FALSE(stripped->HasComputedColumns());
    ASSERT_EQ(2, std::ssize(stripped->Columns()));
    EXPECT_EQ("word", stripped->Columns()[0].Name());
    EXPECT_EQ(NTableClient::ESortOrder::Ascending, stripped->Columns()[0].SortOrder());
    EXPECT_EQ("count", stripped->Columns()[1].Name());
    EXPECT_FALSE(stripped->Columns()[1].SortOrder().has_value());
}

TEST(TStripExpressionColumnsTest, NonLeadingComputedColumn)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="word"; type="string"; sort_order="ascending";};
            {name="hash"; type="uint64"; required=%true; expression="farm_hash(word)"; sort_order="ascending";};
            {name="count"; type="uint64";};
        ]
    )""")));

    auto stripped = StripExpressionColumns(schema);

    ASSERT_FALSE(stripped->HasComputedColumns());
    ASSERT_EQ(2, std::ssize(stripped->Columns()));
    EXPECT_EQ("word", stripped->Columns()[0].Name());
    EXPECT_EQ("count", stripped->Columns()[1].Name());
}

TEST(TStripExpressionColumnsTest, FlagsArePreserved)
{
    auto uniqueKeysSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        <"unique_keys"=%true;>[
            {name="hash"; type="uint64"; required=%true; expression="farm_hash(word)"; sort_order="ascending";};
            {name="word"; type="string"; sort_order="ascending";};
        ]
    )""")));

    auto strippedUniqueKeysSchema = StripExpressionColumns(uniqueKeysSchema);
    EXPECT_TRUE(strippedUniqueKeysSchema->IsUniqueKeys());
    EXPECT_TRUE(strippedUniqueKeysSchema->IsStrict());

    auto nonStrictSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        <"strict"=%false;>[
            {name="hash"; type="uint64"; required=%true; expression="farm_hash(word)"; sort_order="ascending";};
            {name="word"; type="string"; sort_order="ascending";};
        ]
    )""")));

    auto strippedNonStrictSchema = StripExpressionColumns(nonStrictSchema);
    EXPECT_FALSE(strippedNonStrictSchema->IsStrict());
    EXPECT_FALSE(strippedNonStrictSchema->IsUniqueKeys());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPayloadConverterCacheTest, ConvertWithInplaceExpression)
{
    auto sourceSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="order_id"; type="uint64";};
        ]
    )"""")));

    auto targetSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="hash"; type="uint64"; expression="farm_hash(order_id)"; sort_order="ascending";};
            {name="order_id"; type="uint64"; sort_order="ascending";};
        ]
    )""")));

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(456, 0));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    auto sourceMessage = builder.Finish();

    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    auto newMessage = ConvertMessageToNewSchema(sourceMessage, targetSchema, converterCache);

    ASSERT_EQ(NTableClient::EValueType::Uint64, newMessage.Payload.Underlying()[0].Type);
    ASSERT_EQ(NTableClient::EValueType::Uint64, newMessage.Payload.Underlying()[1].Type);
    ASSERT_EQ(456u, newMessage.Payload.Underlying()[1].Data.Uint64);
}

TEST(TPayloadConverterCacheTest, ConvertWithMissingColumn)
{
    auto sourceSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="order_id"; type="uint64";};
        ]
    )"""")));

    // Target has a column not present in source — should be filled with null.
    auto targetSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="order_id"; type="uint64";};
            {name="extra"; type="uint64";};
        ]
    )""")));

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(42, 0));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    auto sourceMessage = builder.Finish();

    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    auto newMessage = ConvertMessageToNewSchema(sourceMessage, targetSchema, converterCache);

    ASSERT_EQ(NTableClient::EValueType::Uint64, newMessage.Payload.Underlying()[0].Type);
    ASSERT_EQ(42u, newMessage.Payload.Underlying()[0].Data.Uint64);
    ASSERT_EQ(NTableClient::EValueType::Null, newMessage.Payload.Underlying()[1].Type);
}

TEST(TPayloadConverterCacheTest, SameSchemaReturnsOriginalPayload)
{
    auto schema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="order_id"; type="uint64";};
        ]
    )"""")));

    TMessageBuilder builder("stream", schema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(99, 0));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    auto sourceMessage = builder.Finish();

    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    // Same schema pointer — should return payload as-is without going through converter.
    auto newMessage = ConvertMessageToNewSchema(sourceMessage, schema, converterCache);
    ASSERT_EQ(sourceMessage.Payload, newMessage.Payload);
}

TEST(TPayloadConverterCacheTest, EquivalentSchemaReturnsOriginalPayload)
{
    static constexpr TStringBuf schemaYson = R""""(
        [
            {name="key"; type="string";};
            {name="data"; type="string";};
        ]
    )"""";
    auto sourceSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(schemaYson));
    auto targetSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(schemaYson));
    ASSERT_NE(sourceSchema.Get(), targetSchema.Get());

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue("k", 0));
    builder.Payload().SetValue(NTableClient::MakeUnversionedStringValue("d", 1));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    auto sourceMessage = builder.Finish();

    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    auto newMessage = ConvertMessageToNewSchema(sourceMessage, targetSchema, converterCache);
    // Different schema pointer, equivalent shape — Convert should short-circuit and return
    // the source payload unchanged.
    ASSERT_EQ(sourceMessage.Payload, newMessage.Payload);
    ASSERT_EQ(targetSchema, newMessage.PayloadSchema);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPayloadConverterCacheTest, EvaluatorFreeCacheConvertsToPlainTarget)
{
    auto sourceSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="order_id"; type="uint64";};
            {name="is_click"; type="boolean";};
        ]
    )""")));

    auto targetSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="is_click"; type="boolean";};
            {name="order_id"; type="uint64";};
            {name="extra"; type="uint64";};
        ]
    )""")));

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(456, 0));
    builder.Payload().SetValue(NTableClient::MakeUnversionedBooleanValue(true, 1));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    auto sourceMessage = builder.Finish();

    auto evaluatorFreeCache = CreatePayloadConverterCache(/*evaluatorCache*/ nullptr);
    auto evaluatorBackedCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());

    auto newMessage = ConvertMessageToNewSchema(sourceMessage, targetSchema, evaluatorFreeCache);
    auto referenceMessage = ConvertMessageToNewSchema(sourceMessage, targetSchema, evaluatorBackedCache);

    ASSERT_EQ(referenceMessage.Payload, newMessage.Payload);
    ASSERT_EQ(NTableClient::EValueType::Boolean, newMessage.Payload.Underlying()[0].Type);
    ASSERT_EQ(456u, newMessage.Payload.Underlying()[1].Data.Uint64);
    ASSERT_EQ(NTableClient::EValueType::Null, newMessage.Payload.Underlying()[2].Type);
}

TEST(TPayloadConverterCacheTest, EvaluatorFreeCacheRejectsComputedTarget)
{
    auto sourceSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="order_id"; type="uint64";};
        ]
    )""")));

    auto targetSchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="hash"; type="uint64"; required=%true; expression="farm_hash(order_id)"; sort_order="ascending";};
            {name="order_id"; type="uint64"; sort_order="ascending";};
        ]
    )""")));

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(NTableClient::MakeUnversionedUint64Value(456, 0));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    auto sourceMessage = builder.Finish();

    auto converterCache = CreatePayloadConverterCache(/*evaluatorCache*/ nullptr);

    EXPECT_THROW_WITH_SUBSTRING(
        ConvertMessageToNewSchema(sourceMessage, targetSchema, converterCache),
        "no column evaluator is available");
}

TEST(TPayloadConverterCacheTest, EvaluatorFreeCacheConvertsKeyToStrippedSchema)
{
    auto keySchema = ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="hash"; type="uint64"; required=%true; expression="farm_hash(word)"; sort_order="ascending";};
            {name="word"; type="string"; sort_order="ascending";};
        ]
    )""")));
    auto strippedKeySchema = StripExpressionColumns(keySchema);

    auto converterCache = CreatePayloadConverterCache(/*evaluatorCache*/ nullptr);

    auto key = MakeKey(ui64(2044001940267648219ull), TStringBuf("word"));
    auto strippedKey = ConvertKeyToSchema(key, keySchema, strippedKeySchema, converterCache);

    ASSERT_EQ(MakeKey(TStringBuf("word")), strippedKey);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemaTest, ValidateGroupBySchemaDoubleNotAllowed)
{
    EXPECT_THROW_WITH_SUBSTRING(
        ValidateGroupBySchema(ConvertTo<NTableClient::TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
            [
                {name="key"; type="string"; sort_order="ascending";};
                {name="value"; type="double"; sort_order="ascending";};
            ]
        )""")))),
        "double");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
