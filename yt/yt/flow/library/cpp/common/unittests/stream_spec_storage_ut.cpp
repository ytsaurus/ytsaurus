#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage_state.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TStreamSpecStorageTest, ComputeKey)
{
    auto sourceSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="banner_id"; type="uint64";};
            {name="order_id"; type="uint64";};
            {name="is_click"; type="boolean";};
        ]
    )"""")));

    auto keySchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R"""(
        [
            {name="hash"; type="uint64"; expression="farm_hash(order_id)"; sort_order="ascending";};
            {name="order_id"; type="uint64"; sort_order="ascending";};
        ]
    )""")));

    TMessageBuilder builder("stream", sourceSchema);
    builder.Payload().SetValue(MakeUnversionedUint64Value(123, 0));
    builder.Payload().SetValue(MakeUnversionedUint64Value(456, 1));
    builder.Payload().SetValue(MakeUnversionedBooleanValue(true, 2));
    builder.SetMessageId(TMessageId(ToString(TGuid::Create())));
    builder.SetSystemTimestamp(TSystemTimestamp(1708958154));
    const auto message = builder.Finish();

    const auto state = New<TVersionedStreamSpecStorageState>();
    state->GetValue()->GroupBySchemas["cid"] = keySchema;
    auto converterCache = CreatePayloadConverterCache(CreateFastColumnEvaluatorCache());
    const auto specStorage = New<TStreamSpecStorage>(state, converterCache)->GetComputationStreamSpecStorage("cid");

    const auto key = specStorage->ComputeKey(message);

    TPayloadBuilder expectedBuilder(keySchema);
    expectedBuilder.SetValue(MakeUnversionedUint64Value(2044001940267648219ull, 0));
    expectedBuilder.SetValue(MakeUnversionedUint64Value(456, 1));

    const TKey expectedKey(expectedBuilder.Finish().Underlying());

    ASSERT_EQ(key, expectedKey);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStramSpecsTest, ThrowOnSpecDuplicate)
{
    auto schemaPtr = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="banner_id"; type="uint64";};
            {name="order_id"; type="uint64";};
            {name="is_click"; type="boolean";};
        ]
    )"""")));

    auto streamSpecsMap = THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>();
    auto originalSpec = New<TStreamSpec>();
    originalSpec->Schema = schemaPtr;
    streamSpecsMap["stream_id_1"].emplace(TStreamSpecId(0), std::move(originalSpec));
    auto duplicateSpec = New<TStreamSpec>();
    duplicateSpec->Schema = schemaPtr;
    streamSpecsMap["stream_id_2"].emplace(TStreamSpecId(1), std::move(duplicateSpec));
    EXPECT_THROW_WITH_SUBSTRING(New<TStreamSpecs>(streamSpecsMap), "Found two StreamSpec versions with same schema");
}

TEST(TStramSpecsTest, ThrowOnStreamSpecIdDuplicate)
{
    auto schemaPtr = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="banner_id"; type="uint64";};
            {name="order_id"; type="uint64";};
            {name="is_click"; type="boolean";};
        ]
    )"""")));
    auto secondSchemaPtr = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(R""""(
        [
            {name="banner_id"; type="uint64";};
            {name="order_id"; type="uint64";};
            {name="is_click"; type="boolean";};
        ]
    )"""")));

    auto streamSpecsMap = THashMap<TStreamId, TMap<TStreamSpecId, TStreamSpecPtr>>();
    auto originalSpec = New<TStreamSpec>();
    originalSpec->Schema = schemaPtr;
    streamSpecsMap["stream_id_1"].emplace(TStreamSpecId(0), std::move(originalSpec));
    auto duplicateSpec = New<TStreamSpec>();
    duplicateSpec->Schema = secondSchemaPtr;
    streamSpecsMap["stream_id_2"].emplace(TStreamSpecId(0), std::move(duplicateSpec));
    EXPECT_THROW_WITH_SUBSTRING(
        New<TStreamSpecs>(streamSpecsMap),
        "Found duplicating StreamSpecId (StreamSpecId: 0, FirstStreamId: stream_id_1, SecondStreamId: stream_id_2)");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
