#include "logical_type_helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/client/table_client/logical_type.h>
#include <yt/client/table_client/proto/chunk_meta.pb.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NTableClient {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TLogicalTypeTest, TestSimplifyLogicalType)
{
    using TPair = std::pair<std::optional<ESimpleLogicalValueType>, bool>;

    EXPECT_EQ(
        SimplifyLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, true)),
        TPair(ESimpleLogicalValueType::Int64, true));

    EXPECT_EQ(
        SimplifyLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64, false)),
        TPair(ESimpleLogicalValueType::Uint64, false));

    EXPECT_EQ(
        SimplifyLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, true))),
        TPair(ESimpleLogicalValueType::Int64, false));

    EXPECT_EQ(
        SimplifyLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, false))),
        TPair(std::nullopt, false));

    EXPECT_EQ(
        SimplifyLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, true))),
        TPair(std::nullopt, true));

    EXPECT_EQ(
        SimplifyLogicalType(StructLogicalType({{"value", SimpleLogicalType(ESimpleLogicalValueType::Int64, true)}})),
        TPair(std::nullopt, true));
}

static const std::vector<TLogicalTypePtr> ComplexTypeExampleList = {
    // Simple types
    SimpleLogicalType(ESimpleLogicalValueType::Int64, true),
    SimpleLogicalType(ESimpleLogicalValueType::String, true),
    SimpleLogicalType(ESimpleLogicalValueType::Utf8, true),
    SimpleLogicalType(ESimpleLogicalValueType::Int64, false),

    // Optionals
    OptionalLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::Utf8, false)),
    OptionalLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Utf8, false))),

    // Lists
    ListLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::Utf8, true)),
    ListLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Utf8, true))),
    ListLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::String, true))),

    // Structs
    StructLogicalType({
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Utf8, true)},
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8, true))},
    }),
    StructLogicalType({
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8, true))},
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Utf8, true)},
    }),
    StructLogicalType({
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Int64, true)},
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64, true))},
    }),
};

TEST(TLogicalTypeTest, TestAllTypesAreInExamples)
{
    auto allMetatypes = TEnumTraits<ELogicalMetatype>::GetDomainValues();
    std::set<ELogicalMetatype> actualMetatypes;
    for (const auto& example : ComplexTypeExampleList) {
        actualMetatypes.insert(example->GetMetatype());
    }
    // This test checks that we have all top level metatypes in our ComplexTypeExampleList
    // and therefore all the tests that use this example list cover all all complex metatypes.
    EXPECT_EQ(actualMetatypes, std::set<ELogicalMetatype>(allMetatypes.begin(), allMetatypes.end()));
}

TEST(TLogicalTypeTest, TestAllExamplesHaveDifferentHash)
{
    std::map<int, TLogicalTypePtr> hashToType;
    auto hashFunction = THash<TLogicalType>();
    for (const auto& example : ComplexTypeExampleList) {
        auto hash = hashFunction(*example);
        auto it = hashToType.find(hash);
        if (it != hashToType.end()) {
            ADD_FAILURE() << Format(
                "Type %Qv and %Qv have the same hash %v",
                *it->second,
                *example,
                hash);
        } else {
            hashToType[hash] = example;
        }

    }
}

class TLogicalTypeTestExamples
    : public ::testing::TestWithParam<TLogicalTypePtr>
{ };

TEST_P(TLogicalTypeTestExamples, TestProtoSerialization)
{
    auto type = GetParam();

    NProto::TLogicalType proto;
    ToProto(&proto, type);

    TLogicalTypePtr deserializedType;
    FromProto(&deserializedType, proto);

    EXPECT_EQ(*type, *deserializedType);
}

TEST_P(TLogicalTypeTestExamples, TestYsonSerialization)
{
    auto type = GetParam();
    auto yson = ConvertToYsonString(type);
    auto deserializedType = ConvertTo<TLogicalTypePtr>(yson);
    EXPECT_EQ(*type, *deserializedType);
}

INSTANTIATE_TEST_CASE_P(
    Examples,
    TLogicalTypeTestExamples,
    ::testing::ValuesIn(ComplexTypeExampleList));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
