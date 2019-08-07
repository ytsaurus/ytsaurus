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

#define EXPECT_EXCEPTION_WITH_MESSAGE(expr, message) \
    do { \
        try { \
            expr; \
            ADD_FAILURE() << "expected to throw"; \
        } catch (const std::exception& ex) { \
            EXPECT_THAT(ex.what(), testing::HasSubstr(message)); \
        } \
    } while (0)

TEST(TLogicalTypeTest, TestSimplifyLogicalType)
{
    using TPair = std::pair<std::optional<ESimpleLogicalValueType>, bool>;

    EXPECT_EQ(
        SimplifyLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        TPair(ESimpleLogicalValueType::Int64, true));

    EXPECT_EQ(
        SimplifyLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Uint64))),
        TPair(ESimpleLogicalValueType::Uint64, false));

    EXPECT_EQ(
        SimplifyLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
        TPair(ESimpleLogicalValueType::Int64, false));

    EXPECT_EQ(
        SimplifyLogicalType(OptionalLogicalType(OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)))),
        TPair(std::nullopt, false));

    EXPECT_EQ(
        SimplifyLogicalType(ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))),
        TPair(std::nullopt, true));

    EXPECT_EQ(
        SimplifyLogicalType(StructLogicalType({{"value", SimpleLogicalType(ESimpleLogicalValueType::Int64)}})),
        TPair(std::nullopt, true));

    EXPECT_EQ(
        SimplifyLogicalType(TupleLogicalType({
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::Uint64)
        })),
        TPair(std::nullopt, true));
}

static const std::vector<TLogicalTypePtr> ComplexTypeExampleList = {
    // Simple types
    SimpleLogicalType(ESimpleLogicalValueType::Int64),
    SimpleLogicalType(ESimpleLogicalValueType::String),
    SimpleLogicalType(ESimpleLogicalValueType::Utf8),
    OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),

    // Optionals
    OptionalLogicalType(
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))),
    OptionalLogicalType(
        ListLogicalType(
            OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)))),

    // Lists
    ListLogicalType(
        SimpleLogicalType(ESimpleLogicalValueType::Utf8)),
    ListLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::Utf8))),
    ListLogicalType(
        ListLogicalType(
            SimpleLogicalType(ESimpleLogicalValueType::String))),

    // Structs
    StructLogicalType({
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Utf8)},
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))},
    }),
    StructLogicalType({
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8))},
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Utf8)},
    }),
    StructLogicalType({
        {"key", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        {"value", ListLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64))},
    }),

    // Tuples
    TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
    }),
    TupleLogicalType({
        OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Int64)),
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
    }),
    TupleLogicalType({
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
        SimpleLogicalType(ESimpleLogicalValueType::Int64),
    }),

    // VariantTuple
    VariantTupleLogicalType(
        {
            SimpleLogicalType(ESimpleLogicalValueType::String),
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
        }),
    VariantTupleLogicalType(
        {
            SimpleLogicalType(ESimpleLogicalValueType::Int64),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        }),
    VariantTupleLogicalType(
        {
            SimpleLogicalType(ESimpleLogicalValueType::String),
            SimpleLogicalType(ESimpleLogicalValueType::String),
        }),

    // VariantStruct
    VariantStructLogicalType(
        {
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"int",    SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
    VariantStructLogicalType(
        {
            {"int",    SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
        }),
    VariantStructLogicalType(
        {
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
            {"string", SimpleLogicalType(ESimpleLogicalValueType::String)},
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

TEST(TLogicalTypeTest, TestAllExamplesAreNotEqual)
{
    // Hopefully our example list will never be so big that n^2 complexity is a problem.
    for (const auto& lhs : ComplexTypeExampleList) {
        for (const auto& rhs : ComplexTypeExampleList) {
            if (&lhs == &rhs) {
                EXPECT_EQ(*lhs, *rhs);
            } else {
                EXPECT_NE(*lhs, *rhs);
            }
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

using TCombineTypeFunc = std::function<TLogicalTypePtr(const TLogicalTypePtr&)>;

std::vector<TCombineTypeFunc> CombineFunctions = {
    [] (const TLogicalTypePtr& type) {
        return OptionalLogicalType(type);
    },
    [] (const TLogicalTypePtr& type) {
        return ListLogicalType(type);
    },
    [] (const TLogicalTypePtr& type) {
        return StructLogicalType({{"field", type}});
    },
    [] (const TLogicalTypePtr& type) {
        return TupleLogicalType({type});
    },
    [] (const TLogicalTypePtr& type) {
        return VariantStructLogicalType({{"field", type}});
    },
    [] (const TLogicalTypePtr& type) {
        return VariantTupleLogicalType({type});
    },
};

TEST(TLogicalTypeTest, TestAllTypesInCombineFunctions)
{
    const auto allMetatypes = TEnumTraits<ELogicalMetatype>::GetDomainValues();
    std::set<ELogicalMetatype> actualMetatypes;
    for (const auto& function : CombineFunctions) {
        auto combined = function(SimpleLogicalType(ESimpleLogicalValueType::Int64));
        actualMetatypes.insert(combined->GetMetatype());
    }
    std::set<ELogicalMetatype> expectedMetatypes(allMetatypes.begin(), allMetatypes.end());
    expectedMetatypes.erase(ELogicalMetatype::Simple);
    EXPECT_EQ(actualMetatypes, expectedMetatypes);
}

class TCombineLogicalMetatypeTests
    : public ::testing::TestWithParam<TCombineTypeFunc>
{ };

TEST_P(TCombineLogicalMetatypeTests, TestValidate)
{
    auto badType = StructLogicalType({{"", SimpleLogicalType(ESimpleLogicalValueType::Int64)}});
    EXPECT_EXCEPTION_WITH_MESSAGE(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", badType)),
        "Name of struct field #0 is empty");

    const auto combineFunc = GetParam();
    const auto combinedType1 = combineFunc(badType);
    const auto combinedType2 = combineFunc(combinedType1);
    EXPECT_NE(*combinedType1, *badType);
    EXPECT_NE(*combinedType1, *combinedType2);

    EXPECT_EXCEPTION_WITH_MESSAGE(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", combinedType1)),
        "Name of struct field #0 is empty");

    EXPECT_EXCEPTION_WITH_MESSAGE(
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", combinedType2)),
        "Name of struct field #0 is empty");
}

INSTANTIATE_TEST_CASE_P(
    CombineFunctions,
    TCombineLogicalMetatypeTests,
    ::testing::ValuesIn(CombineFunctions));

class TStructValidationTest
    : public ::testing::TestWithParam<ELogicalMetatype>
{ };

TEST_P(TStructValidationTest, Test)
{
    auto metatype = GetParam();
    auto validate = [&] (const std::vector<TStructField>& fields) {
        std::function<TLogicalTypePtr(std::vector<TStructField> fields)> typeConstructor;
        if (metatype == ELogicalMetatype::Struct) {
            typeConstructor = StructLogicalType;
        } else {
            YT_VERIFY(metatype == ELogicalMetatype::VariantStruct);
            typeConstructor = VariantStructLogicalType;
        }
        ValidateLogicalType(TComplexTypeFieldDescriptor("test-column", typeConstructor(fields)));
    };

    EXPECT_NO_THROW(validate({}));

    EXPECT_EXCEPTION_WITH_MESSAGE(
        validate({{"", SimpleLogicalType(ESimpleLogicalValueType::Int64)}}),
        "Name of struct field #0 is empty");

    EXPECT_EXCEPTION_WITH_MESSAGE(
        validate({
            {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
            {"a", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
        "Struct field name \"a\" is used twice");

    EXPECT_EXCEPTION_WITH_MESSAGE(
        validate({
            {TString(257, 'a'), SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
        "Name of struct field #0 exceeds limit");
    EXPECT_EXCEPTION_WITH_MESSAGE(
        validate({
            {"\xFF", SimpleLogicalType(ESimpleLogicalValueType::Int64)},
        }),
        "Name of struct field #0 is not valid utf8");
}

INSTANTIATE_TEST_CASE_P(
    Tests,
    TStructValidationTest,
    ::testing::ValuesIn({ELogicalMetatype::Struct, ELogicalMetatype::VariantStruct}));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
