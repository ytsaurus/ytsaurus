#include <yt/yt/client/arrow/schema.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NArrow {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static void TestConversions(const std::shared_ptr<arrow20::Schema>& arrowSchema, TTableSchemaPtr expected)
{
    EXPECT_EQ(*CreateYTTableSchemaFromArrowSchema(arrowSchema), *expected);
    EXPECT_EQ(*CreateYTTableSchemaFromArrowSchema(
        std::make_shared<arrow20::Schema>(CreateArrowSchemaFromYTTableSchema(
            *CreateYTTableSchemaFromArrowSchema(arrowSchema)
        ))
    ), *expected);
}

TEST(TArrowConversionsTest, Empty)
{
    TestConversions(
        std::make_shared<arrow20::Schema>(arrow20::FieldVector()),
        New<TTableSchema>(std::vector<TColumnSchema>()));
}

TEST(TArrowConversionsTest, Bool)
{
    TestConversions(
        std::make_shared<arrow20::Schema>(arrow20::FieldVector({
            std::make_shared<arrow20::Field>("bool", std::make_shared<arrow20::BooleanType>(), false)
        })),
        New<TTableSchema>(std::vector<TColumnSchema>({
            TColumnSchema(TString("bool"), SimpleLogicalType(ESimpleLogicalValueType::Boolean))
        })));
}

TEST(TArrowConversionsTest, Binary)
{
    TestConversions(
        std::make_shared<arrow20::Schema>(arrow20::FieldVector({
            std::make_shared<arrow20::Field>("binary", std::make_shared<arrow20::BinaryType>(), false)
        })),
        New<TTableSchema>(std::vector<TColumnSchema>({
            TColumnSchema(TString("binary"), SimpleLogicalType(ESimpleLogicalValueType::String))
        })));
}

TEST(TArrowConversionsTest, String)
{
    TestConversions(
        std::make_shared<arrow20::Schema>(arrow20::FieldVector({
            std::make_shared<arrow20::Field>("string", std::make_shared<arrow20::StringType>(), false)
        })),
        New<TTableSchema>(std::vector<TColumnSchema>({
            TColumnSchema(TString("string"), SimpleLogicalType(ESimpleLogicalValueType::Utf8))
        })));
}

TEST(TArrowConversionsTest, NullableString)
{
    TestConversions(
        std::make_shared<arrow20::Schema>(arrow20::FieldVector({
            std::make_shared<arrow20::Field>("string", std::make_shared<arrow20::StringType>(), true)
        })),
        New<TTableSchema>(std::vector<TColumnSchema>({
            TColumnSchema(TString("string"), OptionalLogicalType(SimpleLogicalType(ESimpleLogicalValueType::Utf8)))
        })));
}

TEST(TArrowConversionsTest, Struct)
{
    TestConversions(
        std::make_shared<arrow20::Schema>(arrow20::FieldVector({
            std::make_shared<arrow20::Field>("struct", std::make_shared<arrow20::StructType>(
                arrow20::FieldVector({
                    std::make_shared<arrow20::Field>("string", std::make_shared<arrow20::StringType>(), false)
                })
            ), false)
        })),
        New<TTableSchema>(std::vector<TColumnSchema>({
            TColumnSchema(TString("struct"), StructLogicalType(std::vector<TStructField>({
                {"string", SimpleLogicalType(ESimpleLogicalValueType::Utf8)}
            })))
        })));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NArrow
