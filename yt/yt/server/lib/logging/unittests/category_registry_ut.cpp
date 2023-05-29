#include <gtest/gtest-spi.h>

#include <yt/yt/server/lib/logging/category_registry.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/row_base.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/fluent_log.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NLogging {
namespace {

using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

const TLogger Logger("Test");

////////////////////////////////////////////////////////////////////////////////

TEST(TCategoryRegistryTest, Basic)
{
    auto schema = New<TTableSchema>(std::vector{
        TColumnSchema("required_string_field", ESimpleLogicalValueType::String).SetRequired(true),
        TColumnSchema("optional_int64_field", ESimpleLogicalValueType::Int64).SetRequired(false),
        TColumnSchema("optional_any_field", ESimpleLogicalValueType::Any).SetRequired(false)
    });
    auto schemafulLogger = CreateSchemafulLogger("CategoryRegistrySchemafulLogger", schema);

    TStringStream output;
    TYsonWriter writer(&output);
    TStructuredCategoryRegistry::Get()->DumpCategories(&writer);

    auto categories = ConvertToNode(TYsonString(TString(output.Str())))->AsMap();
    EXPECT_EQ(categories->GetKeys().size(), size_t(1));
    EXPECT_EQ(categories->GetKeys()[0], "CategoryRegistrySchemafulLogger");

    auto category = categories->GetChildValueOrThrow<IMapNodePtr>("CategoryRegistrySchemafulLogger");
    EXPECT_EQ(category->GetKeys().size(), size_t(1));
    EXPECT_EQ(category->GetKeys()[0], "schema");

    auto schemaNode = category->GetChildValueOrThrow<INodePtr>("schema");

    TTableSchemaPtr resultSchema;
    Deserialize(resultSchema, schemaNode);
    EXPECT_EQ(resultSchema->Columns().size(), schema->Columns().size());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSchemafulLoggerTest, LogWithNullField)
{
    auto config = TLogManagerConfig::CreateDefault();
    config->AbortOnAlert = true;
    TLogManager::Get()->Configure(config, /*sync*/ true);

    auto schemafulLogger = CreateSchemafulLogger("LogWithNullFieldSchemafulLogger", New<TTableSchema>(std::vector{
        TColumnSchema("required_field", ESimpleLogicalValueType::Int64).SetRequired(true),
        TColumnSchema("optional_field", ESimpleLogicalValueType::Int64).SetRequired(false)
    }));

    auto logWithRequiredNullField = [&schemafulLogger] () {
        LogStructuredEventFluently(schemafulLogger, ELogLevel::Info)
            .Item("optional_field").Value(0);
    };

    // This statement generates a googletest warning:  "Death test use fork(), which is unsafe particularly in a threaded context..."
    // In fact, it is safe because the execution of this function is single-threaded.
    ASSERT_DEATH(logWithRequiredNullField(), "");

    auto logWithOptionalNullField = [&schemafulLogger] () {
        LogStructuredEventFluently(schemafulLogger, ELogLevel::Info)
            .Item("required_field").Value(0);
    };
    logWithOptionalNullField();
}

TEST(TSchemafulLoggerTest, LogWithNotSpecifiedField)
{
    auto config = TLogManagerConfig::CreateDefault();
    config->AbortOnAlert = true;
    TLogManager::Get()->Configure(config, /*sync*/ true);

    auto schemafulLogger = CreateSchemafulLogger("LogWithNotSpecifiedFieldSchemafulLogger", New<TTableSchema>(std::vector{
        TColumnSchema("optional_field", ESimpleLogicalValueType::Int64).SetRequired(false)
    }));

    auto logWithNotSpecifiedField = [&schemafulLogger] () {
        LogStructuredEventFluently(schemafulLogger, ELogLevel::Info)
            .Item("not_specified").Value(0);
    };

    // This statement generates a googletest warning:  "Death test use fork(), which is unsafe particularly in a threaded context..."
    // In fact, it is safe because the execution of this function is single-threaded.
    ASSERT_DEATH(logWithNotSpecifiedField(), "");
}


TEST(TSchemafulLoggerTest, LogWithWrongFieldType)
{
    auto config = TLogManagerConfig::CreateDefault();
    config->AbortOnAlert = true;
    TLogManager::Get()->Configure(config, /*sync*/ true);

    auto schemafulLogger = CreateSchemafulLogger("LogWithWrongFieldTypeSchemafulLogger", New<TTableSchema>(std::vector{
        TColumnSchema("optional_field", ESimpleLogicalValueType::Int64).SetRequired(false)
    }));

    auto logWithWrongFieldType = [&schemafulLogger] () {
        LogStructuredEventFluently(schemafulLogger, ELogLevel::Info)
            .Item("optional_field").Value("value");
    };

    // This statement generates a googletest warning:  "Death test use fork(), which is unsafe particularly in a threaded context..."
    // In fact, it is safe because the execution of this function is single-threaded.
    ASSERT_DEATH(logWithWrongFieldType(), "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
