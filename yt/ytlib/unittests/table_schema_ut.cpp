#include <yt/core/test_framework/framework.h>

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/chunk_meta.pb.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NTableClient {
namespace {

using namespace NYson;
using namespace NYTree;

class TTableSchemaTest
    : public ::testing::Test
{ };

TEST_F(TTableSchemaTest, ColumnSchemaValidation)
{
    std::vector<TColumnSchema> invalidSchemas{
        // Empty names are not ok.
        TColumnSchema("", EValueType::String),
        // Names starting from SystemColumnNamePrefix are not ok.
        TColumnSchema(SystemColumnNamePrefix + "Name", EValueType::String),
        // Names longer than MaxColumnNameLength are not ok.
        TColumnSchema(TString(MaxColumnNameLength + 1, 'z'), EValueType::String),
        // Empty lock names are not ok.
        TColumnSchema("Name", EValueType::String)
            .SetLock(TString("")),
        // Locks on key columns are not ok.
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetLock(TString("LockName")),
        // Locks longer than MaxColumnLockLength are not ok.
        TColumnSchema("Name", EValueType::String)
            .SetLock(TString(MaxColumnLockLength + 1, 'z')),
        // Column type should be valid according to the ValidateSchemaValueType function.
        // Non-key columns can't be computed.
        TColumnSchema("Name", EValueType::String)
            .SetExpression(TString("SomeExpression")),
        // Key columns can't be aggregated.
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetAggregate(TString("sum"))
    };

    for (const auto& columnSchema : invalidSchemas) {
        EXPECT_THROW(ValidateColumnSchema(columnSchema, true, true), std::exception);
    }

    std::vector<TColumnSchema> validSchemas{
        TColumnSchema("Name", EValueType::String),
        TColumnSchema("Name", EValueType::Any),
        TColumnSchema(TString(256, 'z'), EValueType::String)
            .SetLock(TString(256, 'z')),
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(TString("SomeExpression")),
        TColumnSchema("Name", EValueType::String)
            .SetAggregate(TString("sum"))
    };

    for (const auto& columnSchema : validSchemas) {
        ValidateColumnSchema(columnSchema);
    }
}

TEST_F(TTableSchemaTest, ColumnSchemaUpdateValidation)
{
    std::vector<std::vector<TColumnSchema>> invalidUpdates{
        // Changing column type is not ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::Int64)
        },
        // Changing column sort order from null to something is not ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        },
        // Changing column expression is not ok.
        {
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("SomeExpression"))
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("SomeExpression")),
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("SomeExpression")),
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("SomeOtherExpression"))
        },
        // Changing column aggregate is only allowed if columns was not aggregated.
        {
            TColumnSchema("Name", EValueType::String)
                .SetAggregate(TString("sum")),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetAggregate(TString("sum")),
            TColumnSchema("Name", EValueType::String)
                .SetAggregate(TString("max"))
        },
    };

    for (const auto& pairOfSchemas : invalidUpdates) {
        ValidateColumnSchema(pairOfSchemas[0]);
        ValidateColumnSchema(pairOfSchemas[1]);
        EXPECT_THROW(ValidateColumnSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]), std::exception);
    }

    std::vector<std::vector<TColumnSchema>> validUpdates{
        // Making column aggregated if it wasn't is ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String)
                .SetAggregate(TString("sum"))
        },
        // Changing column lock is ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String)
                .SetLock(TString("Lock"))
        },
        // Making a column not sorted is ok.
        {
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetLock(TString("Lock")),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetLock(TString("Lock")),
            TColumnSchema("Name", EValueType::String)
                .SetLock(TString("OtherLock"))
        }
    };

    for (const auto& pairOfSchemas : validUpdates) {
        ValidateColumnSchema(pairOfSchemas[0]);
        ValidateColumnSchema(pairOfSchemas[1]);
        ValidateColumnSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]);
    }
}

TEST_F(TTableSchemaTest, TableSchemaValidation)
{
    std::vector<std::vector<TColumnSchema>> invalidSchemas{
        {
            // TTableSchema can't contain invalid columns.
            TColumnSchema("", EValueType::String),
        },
        {
            // Names should be unique.
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String)
        },
        {
            // Key columns should form a prefix.
            TColumnSchema("Key1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Value", EValueType::String),
            TColumnSchema("Key2", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Expression type should match the type of a column.
            TColumnSchema("Key1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("Key2")),
            TColumnSchema("Key2", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Computed columns may only depend on key columns.
            TColumnSchema("Key1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("Key2")),
            TColumnSchema("Key2", EValueType::String)
        },
        {
            // Computed columns may only depend on non-computed columns.
            TColumnSchema("Key1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("Key2")),
            TColumnSchema("Key2", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("Key3")),
            TColumnSchema("Key3", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Aggregate function should appear in a pre-defined list.
            TColumnSchema("Key1", EValueType::String)
                .SetAggregate(TString("MyFancyAggregateFunction")),
        },
        {
            // Type of aggregate function should match the type of a column.
            TColumnSchema("Key1", EValueType::String)
                .SetAggregate(TString("sum"))
        }
    };

    // There should be no more than MaxColumnLockCount locks.
    std::vector<TColumnSchema> schemaWithManyLocks;
    for (int index = 0; index < MaxColumnLockCount + 1; ++index) {
        schemaWithManyLocks.push_back(TColumnSchema("Name" + ToString(index), EValueType::String)
            .SetLock("Lock" + ToString(index)));
    }

    std::vector<std::vector<TColumnSchema>> validSchemas{
        {
            // Empty schema is valid.
        },
        {
            // Schema without key columns is valid.
            TColumnSchema("Name", EValueType::String)
        },
        {
            // Schema consisting only of key columns is also valid.
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Example of a well-formed schema.
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Height", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Weight", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("HeightPlusWeight", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("Height + Weight")),
            TColumnSchema("MaximumActivity", EValueType::Int64)
                .SetAggregate(TString("max"))
        }
    };

    for (const auto& tableSchema : invalidSchemas) {
        TTableSchema schema(tableSchema);
        EXPECT_THROW(ValidateTableSchema(schema, true), std::exception);
    }

    for (const auto& tableSchema : validSchemas) {
        TTableSchema schema(tableSchema);
        ValidateTableSchema(schema);
    }
}

TEST_F(TTableSchemaTest, TableSchemaUpdateValidation)
{
    std::vector<std::vector<TTableSchema>> invalidUpdates{
        {
            // Changing Strict = false to Strict = true is not ok.
            TTableSchema({}, false),
            TTableSchema({}, true)
        },
        {
            // Adding columns when Strict = false is not ok.
            TTableSchema({}, false),
            TTableSchema({
                TColumnSchema("Name", EValueType::String)
            }, false),
        },
        {
            // Removing columns when Strict = true is not ok.
            TTableSchema({
                TColumnSchema("Name", EValueType::String)
            }, true),
            TTableSchema({}, true)
        },
        {
            // Changing columns simultaneously with changing Strict = true to
            // Strict = false is not ok.
            TTableSchema({
                TColumnSchema("Name", EValueType::String)
            }, true),
            TTableSchema({
                TColumnSchema("Name", EValueType::String),
                TColumnSchema("Name2", EValueType::String)
            }, false)
        },
        {
            // Changing positions of key columns is not ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            }),
            TTableSchema({
                TColumnSchema("Name2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            })
        },
        {
            // Changing columns attributes should be validated by ValidateColumnSchemaUpdate function.
            TTableSchema({
                TColumnSchema("Name", EValueType::Int64)
                    .SetAggregate(TString("sum"))
            }),
            TTableSchema({
                TColumnSchema("Name", EValueType::Int64)
                    .SetAggregate(TString("max"))
            })
        },
        {
            // It is allowed to add computed column only on creation of a table (in loop below
            // the fourth argument of ValidateTableSchemaUpdate is IsEmpty = false, so the table
            // is considered non-empty).
            TTableSchema({}, true),
            TTableSchema({
                TColumnSchema("Name", EValueType::Int64)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::Int64)
                    .SetSortOrder(ESortOrder::Ascending)
                    .SetExpression(TString("Name"))
            })
        },
        {
            // When making some key column unsorted by removing sort order, unique_keys can no longer be true.
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
            }, false /* strict */, true /* unique_keys */),
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String),
            }, false /* strict */, true /* unique_keys */)
        },
        {
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName3", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
            }, false /* strict */),
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName4", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName3", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String),
            }, false /* strict */)
        },
    };

    for (const auto& pairOfSchemas : invalidUpdates) {
        EXPECT_THROW(ValidateTableSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]), std::exception);
    }

    EXPECT_THROW(ValidateTableSchemaUpdate(
        TTableSchema({
            TColumnSchema("KeyName1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name1", EValueType::Int64)
        }, true /* strict */),
        TTableSchema({
            TColumnSchema("KeyName1", EValueType::String),
            TColumnSchema("Name1", EValueType::Int64)
        }, true /* strict */),
        true /* isDynamicTable */), std::exception);

    EXPECT_THROW(ValidateTableSchemaUpdate(
        TTableSchema({}, true),
        TTableSchema({}, false),
        true /* isDynamicTable */), std::exception);

    std::vector<std::vector<TTableSchema>> validUpdates{
        {
            // Changing positions of non-key columns when Strict = true is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String),
                TColumnSchema("Name2", EValueType::String)
            }, false),
            TTableSchema({
                TColumnSchema("Name2", EValueType::String),
                TColumnSchema("Name1", EValueType::String)
            }, false)
        },
        {
            // Changing positions of non-key columns when Strict = false is also ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String),
                TColumnSchema("Name2", EValueType::String)
            }, false),
            TTableSchema({
                TColumnSchema("Name2", EValueType::String),
                TColumnSchema("Name1", EValueType::String)
            }, false)
        },
        {
            // Adding key columns at the end of key columns prefix when Strict = true is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            }, true),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            }, true)
        },
        {
            // Adding non-key columns at arbitrary place (after key columns) when Strict = true is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            }, true),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String)
            }, true)
        },
        {
            // Removing key columns from the end of key columns prefix when Strict = false is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            }, false),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            }, false)
        },
        {
            // Removing key columns from arbitrary place when Strict = false is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String)
            }, false),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending)
            }, false)
        },
        {
            // Changing Strict = true to Strict = false without changing anything else is ok.
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name1", EValueType::String),
            }, true),
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name1", EValueType::String),
            }, false)
        },
        {
            // Making several last key columns non-key (possibly with changing their order) is ok.
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName3", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
            }, false /* strict */, true /* uniqueKeys */),
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName3", EValueType::String),
                TColumnSchema("KeyName2", EValueType::String),
            }, false /* strict */)
        },
        {
            // We may even make several last key columns non-key and add some new key columns at the same time.
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
            }, true /* strict */),
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName3", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String),
            }, true /* strict */)
        },
        {
            // Making table unsorted by removing sort order for all columns is also ok.
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String)
                    .SetSortOrder(ESortOrder::Ascending),
            }, false),
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String),
                TColumnSchema("KeyName2", EValueType::String),
            }, false)
        },
    };

    for (const auto& pairOfSchemas : validUpdates) {
        ValidateTableSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]);
    }

    // It is allowed to add computed columns if table is empty.
    ValidateTableSchemaUpdate(
        TTableSchema({
            TColumnSchema("Name", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
        }, true),
        TTableSchema({
            TColumnSchema("Name", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name2", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(TString("Name"))
        }), false /* isDynamicTable */, true /* isEmptyTable */);
}

TEST_F(TTableSchemaTest, ColumnSchemaProtobufBackwardCompatibility)
{
    NProto::TColumnSchema columnSchemaProto;
    columnSchemaProto.set_name("foo");
    columnSchemaProto.set_type(static_cast<int>(EValueType::Uint64));

    TColumnSchema columnSchema;
    FromProto(&columnSchema, columnSchemaProto);

    EXPECT_EQ(columnSchema.LogicalType(), ELogicalValueType::Uint64);
    EXPECT_EQ(columnSchema.GetPhysicalType(), EValueType::Uint64);
    EXPECT_EQ(columnSchema.Name(), "foo");

    columnSchemaProto.set_logical_type(static_cast<int>(ELogicalValueType::Uint32));
    FromProto(&columnSchema, columnSchemaProto);

    EXPECT_EQ(columnSchema.LogicalType(), ELogicalValueType::Uint32);
    EXPECT_EQ(columnSchema.GetPhysicalType(), EValueType::Uint64);
    EXPECT_EQ(columnSchema.Name(), "foo");
}

////////////////////////////////////////////////////////////////////////////////

using TInferSchemaTestCase = std::tuple<std::vector<const char*>, const char*, bool>;

class TInferSchemaTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<TInferSchemaTestCase>
{ };

TEST_P(TInferSchemaTest, Basic)
{
    const auto& param = GetParam();
    const auto& schemaStrings = std::get<0>(param);
    const auto& resultSchamaString = std::get<1>(param);
    bool discardKeyColumns = std::get<2>(param);

    std::vector<TTableSchema> schemas;
    for (const auto* schemaString : schemaStrings) {
        schemas.emplace_back();
        Deserialize(schemas.back(), ConvertToNode(TYsonString(schemaString)));
    }

    TTableSchema resultSchema;
    Deserialize(resultSchema, ConvertToNode(TYsonString(resultSchamaString)));

    EXPECT_EQ(resultSchema, InferInputSchema(schemas, discardKeyColumns));
}

auto* schema1 = "[{name=Key1;type=string;sort_order=ascending}; {name=Value1;type=string}]";
auto* schema1k = "[{name=Key1;type=string}; {name=Value1;type=string}]";
auto* schema2 = "[{name=Key2;type=int64;sort_order=ascending}; {name=Value2;type=int64}]";
auto* schema3 = "[{name=Key1;type=string;sort_order=ascending}; {name=Value2;type=int64}]";
auto* schema12 = "[{name=Key1;type=string}; {name=Value1;type=string}; {name=Key2;type=int64}; {name=Value2;type=int64}]";
auto* schema123 = schema12;
auto* schema13 = "[{name=Key1;type=string;sort_order=ascending}; {name=Value1;type=string}; {name=Value2;type=int64}]";
auto* schema13k = "[{name=Key1;type=string}; {name=Value1;type=string}; {name=Value2;type=int64}]";
auto* schema4 = "[{name=ColumnA;type=string;sort_order=ascending}; {name=ColumnB;type=string}]";
auto* schema5 = "[{name=ColumnB;type=string;sort_order=ascending}; {name=ColumnC;type=string;sort_order=ascending;expression=ColumnB}]";
auto* schema45 = "[{name=ColumnA;type=string}; {name=ColumnB;type=string}; {name=ColumnC;type=string}]";

INSTANTIATE_TEST_CASE_P(
    TInferSchemaTest,
    TInferSchemaTest,
    ::testing::Values(
        TInferSchemaTestCase({schema1}, schema1, false),
        TInferSchemaTestCase({schema1}, schema1k, true),
        TInferSchemaTestCase({schema1, schema3}, schema13, false),
        TInferSchemaTestCase({schema1, schema3}, schema13k, true),
        TInferSchemaTestCase({schema1, schema2}, schema12, false),
        TInferSchemaTestCase({schema1, schema2, schema3}, schema123, false),
        TInferSchemaTestCase({schema4, schema5}, schema45, false)
));

////////////////////////////////////////////////////////////////////////////////

class TInferSchemaInvalidTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::vector<const char*>>
{ };

TEST_P(TInferSchemaInvalidTest, Basic)
{
    const auto& schemaStrings = GetParam();

    std::vector<TTableSchema> schemas;
    for (const auto* schemaString : schemaStrings) {
        schemas.emplace_back();
        Deserialize(schemas.back(), ConvertToNode(TYsonString(schemaString)));
    }

    EXPECT_THROW(InferInputSchema(schemas, true), std::exception);
}

INSTANTIATE_TEST_CASE_P(
    TInferSchemaInvalidTest,
    TInferSchemaInvalidTest,
    ::testing::Values(
        std::vector<const char*>{"<strict=%false>[{name=Key1;type=string}]"},
        std::vector<const char*>{"[{name=Key1;type=string}]", "[{name=Key1;type=any}]"}
));

////////////////////////////////////////////////////////////////////////////////

class TInvalidSchemaTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<const char*>
{ };

TEST_P(TInvalidSchemaTest, Basic)
{
    const auto& schemaString = GetParam();

    TTableSchema schema;
    Deserialize(schema, ConvertToNode(TYsonString(schemaString)));

    EXPECT_THROW(ValidateTableSchema(schema, true), std::exception);
}

INSTANTIATE_TEST_CASE_P(
    TInvalidSchemaTest,
    TInvalidSchemaTest,
    ::testing::Values(
        "[{name=x;type=int64;sort_order=ascending;expression=z}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]",
        "[{name=x;type=int64;sort_order=ascending;expression=a}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]",
        "[{name=x;type=int64;sort_order=ascending;expression=y}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]",
        "[{name=x;type=int64;sort_order=ascending;expression=x}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]",
        "[{name=x;type=int64;sort_order=ascending;expression=\"uint64(y)\"}; {name=y;type=uint64;sort_order=ascending}; {name=a;type=int64}]"
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTableClient
} // namespace NYT
