#include "framework.h"

#include <yt/ytlib/table_client/schema.h>

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
        TColumnSchema(Stroka(MaxColumnNameLength + 1, 'z'), EValueType::String),
        // Empty lock names are not ok.
        TColumnSchema("Name", EValueType::String)
            .SetLock(Stroka("")),
        // Locks on key columns are not ok.
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetLock(Stroka("LockName")),
        // Locks longer than MaxColumnLockLength are not ok.
        TColumnSchema("Name", EValueType::String)
            .SetLock(Stroka(MaxColumnLockLength + 1, 'z')),
        // Column type should be valid according to the ValidateSchemaValueType function.
        TColumnSchema("Name", EValueType::TheBottom),
        // Non-key columns can't be computed.
        TColumnSchema("Name", EValueType::String)
            .SetExpression(Stroka("SomeExpression")),
        // Key columns can't be aggregated.
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetAggregate(Stroka("sum"))
    };

    for (const auto& columnSchema : invalidSchemas) {
        EXPECT_THROW(ValidateColumnSchema(columnSchema, true), std::exception);
    }

    std::vector<TColumnSchema> validSchemas{
        TColumnSchema("Name", EValueType::String),
        TColumnSchema("Name", EValueType::Any),
        TColumnSchema(Stroka(256, 'z'), EValueType::String)
            .SetLock(Stroka(256, 'z')),
        TColumnSchema("Name", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(Stroka("SomeExpression")),
        TColumnSchema("Name", EValueType::String)
            .SetAggregate(Stroka("sum"))
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
                .SetExpression(Stroka("SomeExpression"))
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(Stroka("SomeExpression")),
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(Stroka("SomeExpression")),
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(Stroka("SomeOtherExpression"))
        },
        // Changing column aggregate is only allowed if columns was not aggregated.
        {
            TColumnSchema("Name", EValueType::String)
                .SetAggregate(Stroka("sum")),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetAggregate(Stroka("sum")),
            TColumnSchema("Name", EValueType::String)
                .SetAggregate(Stroka("max"))
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
                .SetAggregate(Stroka("sum"))
        },
        // Changing column lock is ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String)
                .SetLock(Stroka("Lock"))
        },
        // Making a column not sorted is ok.
        {
            TColumnSchema("Name", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetLock(Stroka("Lock")),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String)
                .SetLock(Stroka("Lock")),
            TColumnSchema("Name", EValueType::String)
                .SetLock(Stroka("OtherLock"))
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
                .SetExpression(Stroka("Key2")),
            TColumnSchema("Key2", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Computed columns may only depend on key columns.
            TColumnSchema("Key1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(Stroka("Key2")),
            TColumnSchema("Key2", EValueType::String)
        },
        {
            // Computed columns may only depend on non-computed columns.
            TColumnSchema("Key1", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(Stroka("Key2")),
            TColumnSchema("Key2", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
                .SetExpression(Stroka("Key3")),
            TColumnSchema("Key3", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Aggregate function should appear in a pre-defined list.
            TColumnSchema("Key1", EValueType::String)
                .SetAggregate(Stroka("MyFancyAggregateFunction")),
        },
        {
            // Type of aggregate function should match the type of a column.
            TColumnSchema("Key1", EValueType::String)
                .SetAggregate(Stroka("sum"))
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
                .SetExpression(Stroka("Height + Weight")),
            TColumnSchema("MaximumActivity", EValueType::Int64)
                .SetAggregate(Stroka("max"))
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
                    .SetAggregate(Stroka("sum"))
            }),
            TTableSchema({
                TColumnSchema("Name", EValueType::Int64)
                    .SetAggregate(Stroka("max"))
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
                    .SetExpression(Stroka("Name"))
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
                .SetExpression(Stroka("Name"))
        }), false /* isDynamicTable */, true /* isEmptyTable */);
}

TEST_F(TTableSchemaTest, InferInputSchema)
{
    TTableSchema schema1({
        TColumnSchema("Key1", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("Value1", EValueType::String)
    }, false /* strict */);
    TTableSchema schema1k({
        TColumnSchema("Key1", EValueType::String),
        TColumnSchema("Value1", EValueType::String)
    }, false /* strict */);
    TTableSchema schema2({
        TColumnSchema("Key2", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("Value2", EValueType::Int64)
    }, false /* strict */);
    TTableSchema schema3({
        TColumnSchema("Key1", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("Value2", EValueType::Int64)
    }, false /* strict */);
    TTableSchema schema12({
        TColumnSchema("Key1", EValueType::String),
        TColumnSchema("Value1", EValueType::String),
        TColumnSchema("Key2", EValueType::Int64),
        TColumnSchema("Value2", EValueType::Int64)
    }, false /* strict */);
    TTableSchema schema123 = schema12;
    TTableSchema schema13({
        TColumnSchema("Key1", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("Value1", EValueType::String),
        TColumnSchema("Value2", EValueType::Int64)
    }, false /* strict */);
    TTableSchema schema13k({
        TColumnSchema("Key1", EValueType::String),
        TColumnSchema("Value1", EValueType::String),
        TColumnSchema("Value2", EValueType::Int64)
    }, false /* strict */);
    TTableSchema schema1x({
        TColumnSchema("Key1", EValueType::Int64)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("Value1", EValueType::String)
    }, false /* strict */);
    TTableSchema schema31x({
        TColumnSchema("Key1", EValueType::Any)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("Value2", EValueType::Int64),
        TColumnSchema("Value1", EValueType::String)
    }, false /* strict */);
    TTableSchema schema4({
        TColumnSchema("ColumnA", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("ColumnB", EValueType::String)
    }, false /* strict */);
    TTableSchema schema5({
        TColumnSchema("ColumnB", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending),
        TColumnSchema("ColumnC", EValueType::String)
            .SetSortOrder(ESortOrder::Ascending)
            .SetExpression(Stroka("ColumnB"))
    }, false /* strict */);
    TTableSchema schema45({
        TColumnSchema("ColumnA", EValueType::String),
        TColumnSchema("ColumnB", EValueType::String),
        TColumnSchema("ColumnC", EValueType::String)
    }, false /* strict */);
    // TODO(max42): uncomment this when Null becomes
    // an allowed column value type.
    TTableSchema schema6({
        TColumnSchema("ColumnA", EValueType::String),
        TColumnSchema("ColumnB", EValueType::Int64),
        //TColumnSchema("ColumnC", EValueType::Null),
        TColumnSchema("ColumnD", EValueType::Any),
        TColumnSchema("ColumnE", EValueType::String),
        TColumnSchema("ColumnF", EValueType::Int64),
        //TColumnSchema("ColumnG", EValueType::Null),
        TColumnSchema("ColumnH", EValueType::Any),
        //TColumnSchema("ColumnI", EValueType::String),
        //TColumnSchema("ColumnJ", EValueType::Int64),
        //TColumnSchema("ColumnK", EValueType::Null),
        //TColumnSchema("ColumnL", EValueType::Any),
        TColumnSchema("ColumnM", EValueType::String),
        TColumnSchema("ColumnN", EValueType::Int64),
        //TColumnSchema("ColumnO", EValueType::Null),
        TColumnSchema("ColumnP", EValueType::Any),
    }, false /* strict */);
    TTableSchema schema7({
        TColumnSchema("ColumnA", EValueType::String),
        TColumnSchema("ColumnB", EValueType::String),
        //TColumnSchema("ColumnC", EValueType::String),
        TColumnSchema("ColumnD", EValueType::String),
        TColumnSchema("ColumnE", EValueType::Int64),
        TColumnSchema("ColumnF", EValueType::Int64),
        //TColumnSchema("ColumnG", EValueType::Int64),
        TColumnSchema("ColumnH", EValueType::Int64),
        //TColumnSchema("ColumnI", EValueType::Null),
        //TColumnSchema("ColumnJ", EValueType::Null),
        //TColumnSchema("ColumnK", EValueType::Null),
        //TColumnSchema("ColumnL", EValueType::Null),
        TColumnSchema("ColumnM", EValueType::Any),
        TColumnSchema("ColumnN", EValueType::Any),
        //TColumnSchema("ColumnO", EValueType::Any),
        TColumnSchema("ColumnP", EValueType::Any),
    }, false /* strict */);
    TTableSchema schema67({
        TColumnSchema("ColumnA", EValueType::String),
        TColumnSchema("ColumnB", EValueType::Any),
        //TColumnSchema("ColumnC", EValueType::String),
        TColumnSchema("ColumnD", EValueType::Any),
        TColumnSchema("ColumnE", EValueType::Any),
        TColumnSchema("ColumnF", EValueType::Int64),
        //TColumnSchema("ColumnG", EValueType::Int64),
        TColumnSchema("ColumnH", EValueType::Any),
        //TColumnSchema("ColumnI", EValueType::String),
        //TColumnSchema("ColumnJ", EValueType::Int64),
        //TColumnSchema("ColumnK", EValueType::Null),
        //TColumnSchema("ColumnL", EValueType::Any),
        TColumnSchema("ColumnM", EValueType::Any),
        TColumnSchema("ColumnN", EValueType::Any),
        //TColumnSchema("ColumnO", EValueType::Any),
        TColumnSchema("ColumnP", EValueType::Any),
    }, false /* strict */);
    TTableSchema schema8({
        TColumnSchema("Value1", EValueType::Any),
    }, true /* strict */);
    TTableSchema schema8ns({
        TColumnSchema("Value1", EValueType::Any),
    }, false /* strict */);
    TTableSchema schema9({
        TColumnSchema("Value2", EValueType::Any),
    }, true /* strict */);
    TTableSchema schema9ns({
        TColumnSchema("Value2", EValueType::Any),
    }, false /* strict */);
    TTableSchema schema89({
        TColumnSchema("Value1", EValueType::Any),
        TColumnSchema("Value2", EValueType::Any),
    }, true /* strict */);
    TTableSchema schema89ns({
        TColumnSchema("Value1", EValueType::Any),
        TColumnSchema("Value2", EValueType::Any),
    }, false /* strict */);

    EXPECT_EQ(schema1, InferInputSchema({schema1}, false /* discardKeyColumns */));
    EXPECT_EQ(schema1k, InferInputSchema({schema1}, true /* discardKeyColumns */));

    EXPECT_EQ(schema13, InferInputSchema({schema1, schema3}, false /* discardKeyColumns */));
    EXPECT_EQ(schema13k, InferInputSchema({schema1, schema3}, true /* discardKeyColumns */));

    EXPECT_EQ(schema12, InferInputSchema({schema1, schema2}, false /* discardKeyColumns */));
    EXPECT_EQ(schema123, InferInputSchema({schema1, schema2, schema3}, false /* discardKeyColumns */));

    EXPECT_EQ(schema31x, InferInputSchema({schema3, schema1x}, false /* discardKeyColumns */));
    EXPECT_EQ(schema45, InferInputSchema({schema4, schema5}, false /* discardKeyColumns */));

    EXPECT_EQ(schema67, InferInputSchema({schema6, schema7}, false /* discardKeyColumns */));

    EXPECT_EQ(schema89, InferInputSchema({schema8, schema9}, false /* discardKeyColumns */));
    EXPECT_EQ(schema89ns, InferInputSchema({schema8, schema9ns}, false /* discardKeyColumns */));
    EXPECT_EQ(schema89ns, InferInputSchema({schema8ns, schema9ns}, false /* discardKeyColumns */));
}

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
