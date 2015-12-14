#include "framework.h"

#include <yt/core/ytree/convert.h>

#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/chunk_client/schema.h>

namespace NYT {
namespace NChunkClient {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TSchemaTest
    : public ::testing::Test
{ };

////////////////////////////////////////////////////////////////////////////////

const char ZeroLiteral = '\0'; // workaround for gcc

TEST_F(TSchemaTest, RangeContains)
{
    {
        TColumnRange range(""); // Infinite range
        EXPECT_TRUE(range.Contains(""));
        EXPECT_TRUE(range.Contains(Stroka(ZeroLiteral)));
        EXPECT_TRUE(range.Contains(TColumnRange("")));
        EXPECT_TRUE(range.Contains("anything"));
    }

    {
        TColumnRange range("", Stroka(ZeroLiteral));
        EXPECT_TRUE(range.Contains(""));
        EXPECT_FALSE(range.Contains(Stroka(ZeroLiteral)));
        EXPECT_FALSE(range.Contains(TColumnRange("")));
        EXPECT_FALSE(range.Contains("anything"));
    }

    {
        TColumnRange range("abc", "abe");
        EXPECT_FALSE(range.Contains(""));
        EXPECT_TRUE(range.Contains("abcjkdhfsdhf"));
        EXPECT_TRUE(range.Contains("abd"));

        EXPECT_FALSE(range.Contains(TColumnRange("")));
        EXPECT_TRUE(range.Contains(TColumnRange("abc", "abd")));
        EXPECT_TRUE(range.Contains(TColumnRange("abc", "abe")));
    }
}

TEST_F(TSchemaTest, RangeOverlaps)
{
    {
        TColumnRange range("a", "b");
        EXPECT_FALSE(range.Overlaps(TColumnRange("b", "c")));
        EXPECT_TRUE(range.Overlaps(TColumnRange("anything", "c")));
    }

    {
        TColumnRange range("");
        EXPECT_TRUE(range.Overlaps(TColumnRange("")));
        EXPECT_TRUE(range.Overlaps(TColumnRange("", Stroka(ZeroLiteral))));
        EXPECT_TRUE(range.Overlaps(TColumnRange("anything", "c")));
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSchemaTest, ChannelContains)
{
    auto ch1 = TChannel::Empty();
    ch1.AddColumn("anything");
    EXPECT_TRUE(ch1.Contains("anything"));
    EXPECT_FALSE(ch1.Contains(TColumnRange("anything")));

    {
        auto ch2 = TChannel::Empty();
        ch2.AddColumn("anything");
        EXPECT_TRUE(ch1.Contains(ch2));
        EXPECT_TRUE(ch2.Contains(ch1));
    }

    ch1.AddRange(TColumnRange("m", "p"));

    {
        auto ch2 = TChannel::Empty();
        ch2.AddColumn("anything");
        EXPECT_TRUE(ch1.Contains(ch2));
        EXPECT_FALSE(ch2.Contains(ch1));

        ch2.AddRange(TColumnRange("m"));
        EXPECT_FALSE(ch1.Contains(ch2));
        EXPECT_TRUE(ch2.Contains(ch1));
    }
}

TEST_F(TSchemaTest, ChannelOverlaps)
{
    auto ch1 = TChannel::Empty();
    ch1.AddRange(TColumnRange("a", "c"));

    {
        auto ch2 = TChannel::Empty();
        ch2.AddColumn("anything");
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }

    {
        EXPECT_TRUE(TColumnRange("a", "c").Overlaps(TColumnRange("b", "d")));
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange("b", "d"));
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }

    {
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange(""));
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }

    {
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange("c", "d"));
        EXPECT_FALSE(ch1.Overlaps(ch2));
        EXPECT_FALSE(ch2.Overlaps(ch1));
    }

    ch1.AddColumn("Hello!");

    {
        auto ch2 = TChannel::Empty();
        ch2.AddRange(TColumnRange("c", "d"));
        ch2.AddColumn("Hello!");
        EXPECT_TRUE(ch1.Overlaps(ch2));
        EXPECT_TRUE(ch2.Overlaps(ch1));
    }
}

TEST_F(TSchemaTest, ChannelSubtract)
{
    {
        TChannel
            ch1 = TChannel::Empty(),
            ch2 = TChannel::Empty(),
            res = TChannel::Empty();

        ch1.AddRange(TColumnRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddColumn("something");
        ch1 -= ch2;

        EXPECT_FALSE(ch1.Contains(ch2));

        res.AddRange(TColumnRange("a", "c"));
        EXPECT_TRUE(ch1.Contains(res));
        EXPECT_TRUE(res.Contains(ch1));
    }

    {
        TChannel
            ch1 = TChannel::Empty(),
            ch2 = TChannel::Empty(),
            res = TChannel::Empty();

        ch1.AddRange(TColumnRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddRange(TColumnRange("a", "c"));
        ch1 -= ch2;

        EXPECT_FALSE(ch1.Contains(ch2));

        res.AddColumn("something");
        EXPECT_TRUE(ch1.Contains(res));
        EXPECT_TRUE(res.Contains(ch1));
    }

    {
        TChannel
            ch1 = TChannel::Empty(),
            ch2 = TChannel::Empty(),
            res = TChannel::Empty();

        ch1.AddRange(TColumnRange("a", "c"));
        ch1.AddColumn("something");

        ch2.AddRange(TColumnRange("b", "c"));
        ch1 -= ch2;

        EXPECT_FALSE(ch1.Contains(ch2));

        res.AddColumn("something");
        res.AddRange(TColumnRange("a", "b"));
        EXPECT_TRUE(ch1.Contains(res));
        EXPECT_TRUE(res.Contains(ch1));
    }
}

} // namespace
} // namespace NChunkClient

////////////////////////////////////////////////////////////////////////////////

namespace NTableClient {

using namespace NYson;

class TTableSchemaTest
    : public ::testing::Test
{ };

TEST_F(TTableSchemaTest, ColumnSchemaValidationTest)
{
    std::vector<TColumnSchema> invalidSchemas = {
        // Empty names are not ok.
        TColumnSchema("", EValueType::String),
        // Names starting from SystemColumnNamePrefix are not ok.
        TColumnSchema("$Name", EValueType::String),
        // Names longer than MaxColumnNameLength are not ok.
        TColumnSchema(Stroka(257, 'z'), EValueType::String),
        // Empty lock names are not ok.
        TColumnSchema("Name", EValueType::String).SetLock(Stroka("")),
        // Locks on key columns are not ok.
        TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetLock(Stroka("LockName")),
        // Locks longer than MaxLockNameLength are not ok.
        TColumnSchema("Name", EValueType::String).SetLock(Stroka(257, 'z')),
        // Column type should be valid according to the ValidateSchemaValueType function.
        TColumnSchema("Name", EValueType::TheBottom),
        // Non-key columns can't be computed.
        TColumnSchema("Name", EValueType::String).SetExpression(Stroka("SomeExpression")),
        // Key columns can't be aggregated.
        TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetAggregate(Stroka("sum"))
    };

    for (const auto& columnSchema : invalidSchemas) {
        EXPECT_THROW(ValidateColumnSchema(columnSchema), std::exception);
    }
    
    std::vector<TColumnSchema> validSchemas = {
        TColumnSchema("Name", EValueType::String),
        TColumnSchema("Name", EValueType::Any),
        TColumnSchema(Stroka(256, 'z'), EValueType::String).SetLock(Stroka(256, 'z')),
        TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("SomeExpression")),
        TColumnSchema("Name", EValueType::String).SetAggregate(Stroka("sum"))
    };

    for (const auto& columnSchema : validSchemas) {
        ValidateColumnSchema(columnSchema);
    }
}

TEST_F(TTableSchemaTest, ColumnSchemaUpdateValidationTest)
{
    std::vector<std::vector<TColumnSchema>> invalidUpdates = {
        // Changing column type is not ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::Int64)
        },
        // Changing column sort order is not ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending)
        },
        {
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name", EValueType::String)
        },
        // Changing column expression is not ok.
        {
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("SomeExpression"))
        },
        {
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("SomeExpression")),
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending)
        },
        {
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("SomeExpression")),
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("SomeOtherExpression"))
        },
        // Changing column aggregate is only allowed if columns was not aggregated.
        {
            TColumnSchema("Name", EValueType::String).SetAggregate(Stroka("sum")),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String).SetAggregate(Stroka("sum")),
            TColumnSchema("Name", EValueType::String).SetAggregate(Stroka("max"))
        },
    };

    for (const auto& pairOfSchemas : invalidUpdates) {
        ValidateColumnSchema(pairOfSchemas[0]);
        ValidateColumnSchema(pairOfSchemas[1]);
        EXPECT_THROW(ValidateColumnSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]), std::exception);
    }

    std::vector<std::vector<TColumnSchema>> validUpdates = {
        // Making column aggregated if it wasn't is ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String).SetAggregate(Stroka("sum"))
        },
        // Changing column lock is ok.
        {
            TColumnSchema("Name", EValueType::String),
            TColumnSchema("Name", EValueType::String).SetLock(Stroka("Lock"))
        },
        {
            TColumnSchema("Name", EValueType::String).SetLock(Stroka("Lock")),
            TColumnSchema("Name", EValueType::String)
        },
        {
            TColumnSchema("Name", EValueType::String).SetLock(Stroka("Lock")),
            TColumnSchema("Name", EValueType::String).SetLock(Stroka("OtherLock"))
        }
    };

    for (const auto& pairOfSchemas : validUpdates) {
        ValidateColumnSchema(pairOfSchemas[0]);
        ValidateColumnSchema(pairOfSchemas[1]);
        ValidateColumnSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]);
    }
}

TEST_F(TTableSchemaTest, TableSchemaValidationTest)
{
    std::vector<std::vector<TColumnSchema>> invalidSchemas = {
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
            TColumnSchema("Key1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Value", EValueType::String),
            TColumnSchema("Key2", EValueType::String).SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Expression type should match the type of a column.
            TColumnSchema("Key1", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Key2")),
            TColumnSchema("Key2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Computed columns may only depend on key columns.
            TColumnSchema("Key1", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Key2")),
            TColumnSchema("Key2", EValueType::String)
        },
        {
            // Computed columns may only depend on non-computed columns.
            TColumnSchema("Key1", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Key2")),
            TColumnSchema("Key2", EValueType::String).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Key3")),
            TColumnSchema("Key3", EValueType::String).SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Aggregate function should appear in a pre-defined list.
            TColumnSchema("Key1", EValueType::String).SetAggregate(Stroka("MyFancyAggregateFunction")),
        },
        {
            // Type of aggregate function should match the type of a column.
            TColumnSchema("Key1", EValueType::String).SetAggregate(Stroka("sum"))
        }
    };

    // There should be no more than MaxColumnLockCount locks.
    std::vector<TColumnSchema> schemaWithManyLocks;
    for (int index = 0; index < 33; ++index) {
        schemaWithManyLocks.push_back(TColumnSchema("Name" + ToString(index), EValueType::String).SetLock("Lock" + ToString(index)));
    }

    std::vector<std::vector<TColumnSchema>> validSchemas = {
        { 
            // Empty schema is valid.
        },
        {
            // Schema without key columns is valid.
            TColumnSchema("Name", EValueType::String)
        },
        {
            // Schema consisting only of key columns is also valid.
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending)
        },
        {
            // Example of a well-formed schema.
            TColumnSchema("Name", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Height", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Weight", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("HeightPlusWeight", EValueType::Int64).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Height + Weight")),
            TColumnSchema("MaximumActivity", EValueType::Int64).SetAggregate(Stroka("max"))
        }
    };
       
    for (const auto& tableSchema : invalidSchemas) {
        EXPECT_THROW(TTableSchema schema(tableSchema), std::exception);
    }

    for (const auto& tableSchema : validSchemas) {
        TTableSchema schema(tableSchema);
    }
}

TEST_F(TTableSchemaTest, TableSchemaUpdateValidationTest)
{
    std::vector<std::vector<TTableSchema>> invalidUpdates = {
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
            // Changing positions of key columns is not ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            }),
            TTableSchema({
                TColumnSchema("Name2", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            })
        },
        {
            // Changing columns attributes should be validated by ValidateColumnSchemaUpdate function.
            TTableSchema({
                TColumnSchema("Name", EValueType::Int64).SetAggregate(Stroka("sum"))
            }),
            TTableSchema({
                TColumnSchema("Name", EValueType::Int64).SetAggregate(Stroka("max"))
            })
        },
        {
            // It is allowed to add computed column only on creation of a table (in loop below
            // the fourth argument of ValidateTableSchemaUpdate is IsEmpty = false, so the table
            // is considered non-empty).
            TTableSchema({}, true),
            TTableSchema({
                TColumnSchema("Name", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Name"))
            })
        }
    };
      
    for (const auto& pairOfSchemas : invalidUpdates) {
        ValidateTableSchema(pairOfSchemas[0]);
        ValidateTableSchema(pairOfSchemas[1]);
        EXPECT_THROW(ValidateTableSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]), std::exception);
    }
    
    // It is not allowed to add computed column 
    EXPECT_THROW(ValidateTableSchemaUpdate(
        TTableSchema({
            TColumnSchema("Name", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
        }),
        TTableSchema({
            TColumnSchema("Name", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Name"))
        }), false /* isDynamicTable */, true /* isEmptyTable */), std::exception);

    EXPECT_THROW(ValidateTableSchemaUpdate(
        TTableSchema({}, true),
        TTableSchema({}, false),
        true /* isDynamicTable */), std::exception);

    std::vector<std::vector<TTableSchema>> validUpdates = {
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
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            }, true),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            }, true)
        },
        {
            // Adding non-key columns at arbitrary place (after key columns) when Strict = true is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            }, true),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String)
            }, true)
        },
        {
            // Removing key columns from the end of key columns prefix when Strict = false is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            }, false),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            }, false)
        },
        {
            // Removing key columns from arbitrary place when Strict = false is ok.
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name2", EValueType::String)
            }, false),
            TTableSchema({
                TColumnSchema("Name1", EValueType::String).SetSortOrder(ESortOrder::Ascending)
            }, false)
        },
        {
            // NB: When changing Strict = true to Strict = false, it is allowed to perform two
            // actions simultaneosly: add some key columns and non-key columns as if Strict = true,
            // and after that remove some key columns and non-key columns from the result as if Strict = false.
            // Note that attributes of all columns that are presented both in old and new schema should
            // not be changed.
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName2", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name1", EValueType::String),
                TColumnSchema("Name2", EValueType::String),
                TColumnSchema("Name3", EValueType::String)
            }, true),
            TTableSchema({
                TColumnSchema("KeyName1", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName3", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("KeyName4", EValueType::String).SetSortOrder(ESortOrder::Ascending),
                TColumnSchema("Name4", EValueType::String),
                TColumnSchema("Name3", EValueType::String),
            }, false)
        }
    };
    
    for (const auto& pairOfSchemas : validUpdates) {
        ValidateTableSchema(pairOfSchemas[0]);
        ValidateTableSchema(pairOfSchemas[1]);
        ValidateTableSchemaUpdate(pairOfSchemas[0], pairOfSchemas[1]);
    }

    // It allowed to add computed columns if table is empty and table schema is empty. 
    ValidateTableSchemaUpdate(
        TTableSchema({}, true),
        TTableSchema({
            TColumnSchema("Name", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("Name2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending).SetExpression(Stroka("Name"))
        }), false /* isDynamicTable */, true /* isEmptyTable */);
}


} // namespace NTableClient
} // namespace NYT
