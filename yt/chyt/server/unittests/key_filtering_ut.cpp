#include "helpers.h"

#include <yt/chyt/server/conversion.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/key.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>

#include <Interpreters/ExpressionActions.h>

#include <Storages/KeyDescription.h>

namespace NYT::NClickHouseServer {
namespace {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DB::KeyCondition CreateKeyCondition(const TTableSchema& schema, const DB::DataTypes& dataTypes, TUnversionedRow key)
{
    auto context = GetGlobalContext();
    DB::KeyDescription description;
    description.column_names = schema.GetKeyColumns();
    DB::NamesAndTypesList columns;
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        columns.emplace_back(description.column_names[index], dataTypes[index]);
        description.reverse_flags.push_back(schema.Columns()[index].SortOrder() == ESortOrder::Descending);
    }
    description.expression = std::make_shared<DB::ExpressionActions>(DB::ActionsDAG(columns));
    DB::ActionsDAGWithInversionPushDown filter(nullptr, context);
    DB::KeyCondition condition(filter, context, description);
    for (int index = 0; index < schema.GetKeyColumnCount(); ++index) {
        auto value = key[index].Type == EValueType::Null
            ? DB::Field(DB::NEGATIVE_INFINITY)
            : DB::Field(key[index].Data.Int64);
        YT_VERIFY(condition.addCondition(description.column_names[index], DB::Range(value)));
    }
    return condition;
}

TEST(TDescendingKeyFilteringTest, KeyConditionPreservesKeyBounds)
{
    std::vector<TUnversionedOwningRow> prefixes;
    std::vector<TUnversionedOwningRow> keys;
    TUnversionedRowBuilder builder;
    prefixes.emplace_back(builder.GetRow());
    for (int a = -1; a <= 1; ++a) {
        builder.Reset();
        builder.AddValue(a == -1 ? MakeUnversionedNullValue() : MakeUnversionedInt64Value(a));
        prefixes.emplace_back(builder.GetRow());
        for (int b = -1; b <= 1; ++b) {
            TUnversionedRowBuilder keyBuilder = builder;
            keyBuilder.AddValue(b == -1 ? MakeUnversionedNullValue(1) : MakeUnversionedInt64Value(b, 1));
            keys.emplace_back(keyBuilder.GetRow());
            prefixes.emplace_back(keyBuilder.GetRow());
        }
    }
    for (int directions = 0; directions < 4; ++directions) {
        auto schema = TTableSchema({
            TColumnSchema("a", EValueType::Int64).SetSortOrder(directions & 1 ? ESortOrder::Descending : ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Int64).SetSortOrder(directions & 2 ? ESortOrder::Descending : ESortOrder::Ascending),
        });
        auto comparator = schema.ToComparator();
        DB::DataTypes dataTypes(2, DB::makeNullable(std::make_shared<DB::DataTypeInt64>()));
        for (const auto& lowerPrefix : prefixes) {
            for (const auto& upperPrefix : prefixes) {
                for (int inclusive = 0; inclusive < 4; ++inclusive) {
                    auto lower = TOwningKeyBound::FromRow(lowerPrefix, /*isInclusive*/ inclusive & 1, /*isUpper*/ false);
                    auto upper = TOwningKeyBound::FromRow(upperPrefix, /*isInclusive*/ inclusive & 2, /*isUpper*/ true);
                    if (comparator.IsRangeEmpty(lower, upper)) {
                        continue;
                    }
                    auto bounds = ToClickHouseKeys(lower, upper, schema, dataTypes, /*usedKeyColumnCount*/ 2);
                    for (const auto& key : keys) {
                        bool expected = comparator.TestKey(TKey::FromRow(key), lower) &&
                            comparator.TestKey(TKey::FromRow(key), upper);
                        // Inclusive conversion can retain extra keys for exclusive or incomplete bounds.
                        if (!expected && (inclusive != 3 || lowerPrefix.GetCount() != 2 || upperPrefix.GetCount() != 2)) {
                            continue;
                        }
                        auto condition = CreateKeyCondition(schema, dataTypes, key);
                        bool actual = condition.mayBeTrueInRange(2, bounds.MinKey.data(), bounds.MaxKey.data(), dataTypes);
                        ASSERT_EQ(expected, actual)
                            << "Directions: " << directions << ", inclusiveness: " << inclusive
                            << ", lower: " << ToString(lower) << ", upper: " << ToString(upper)
                            << ", key: " << ToString(key);
                    }
                }
            }
        }
    }
}

TEST(TDescendingKeyFilteringTest, KeyConditionShortensPhysicalKeyBounds)
{
    auto schema = TTableSchema({TColumnSchema("a", EValueType::Int64).SetSortOrder(ESortOrder::Descending)});
    DB::DataTypes dataTypes{DB::makeNullable(std::make_shared<DB::DataTypeInt64>())};
    for (int inclusive = 0; inclusive < 4; ++inclusive) {
        for (int upperKey : {18, 42}) {
            TUnversionedRowBuilder lower;
            lower.AddValue(MakeUnversionedInt64Value(42));
            lower.AddValue(MakeUnversionedStringValue("z", 1));
            TUnversionedRowBuilder upper;
            upper.AddValue(MakeUnversionedInt64Value(upperKey));
            upper.AddValue(MakeUnversionedStringValue("x", 1));
            auto bounds = ToClickHouseKeys(
                TKeyBound::FromRow(lower.GetRow(), /*isInclusive*/ inclusive & 1, /*isUpper*/ false),
                TKeyBound::FromRow(upper.GetRow(), /*isInclusive*/ inclusive & 2, /*isUpper*/ true),
                schema,
                dataTypes,
                /*usedKeyColumnCount*/ 1);
            for (i64 key : {17, 18, 30, 42, 43}) {
                TUnversionedRowBuilder keyBuilder;
                keyBuilder.AddValue(MakeUnversionedInt64Value(key));
                auto condition = CreateKeyCondition(schema, dataTypes, keyBuilder.GetRow());
                bool actual = condition.mayBeTrueInRange(1, bounds.MinKey.data(), bounds.MaxKey.data(), dataTypes);
                EXPECT_EQ(key >= upperKey && key <= 42, actual)
                    << "Key: " << key << ", upper key: " << upperKey << ", inclusiveness: " << inclusive;
            }
        }
    }
}

} // namespace
} // namespace NYT::NClickHouseServer
