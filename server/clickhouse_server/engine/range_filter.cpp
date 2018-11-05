#include "range_filter.h"

#include "db_helpers.h"

#include <yt/server/clickhouse_server/native/range_filter.h>

#include <Common/Exception.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB {

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}   // namespace DB

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

using namespace DB;

////////////////////////////////////////////////////////////////////////////////

std::vector<DB::SortColumnDescription> NamesToSortColumnDescriptions(Names names)
{
    std::vector<DB::SortColumnDescription> result;
    result.reserve(names.size());
    for (auto& name : names) {
        result.emplace_back(name, -1, -1, nullptr);
    }
    return result;
}

class TRangeFilter
    : public NNative::IRangeFilter
{
private:
    const KeyCondition Condition;
    const DB::DataTypes KeyDataTypes;

public:
    TRangeFilter(const Context& context,
                 const SelectQueryInfo& queryInfo,
                 const NamesAndTypesList& allColumns,
                 const Names& primarySortColumns,
                 ExpressionActionsPtr pkExpression,
                 DB::DataTypes keyDataTypes)
        : Condition(queryInfo, context, allColumns, std::move(primarySortColumns), std::move(pkExpression))
        , KeyDataTypes(std::move(keyDataTypes))
    {}

    bool CheckRange(
        const NNative::TValue* leftKey,
        const NNative::TValue* rightKey,
        size_t keySize) const override;
};

////////////////////////////////////////////////////////////////////////////////

bool TRangeFilter::CheckRange(
    const NNative::TValue* leftKey,
    const NNative::TValue* rightKey,
    size_t keySize) const
{
    if (keySize != KeyDataTypes.size()) {
        throw Exception("invalid key size", ErrorCodes::LOGICAL_ERROR);
    }

    auto left = GetFields(leftKey, keySize);
    auto right = GetFields(rightKey, keySize);

    return Condition.mayBeTrueInRange(
        keySize,
        left.data(),
        right.data(),
        KeyDataTypes);
}

////////////////////////////////////////////////////////////////////////////////

NNative::IRangeFilterPtr CreateRangeFilter(
    const Context& context,
    const SelectQueryInfo& queryInfo,
    const TTableSchema& schema)
{
    auto pkExpression = std::make_shared<ExpressionActions>(
        schema.KeyColumns,
        context);

    return std::make_shared<TRangeFilter>(
        context,
        queryInfo,
        schema.Columns,
        schema.PrimarySortColumns,
        std::move(pkExpression),
        schema.GetKeyDataTypes());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
