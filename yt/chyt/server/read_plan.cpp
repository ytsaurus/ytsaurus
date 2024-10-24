#include "read_plan.h"

#include "conversion.h"

#include <yt/yt/client/table_client/schema.h>

#include <Functions/IFunction.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

DB::DataTypesWithConstInfo GetDataTypesWithConstInfo(const DB::ActionsDAG::NodeRawConstPtrs& nodes)
{
    DB::DataTypesWithConstInfo types;
    types.reserve(nodes.size());
    for (const auto& child : nodes) {
        bool isConst = child->column && isColumnConst(*child->column);
        types.push_back({child->result_type, isConst});
    }
    return types;
}

bool HasShortCircuitActions(const DB::ActionsDAGPtr& actionsDag)
{
    if (!actionsDag) {
        return false;
    }
    for (const auto& node : actionsDag->getNodes()) {
        if (node.type == DB::ActionsDAG::ActionType::FUNCTION) {
            auto arguments = GetDataTypesWithConstInfo(node.children);
            if (node.function_base->isSuitableForShortCircuitArgumentsExecution(arguments)) {
                return true;
            }
        }
    }
    return false;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TFilterInfo::Execute(TBlockWithFilter& blockWithFilter) const
{
    auto& [block, rowCount, currentFilter, rowCountAfterFilter] = blockWithFilter;

    {
        // execute requires size_t as an argument.
        size_t numRows = rowCount;
        Actions->execute(block, numRows);
    }

    auto filterColumnPosition = block.getPositionByName(FilterColumnName);
    auto filterColumn = block.getByPosition(filterColumnPosition).column->convertToFullIfNeeded();

    // Output block should contain at least one column to preserve row count.
    if (RemoveFilterColumn && block.columns() != 1) {
        block.erase(filterColumnPosition);
    }

    if (currentFilter.empty()) {
        currentFilter.resize_fill(rowCount, 1);
    }

    auto throwIllegalType = [&] {
        THROW_ERROR_EXCEPTION("Illegal type for filter in PREWHERE: %Qv", filterColumn->getName());
    };

    // Combine current filter and filter column.
    // Note that filter column is either UInt8 or Nullable(UInt8).
    if (const auto* nullableFilterColumn = DB::checkAndGetColumn<DB::ColumnNullable>(filterColumn.get())) {
        const auto* nullMapColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(nullableFilterColumn->getNullMapColumn());
        YT_VERIFY(nullMapColumn);
        const auto& nullMap = nullMapColumn->getData();
        YT_VERIFY(std::ssize(nullMap) == rowCount);

        const auto* nestedColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(nullableFilterColumn->getNestedColumn());
        if (!nestedColumn) {
            throwIllegalType();
        }
        const auto& values = nestedColumn->getData();
        YT_VERIFY(std::ssize(values) == rowCount);

        for (i64 index = 0; index < rowCount; ++index) {
            currentFilter[index] &= !nullMap[index] && values[index];
        }
    } else {
        const auto* valueColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(filterColumn.get());
        if (!valueColumn) {
            throwIllegalType();
        }
        const auto& values = valueColumn->getData();
        YT_VERIFY(std::ssize(values) == rowCount);

        for (i64 index = 0; index < rowCount; ++index) {
            currentFilter[index] &= static_cast<bool>(values[index]);
        }
    }

    rowCountAfterFilter = DB::countBytesInFilter(currentFilter);
}

////////////////////////////////////////////////////////////////////////////////

int TReadPlanWithFilter::GetReadColumnCount() const
{
    int columnCount = 0;
    for (const auto& step : Steps) {
        columnCount += step.Columns.size();
    }
    return columnCount;
}

bool TReadPlanWithFilter::SuitableForTwoStagePrewhere() const
{
    return Steps.size() > 1 && !Steps.back().FilterInfo;
}

////////////////////////////////////////////////////////////////////////////////

TReadPlanWithFilterPtr BuildSimpleReadPlan(const std::vector<TColumnSchema>& columns)
{
    std::vector<TReadStepWithFilter> steps;
    steps.emplace_back(columns);
    return New<TReadPlanWithFilter>(std::move(steps), /*NeedFilter*/ false);
}

TReadPlanWithFilterPtr BuildReadPlanWithPrewhere(
    const std::vector<TColumnSchema>& columns,
    const DB::PrewhereInfoPtr& prewhereInfo,
    const DB::Settings& settings)
{
    bool enableMultiplePrewhereReadSteps = settings.enable_multiple_prewhere_read_steps;

    // Do not split conditions with short circuit functions to multiple prewhere steps,
    // because short circuit works only within one step.
    // E.g. `user_id = 1 and toDate(dt) = '1999-10-01'` should not be splited,
    // because `toDate` may throw an exception.
    if (enableMultiplePrewhereReadSteps &&
        settings.short_circuit_function_evaluation != DB::ShortCircuitFunctionEvaluation::DISABLE &&
        HasShortCircuitActions(prewhereInfo->prewhere_actions))
    {
        enableMultiplePrewhereReadSteps = false;
    }

    auto prewhereActions = DB::IMergeTreeSelectAlgorithm::getPrewhereActions(
        prewhereInfo,
        DB::ExpressionActionsSettings::fromSettings(settings, DB::CompileExpressions::yes),
        enableMultiplePrewhereReadSteps);

    std::vector<TReadStepWithFilter> steps;
    steps.reserve(prewhereActions.steps.size() + 1);

    // NB: If we split prewhere actions into several steps, there would be several filter columns.
    // We have two options:
    // 1. Combine all these columns using the `and()` function and return a new filter column to CH.
    // 2. Apply all filters on our side.
    // The second option is easier.
    bool needFilter = prewhereActions.steps.size() > 1;

    THashMap<std::string, TColumnSchema> columnNameToSchema;
    for (const auto& column: columns) {
        columnNameToSchema.emplace(column.Name(), column);
    }

    THashSet<std::string> columnNamesFromPreviousSteps;

    for (const auto& step : prewhereActions.steps)
    {
        std::vector<TColumnSchema> stepColumns;

        for (const auto& columnName : step->actions->getRequiredColumns()) {
            if (!columnNamesFromPreviousSteps.contains(columnName)) {
                auto it = columnNameToSchema.find(columnName);
                if (it == columnNameToSchema.end()) {
                    THROW_ERROR_EXCEPTION("No such column %Qv in read schema", columnName);
                }
                stepColumns.push_back(it->second);
                columnNamesFromPreviousSteps.insert(columnName);
            }
        }

        // CH may compute columns like "greater(a, 2)" and use them in the following steps.
        // Add all such columns to columnNamesFromPreviousSteps.
        for (const auto& columnName : step->actions->getSampleBlock().getNames()) {
            columnNamesFromPreviousSteps.insert(columnName);
        }

        auto filterInfo = TFilterInfo{step->actions, step->filter_column_name, step->remove_filter_column};
        steps.push_back({std::move(stepColumns), std::move(filterInfo)});

        needFilter |= step->need_filter;
    }

    std::vector<TColumnSchema> remainingColumns;

    for (const auto& column : columns) {
        if (!columnNamesFromPreviousSteps.contains(column.Name())) {
            remainingColumns.push_back(column);
        }
    }

    if (!remainingColumns.empty()) {
        steps.push_back({std::move(remainingColumns), /*FilterInfo*/ std::nullopt});
    }

    // Sanity check.
    int totalColumns = 0;
    for (const auto& step : steps) {
        totalColumns += step.Columns.size();
    }
    YT_VERIFY(totalColumns == std::ssize(columns));

    return New<TReadPlanWithFilter>(std::move(steps), needFilter);
}

TReadPlanWithFilterPtr ExtractPrewhereOnlyReadPlan(const TReadPlanWithFilterPtr& readPlan)
{
    auto steps = readPlan->Steps;
    if (!steps.empty() && !steps.back().FilterInfo) {
        steps.pop_back();
    }
    // We always need to filter during a separate prewhere phase.
    return New<TReadPlanWithFilter>(std::move(steps), /*NeedFilter*/ true);
}

////////////////////////////////////////////////////////////////////////////////

DB::Block DeriveHeaderBlockFromReadPlan(const TReadPlanWithFilterPtr& readPlan, const TCompositeSettingsPtr& settings)
{
    TBlockWithFilter blockWithFilter(/*rowCount*/ 0);

    for (const auto& step : readPlan->Steps) {
        for (const auto& column : ToHeaderBlock(step.Columns, settings)) {
            blockWithFilter.Block.insert(column);
        }
        if (step.FilterInfo) {
            step.FilterInfo->Execute(blockWithFilter);
        }
    }

    return blockWithFilter.Block;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
