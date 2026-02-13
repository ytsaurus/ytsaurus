#include "read_plan.h"

#include "conversion.h"

#include <library/cpp/iterator/zip.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/ytree/helpers.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>
#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionActionsSettings.h>

namespace DB::Setting {

////////////////////////////////////////////////////////////////////////////////

extern const SettingsBool enable_multiple_prewhere_read_steps;
extern const SettingsShortCircuitFunctionEvaluation short_circuit_function_evaluation;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::Setting

namespace NYT::NClickHouseServer {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TBlockWithFilter::TBlockWithFilter(i64 rowCount)
    : RowCount(rowCount)
    , RowCountAfterFilter(rowCount)
{ }

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

bool HasShortCircuitActions(const DB::ActionsDAG& actionsDag)
{
    for (const auto& node : actionsDag.getNodes()) {
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

DB::IColumn::Ptr TFilterInfo::RemoveColumnIfNeeded(TBlockWithFilter& blockWithFilter) const
    {
    auto& [block, rowCount, currentFilter, rowCountAfterFilter] = blockWithFilter;

    if (!block.has(FilterColumnName)) {
        return nullptr;
    }
    auto filterColumnPosition = block.getPositionByName(FilterColumnName);
    auto filterColumn = block.getByPosition(filterColumnPosition).column->convertToFullIfNeeded();

    // Output block should contain at least one column to preserve row count.
    if (RemoveFilterColumn && block.columns() != 1) {
        block.erase(filterColumnPosition);
    }

    return filterColumn;
}

void TFilterInfo::Execute(TBlockWithFilter& blockWithFilter) const
{
    auto& [block, rowCount, currentFilter, rowCountAfterFilter] = blockWithFilter;

    {
        // execute requires size_t as an argument.
        size_t numRows = rowCount;
        Actions->execute(block, numRows);
    }

    auto filterColumn = RemoveColumnIfNeeded(blockWithFilter);

    if (currentFilter.empty()) {
        currentFilter.resize_fill(rowCount, 1);
    }

    auto throwIllegalType = [&] {
        THROW_ERROR_EXCEPTION("Illegal column type %Qv for filter in PREWHERE", filterColumn->getName());
    };

    // Combine current filter and filter column.
    // Note that filter column is either UInt8 or Nullable(UInt8).
    if (const auto* nullableFilterColumn = DB::checkAndGetColumn<DB::ColumnNullable>(filterColumn.get())) {
        const auto* nullMapColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(&nullableFilterColumn->getNullMapColumn());
        YT_VERIFY(nullMapColumn);
        const auto& nullMap = nullMapColumn->getData();
        YT_VERIFY(std::ssize(nullMap) == rowCount);

        const auto* nestedColumn = DB::checkAndGetColumn<DB::ColumnVector<DB::UInt8>>(&nullableFilterColumn->getNestedColumn());
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

TReadPlanWithFilterPtr BuildSimpleReadPlan(std::vector<TColumnSchema> columns, std::vector<NYTree::IAttributeDictionaryPtr> columnAttributes)
{
    std::vector<TReadStepWithFilter> steps;
    steps.emplace_back(std::move(columns), std::move(columnAttributes));
    return New<TReadPlanWithFilter>(std::move(steps), /*NeedFilter*/ false);
}

TReadPlanWithFilterPtr BuildReadPlanWithPrewhere(
    std::vector<TColumnSchema> columns,
    std::vector<NYTree::IAttributeDictionaryPtr> columnAttributes,
    const DB::PrewhereInfoPtr& prewhereInfo,
    const DB::Settings& settings)
{
    bool enableMultiplePrewhereReadSteps = settings[DB::Setting::enable_multiple_prewhere_read_steps];
    YT_VERIFY(columnAttributes.empty() || columns.size() == columnAttributes.size());
    if (columnAttributes.empty()) {
        columnAttributes.resize(columns.size(), NYTree::CreateEphemeralAttributes());
    }

    // Do not split conditions with short circuit functions to multiple prewhere steps,
    // because short circuit works only within one step.
    // E.g. `user_id = 1 and toDate(dt) = '1999-10-01'` should not be splited,
    // because `toDate` may throw an exception.
    if (enableMultiplePrewhereReadSteps &&
        settings[DB::Setting::short_circuit_function_evaluation] != DB::ShortCircuitFunctionEvaluation::DISABLE &&
        HasShortCircuitActions(prewhereInfo->prewhere_actions))
    {
        enableMultiplePrewhereReadSteps = false;
    }

    auto prewhereActions = DB::MergeTreeSelectProcessor::getPrewhereActions(
        prewhereInfo,
        DB::ExpressionActionsSettings(settings, DB::CompileExpressions::yes),
        enableMultiplePrewhereReadSteps,
        // TODO(buyval01): investigate
        /*force_short_circuit_execution*/ false);

    std::vector<TReadStepWithFilter> steps;
    steps.reserve(prewhereActions.steps.size() + 1);

    // NB: If we split prewhere actions into several steps, there would be several filter columns.
    // We have two options:
    // 1. Combine all these columns using the `and()` function and return a new filter column to CH.
    // 2. Apply all filters on our side.
    // The second option is easier.
    bool needFilter = prewhereActions.steps.size() > 1;

    THashMap<std::string, int> columnNameToSchemaPos;
    for (int i = 0; i < std::ssize(columns); ++i) {
        columnNameToSchemaPos.emplace(columns[i].Name(), i);
    }

    THashSet<std::string> columnNamesFromPreviousSteps;

    for (const auto& step : prewhereActions.steps)
    {
        std::vector<TColumnSchema> stepColumns;
        std::vector<NYTree::IAttributeDictionaryPtr> stepColumnAttributes;

        for (const auto& columnName : step->actions->getRequiredColumns()) {
            if (!columnNamesFromPreviousSteps.contains(columnName)) {
                auto it = columnNameToSchemaPos.find(columnName);
                if (it == columnNameToSchemaPos.end()) {
                    THROW_ERROR_EXCEPTION("No such column %Qv in read schema", columnName);
                }
                stepColumns.push_back(columns[it->second]);
                stepColumnAttributes.push_back(columnAttributes[it->second]);
                columnNamesFromPreviousSteps.insert(columnName);
            }
        }

        // CH may compute columns like "greater(a, 2)" and use them in the following steps.
        // Add all such columns to columnNamesFromPreviousSteps.
        for (const auto& columnName : step->actions->getSampleBlock().getNames()) {
            columnNamesFromPreviousSteps.insert(columnName);
        }

        auto filterInfo = TFilterInfo{step->actions, step->filter_column_name, step->remove_filter_column};
        steps.push_back({std::move(stepColumns), std::move(stepColumnAttributes), std::move(filterInfo)});

        needFilter |= step->need_filter;
    }

    std::vector<TColumnSchema> remainingColumns;
    std::vector<NYTree::IAttributeDictionaryPtr> remainingColumnAttributes;

    for (const auto& [column, attributes] : Zip(columns, columnAttributes)) {
        if (!columnNamesFromPreviousSteps.contains(column.Name())) {
            remainingColumns.push_back(column);
            remainingColumnAttributes.push_back(attributes);
        }
    }

    if (!remainingColumns.empty()) {
        steps.push_back({std::move(remainingColumns), std::move(remainingColumnAttributes), /*FilterInfo*/ std::nullopt});
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
        for (const auto& column : ToHeaderBlock(step.Columns, step.ColumnAttributes, settings)) {
            blockWithFilter.Block.insert(column);
        }
        if (step.FilterInfo) {
            step.FilterInfo->Execute(blockWithFilter);
        }
    }
    for (const auto& step : readPlan->Steps) {
        if (step.FilterInfo) {
            step.FilterInfo->RemoveColumnIfNeeded(blockWithFilter);
        }
    }

    return blockWithFilter.Block;
}

bool SuitableForDistinctReadOptimization(const TReadPlanWithFilterPtr& readPlan, const TCompositeSettingsPtr& settings)
{
    return DeriveHeaderBlockFromReadPlan(readPlan, settings).columns() == 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
