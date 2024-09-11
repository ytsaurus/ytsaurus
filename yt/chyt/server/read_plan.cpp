#include "read_plan.h"

#include <yt/yt/client/table_client/schema.h>

#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>

namespace NYT::NClickHouseServer {

using namespace NTableClient;

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
    auto prewhereActions = DB::IMergeTreeSelectAlgorithm::getPrewhereActions(
        prewhereInfo,
        DB::ExpressionActionsSettings::fromSettings(settings, DB:: CompileExpressions::yes),
        settings.enable_multiple_prewhere_read_steps);

    std::vector<TReadStepWithFilter> steps;
    steps.reserve(prewhereActions.steps.size() + 1);

    bool needFilter = false;

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

} // namespace NYT::NClickHouseServer
