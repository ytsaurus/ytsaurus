#include "read_from_yt_step.h"

#include <Interpreters/ExpressionActions.h>

#include <IO/Operators.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

TReadFromYTStep::TReadFromYTStep(
    DB::Pipe pipe,
    const DB::SelectQueryInfo& queryInfo,
    std::vector<std::shared_ptr<IChytIndexStat>> indexStats,
    const std::vector<TTablePtr>& tables)
    : DB::ReadFromPreparedSource(std::move(pipe))
    , IndexStats_(std::move(indexStats))
    , PrewhereInfo_(queryInfo.prewhere_info)
{
    TStringBuilder description;
    description.AppendString("Tables: [");
    for (const auto& table : tables) {
        description.AppendString(table->GetPath());
        description.AppendString(", ");
    }
    description.AppendString("] ");
    setStepDescription(description.Flush());
}

String TReadFromYTStep::getName() const
{
    return "ReadFromYT";
}

void TReadFromYTStep::describeIndexes(DB::IQueryPlanStep::FormatSettings& formatSettings) const
{
    if (IndexStats_.empty()) {
        return;
    }
    auto& out = formatSettings.out;

    std::string prefix(formatSettings.offset, formatSettings.indent_char);
    out << prefix << "Indexes: \n";
    for (const auto& indexStat : IndexStats_) {
        indexStat->DescribeIndex(formatSettings);
        out << "\n";
    }
}

void TReadFromYTStep::describeIndexes(DB::JSONBuilder::JSONMap& map) const
{
    if (IndexStats_.empty()) {
        return;
    }
    auto indexesArray = std::make_unique<DB::JSONBuilder::JSONArray>();
    for (const auto& indexStat : IndexStats_) {
        indexesArray->add(indexStat->DescribeIndex());
    }
    map.add("Indexes", std::move(indexesArray));
}

void TReadFromYTStep::describeActions(FormatSettings& formatSettings) const
{
    auto& out = formatSettings.out;

    std::string prefix(formatSettings.offset, formatSettings.indent_char);

    if (PrewhereInfo_) {
        out << prefix << "Prewhere info" << '\n';
        out << prefix << "Need filter: " << PrewhereInfo_->need_filter << '\n';
        out << prefix << "Column: " << PrewhereInfo_->prewhere_column_name;
        if (PrewhereInfo_->remove_prewhere_column) {
            out << " (removed)";
        }
        out << '\n';

        auto expression = std::make_shared<DB::ExpressionActions>(PrewhereInfo_->prewhere_actions.clone());
        expression->describeActions(out, prefix);
    }
}

void TReadFromYTStep::describeActions(DB::JSONBuilder::JSONMap& map) const
{
    if (PrewhereInfo_) {
        auto prewhereInfoMap = std::make_unique<DB::JSONBuilder::JSONMap>();
        prewhereInfoMap->add("Need filter", PrewhereInfo_->need_filter);
        prewhereInfoMap->add("Column", PrewhereInfo_->prewhere_column_name);
        prewhereInfoMap->add("Remove filter column", PrewhereInfo_->remove_prewhere_column);
        auto expression = std::make_shared<DB::ExpressionActions>(PrewhereInfo_->prewhere_actions.clone());
        prewhereInfoMap->add("Prewhere filter expression", expression->toTree());
        map.add("Prewhere info", std::move(prewhereInfoMap));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
