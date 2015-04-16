#include "stdafx.h"
#include "statistics.h"

#include <core/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NYTree;

////////////////////////////////////////////////////////////////////

void TStatistics::Add(const NYPath::TYPath& name, i64 summary)
{
    Data_[name] = summary;
}

void TStatistics::Merge(const TStatistics& other)
{
    for (const auto& pair : other.Data_) {
        Data_[pair.first] = pair.second;
    }
}

void Deserialize(TStatistics& value, INodePtr node)
{
    switch (node->GetType()) {
        case ENodeType::Int64:
            value.Data_.insert(std::make_pair(node->GetPath(), node->AsInt64()->GetValue()));
            break;

        case ENodeType::Uint64:
            value.Data_.insert(std::make_pair(node->GetPath(), node->AsUint64()->GetValue()));
            break;

        case ENodeType::Map:
            for (auto& pair : node->AsMap()->GetChildren()) {
                Deserialize(value, pair.second);
            }
            break;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////

TStatisticsConsumer::TStatisticsConsumer(
    TParsedStatisticsConsumer consumer,
    const NYPath::TYPath& path)
    : Path_(path)
    , TreeBuilder_(CreateBuilderFromFactory(GetEphemeralNodeFactory()))
    , Consumer_(consumer)
{ }

void TStatisticsConsumer::OnMyListItem()
{
    TreeBuilder_->BeginTree();
    Forward(TreeBuilder_.get(), BIND(&TStatisticsConsumer::ProcessItem, this), NYson::EYsonType::Node);
}

void TStatisticsConsumer::ProcessItem()
{
    TStatistics statistics;
    INodePtr parsed;
    if (Path_.empty()) {
        parsed = TreeBuilder_->EndTree();
    } else {
        parsed = GetEphemeralNodeFactory()->CreateMap();
        ForceYPath(parsed, Path_);
        SetNodeByYPath(parsed, Path_, TreeBuilder_->EndTree());
    }
    Deserialize(statistics, parsed);
    Consumer_.Run(statistics);
}

////////////////////////////////////////////////////////////////////

TSummary::TSummary()
    : Sum_(0)
    , Count_(0)
    , Min_(std::numeric_limits<i64>::max())
    , Max_(std::numeric_limits<i64>::min())
{ }

void TSummary::AddSample(i64 value)
{
    Sum_ += value;
    Count_ += 1;
    Min_ = std::min(Min_, value);
    Max_ = std::max(Max_, value);
}

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer)
{
    BuildYsonMapFluently(consumer)
        .Item("sum").Value(summary.GetSum())
        .Item("count").Value(summary.GetCount())
        .Item("min").Value(summary.GetMin())
        .Item("max").Value(summary.GetMax());
}

void Deserialize(TSummary& value, INodePtr node)
{
    auto mapNode = node->AsMap();

    value.Sum_ = ConvertTo<i64>(mapNode->GetChild("sum"));
    value.Count_ = ConvertTo<i64>(mapNode->GetChild("count"));
    value.Min_ = ConvertTo<i64>(mapNode->GetChild("min"));
    value.Max_ = ConvertTo<i64>(mapNode->GetChild("max"));
}

////////////////////////////////////////////////////////////////////

void TAggregatedStatistics::AddSample(const TStatistics& statistics)
{
    for (const auto& pair : statistics.Data_) {
        Data_[pair.first].AddSample(pair.second);
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
