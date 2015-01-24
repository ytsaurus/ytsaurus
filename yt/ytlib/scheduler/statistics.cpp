#include "stdafx.h"
#include "statistics.h"

#include <core/ytree/fluent.h>
#include <core/ytree/serialize.h>
#include <core/ytree/convert.h>
#include <core/ytree/tree_builder.h>

#include <array>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

TSummary::TSummary()
    : Sum_(0)
    , Count_(0)
    , Min_(std::numeric_limits<i64>::max())
    , Max_(std::numeric_limits<i64>::min())
{ }

TSummary::TSummary(i64 value)
    : Sum_(value)
    , Count_(1)
    , Min_(value)
    , Max_(value)
{ }

void TSummary::Merge(const TSummary& other)
{
    Sum_ += other.Sum_;
    Count_ += other.Count_;
    Min_ = std::min(Min_, other.Min_);
    Max_ = std::max(Max_, other.Max_);
}

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("sum").Value(summary.GetSum())
            .Item("count").Value(summary.GetCount())
            .Item("min").Value(summary.GetMin())
            .Item("max").Value(summary.GetMax())
        .EndMap();
}

void Deserialize(TSummary& value, NYTree::INodePtr node)
{
    static const std::array<Stroka, 4> possibleKeys = {
        "sum",
        "count",
        "min",
        "max"
    };

    auto mapNode = node->AsMap();
    auto keys = mapNode->GetKeys();
    if (keys.size() != possibleKeys.size()) {
        THROW_ERROR_EXCEPTION("Expected map with %v values but got %v",
            possibleKeys.size(),
            keys.size());
    }

    value.Sum_ = NYTree::ConvertTo<i64>(mapNode->GetChild("sum"));
    value.Count_ = NYTree::ConvertTo<i64>(mapNode->GetChild("count"));
    value.Min_ = NYTree::ConvertTo<i64>(mapNode->GetChild("min"));
    value.Max_ = NYTree::ConvertTo<i64>(mapNode->GetChild("max"));
}

////////////////////////////////////////////////////////////////////

void TStatistics::Add(const NYPath::TYPath& name, const TSummary& summary)
{
    PathToSummary_[name] = summary;
}

void TStatistics::Merge(const TStatistics& other)
{
    for (const auto& pair : other.PathToSummary_) {
        PathToSummary_[pair.first].Merge(pair.second);
    }
}

void TStatistics::Clear()
{
    PathToSummary_.clear();
}

bool TStatistics::IsEmpty() const
{
    return PathToSummary_.empty();
}

TSummary TStatistics::Get(const NYPath::TYPath& name) const
{
    auto it = PathToSummary_.find(name);
    if (it != PathToSummary_.end()) {
        return it->second;
    }
    THROW_ERROR_EXCEPTION("No such statistics %Qv", name);
}

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    auto root = NYTree::GetEphemeralNodeFactory()->CreateMap();
    for (const auto& pair : statistics.PathToSummary_) {
        ForceYPath(root, pair.first);
        auto value = NYTree::ConvertToNode(pair.second);
        SetNodeByYPath(root, pair.first, value);
    }
    NYTree::Serialize(*root, consumer);
}

void Deserialize(TStatistics& value, NYTree::INodePtr node)
{
    // |node| represents a YSON-encoded TStatistics or just TSummary.
    try {
        TSummary summary;
        Deserialize(summary, node);
        value.PathToSummary_.insert(std::make_pair(node->GetPath(), std::move(summary)));
    } catch (const std::exception& ) {
        for (auto& pair : node->AsMap()->GetChildren()) {
            Deserialize(value, pair.second);
        }
    }
}

////////////////////////////////////////////////////////////////////

TStatisticsConsumer::TStatisticsConsumer(
    TParsedStatisticsConsumer consumer,
    const NYPath::TYPath& path)
    : Path_(path)
    , TreeBuilder_(NYTree::CreateBuilderFromFactory(NYTree::GetEphemeralNodeFactory()))
    , Consumer_(consumer)
{ }

void TStatisticsConsumer::OnStringScalar(const TStringBuf& value)
{
    THROW_ERROR_EXCEPTION("Statistics cannot contain \"string\" values");
}

void TStatisticsConsumer::OnInt64Scalar(i64 value)
{
    if (Depth_ == 0) {
        THROW_ERROR_EXCEPTION("Statistics should use map as a container");
    }
    TreeBuilder_->OnInt64Scalar(value);
}

void TStatisticsConsumer::OnUint64Scalar(ui64 value)
{
    THROW_ERROR_EXCEPTION("Statistics cannot contain \"uint64\"; use \"int64\" instead");
}

void TStatisticsConsumer::OnBooleanScalar(bool value)
{
    THROW_ERROR_EXCEPTION("Statistics cannot contain \"boolean\" values; use \"int64\" instead");
}

void TStatisticsConsumer::OnDoubleScalar(double value)
{
    THROW_ERROR_EXCEPTION("Statistics cannot contain \"double\" values; use \"int64\" instead");
}

void TStatisticsConsumer::OnEntity()
{
    THROW_ERROR_EXCEPTION("Statistics cannot contain entities");
}

void TStatisticsConsumer::OnBeginList()
{
    THROW_ERROR_EXCEPTION("Statistics cannot contain lists");
}

void TStatisticsConsumer::OnListItem()
{
    TreeBuilder_->BeginTree();
}

void TStatisticsConsumer::OnEndList()
{
    YUNREACHABLE();
}

void TStatisticsConsumer::OnBeginMap()
{
    ++Depth_;
    TreeBuilder_->OnBeginMap();
}

void TStatisticsConsumer::OnKeyedItem(const TStringBuf& key)
{
    TreeBuilder_->OnKeyedItem(key);
}

void TStatisticsConsumer::OnEndMap()
{
    TreeBuilder_->OnEndMap();
    --Depth_;
    if (Depth_ == 0) {
        TStatistics statistics;
        NYTree::INodePtr parsed;
        if (Path_.empty()) {
            parsed = TreeBuilder_->EndTree();
        } else {
            parsed = NYTree::GetEphemeralNodeFactory()->CreateMap();
            ForceYPath(parsed, Path_);
            SetNodeByYPath(parsed, Path_, TreeBuilder_->EndTree());
        }
        ConvertToStatistics(statistics, parsed);
        Consumer_.Run(statistics);
    }
}

void TStatisticsConsumer::OnBeginAttributes()
{
    THROW_ERROR_EXCEPTION("Statistics cannot contain attributes");
}

void TStatisticsConsumer::OnEndAttributes()
{
    YUNREACHABLE();
}

void TStatisticsConsumer::ConvertToStatistics(TStatistics& value, NYTree::INodePtr node)
{
    if (node->GetType() == NYTree::ENodeType::Int64) {
        TSummary summary(node->AsInt64()->GetValue());
        value.Add(node->GetPath(), summary);
        return;
    }

    for (const auto& pair : node->AsMap()->GetChildren()) {
        ConvertToStatistics(value, pair.second);
    }
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
