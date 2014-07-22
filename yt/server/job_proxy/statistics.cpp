#include "stdafx.h"
#include "statistics.h"

#include <core/ytree/fluent.h>
#include <core/ytree/serialize.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

i64 min(i64 left, i64 right)
{
    if (left < right) {
        return left;
    } else {
        return right;
    }
}

i64 max(i64 left, i64 right)
{
    if (left < right) {
        return right;
    } else {
        return left;
    }
}

TSummary::TSummary()
    : Summ_(0)
    , Count_(0)
    , Min_(std::numeric_limits<i64>::max())
    , Max_(std::numeric_limits<i64>::min())
{ }

TSummary::TSummary(i64 value)
    : Summ_(value)
    , Count_(1)
    , Min_(value)
    , Max_(value)
{ }

void TSummary::Merge(const TSummary& other)
{
    Summ_ += other.Summ_;
    Count_ += other.Count_;
    Min_ = min(Min_, other.Min_);
    Max_ = max(Max_, other.Max_);
}

void Serialize(const TSummary& summary, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("summ").Value(summary.GetSumm())
            .Item("count").Value(summary.GetCount())
            .Item("min").Value(summary.GetMin())
            .Item("max").Value(summary.GetMax())
        .EndMap();
}

////////////////////////////////////////////////////////////////////

void TStatistics::Add(const Stroka& name, const TSummary& summary)
{
    Statistics_[name] = summary;
}

void TStatistics::Merge(const TStatistics& other)
{
    for (const auto& pair : other.Statistics_) {
        Statistics_[pair.first].Merge(pair.second);
    }
}

void TStatistics::Clear()
{
    Statistics_.clear();
}

bool TStatistics::Empty() const
{
    return Statistics_.empty();
}

void Serialize(const TStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    NYTree::Serialize(statistics.Statistics(), consumer);
}

////////////////////////////////////////////////////////////////////

TStatisticsConvertor::TStatisticsConvertor(TStatisticsConsumer consumer)
    : Depth_(0)
    , Consumer_(consumer)
{ }

void TStatisticsConvertor::OnStringScalar(const TStringBuf& value)
{
    YUNREACHABLE();
}

void TStatisticsConvertor::OnIntegerScalar(i64 value)
{
    Statistics_.Add(LastKey_, TSummary(value));
}

void TStatisticsConvertor::OnDoubleScalar(double value)
{
    YUNREACHABLE();
}

void TStatisticsConvertor::OnEntity()
{
    YUNREACHABLE();
}

void TStatisticsConvertor::OnBeginList()
{
    YUNREACHABLE();
}

void TStatisticsConvertor::OnListItem()
{
    YCHECK(Statistics_.Empty());
}

void TStatisticsConvertor::OnEndList()
{
    YUNREACHABLE();
}

void TStatisticsConvertor::OnBeginMap()
{
    YCHECK(Depth_ == 0);
    ++Depth_;
}

void TStatisticsConvertor::OnKeyedItem(const TStringBuf& key)
{
    LastKey_ = key;
}

void TStatisticsConvertor::OnEndMap()
{
    YCHECK(Depth_ == 1);
    --Depth_;

    Consumer_.Run(Statistics_);
    Statistics_.Clear();
}

void TStatisticsConvertor::OnBeginAttributes()
{
    YUNREACHABLE();
}

void TStatisticsConvertor::OnEndAttributes()
{
    YUNREACHABLE();
}


////////////////////////////////////////////////////////////////////

} // NJobProxy
} // NYT
