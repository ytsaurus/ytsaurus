#include "scheduling_tag.h"

#include <yt/ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TSchedulingTagFilter::TSchedulingTagFilter()
    : Hash_(0)
{ }

TSchedulingTagFilter::TSchedulingTagFilter(const TBooleanFormula& formula)
    : BooleanFormula_(formula)
    , Hash_(BooleanFormula_.GetHash())
{ }

void TSchedulingTagFilter::Reload(const TBooleanFormula& formula)
{
    BooleanFormula_ = formula;
    Hash_ = BooleanFormula_.GetHash();
}

bool TSchedulingTagFilter::CanSchedule(const THashSet<TString>& nodeTags) const
{
    return BooleanFormula_.IsSatisfiedBy(nodeTags);
}

bool TSchedulingTagFilter::IsEmpty() const
{
    return BooleanFormula_.IsEmpty();
};

size_t TSchedulingTagFilter::GetHash() const
{
    return Hash_;
};

const TBooleanFormula& TSchedulingTagFilter::GetBooleanFormula() const
{
    return BooleanFormula_;
}

bool operator==(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs)
{
    return lhs.GetBooleanFormula() == rhs.GetBooleanFormula();
}

bool operator!=(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs)
{
    return !(lhs.GetBooleanFormula() == rhs.GetBooleanFormula());
}

const TSchedulingTagFilter EmptySchedulingTagFilter;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TSchedulingTagFilter& filter, NYson::IYsonConsumer* consumer)
{
    Serialize(filter.GetBooleanFormula(), consumer);
}

void Deserialize(TSchedulingTagFilter& filter, NYTree::INodePtr node)
{
    TBooleanFormula formula;
    Deserialize(formula, node);

    filter.Reload(formula);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

size_t hash<NYT::NScheduler::TSchedulingTagFilter>::operator()(const NYT::NScheduler::TSchedulingTagFilter& filter) const
{
    return filter.GetHash();
}

////////////////////////////////////////////////////////////////////////////////
