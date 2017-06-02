#include "scheduling_tag.h"

#include <yt/ytlib/scheduler/config.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

TSchedulingTagFilter::TSchedulingTagFilter()
    : Hash_(0)
{ }

TSchedulingTagFilter::TSchedulingTagFilter(const TDnfFormula& dnf)
    : Dnf_(dnf)
    , Hash_(Dnf_.GetHash())
{ }

void TSchedulingTagFilter::Reload(const TDnfFormula& dnf)
{
    Dnf_ = dnf;
    Hash_ = Dnf_.GetHash();
}

bool TSchedulingTagFilter::CanSchedule(const yhash_set<TString>& nodeTags) const
{
    return Dnf_.IsSatisfiedBy(nodeTags);
}

bool TSchedulingTagFilter::IsEmpty() const
{
    return Dnf_.Clauses().empty();
};

size_t TSchedulingTagFilter::GetHash() const
{
    return Hash_;
};

const TDnfFormula& TSchedulingTagFilter::GetDnf() const
{
    return Dnf_;
}

bool operator==(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs)
{
    return lhs.GetDnf() == rhs.GetDnf();
}

const TSchedulingTagFilter EmptySchedulingTagFilter;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

////////////////////////////////////////////////////////////////////////////////

size_t hash<NYT::NScheduler::TSchedulingTagFilter>::operator()(const NYT::NScheduler::TSchedulingTagFilter& filter) const
{
    return filter.GetHash();
}

////////////////////////////////////////////////////////////////////////////////
