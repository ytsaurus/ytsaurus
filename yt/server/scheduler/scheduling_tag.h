#pragma once

#include "public.h"

#include <yt/core/misc/dnf.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TSchedulingTagFilter
{
public:
    TSchedulingTagFilter();
    explicit TSchedulingTagFilter(const TDnfFormula& dnf);

    void Reload(const TDnfFormula& dnf);

    bool CanSchedule(const yhash_set<Stroka>& nodeTags) const;

    bool IsEmpty() const;

    size_t GetHash() const;
    const TDnfFormula& GetDnf() const;

private:
    TDnfFormula Dnf_;
    size_t Hash_;
};

bool operator==(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs);

extern const TSchedulingTagFilter EmptySchedulingTagFilter;

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

////////////////////////////////////////////////////////////////////

template <>
struct hash<NYT::NScheduler::TSchedulingTagFilter>
{
    size_t operator()(const NYT::NScheduler::TSchedulingTagFilter& filter) const;
};

////////////////////////////////////////////////////////////////////
