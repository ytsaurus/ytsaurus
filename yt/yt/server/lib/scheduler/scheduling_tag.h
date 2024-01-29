#pragma once

#include "public.h"

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt_proto/yt/core/misc/proto/guid.pb.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TSchedulingTagFilter
{
public:
    TSchedulingTagFilter();
    explicit TSchedulingTagFilter(const TBooleanFormula& formula);

    void Reload(const TBooleanFormula& formula);

    bool CanSchedule(const TBooleanFormulaTags& nodeTags) const;

    bool IsEmpty() const;

    size_t GetHash() const;
    const TBooleanFormula& GetBooleanFormula() const;

private:
    TBooleanFormula BooleanFormula_;
    size_t Hash_;
};

extern const TSchedulingTagFilter EmptySchedulingTagFilter;

bool operator==(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs);

TSchedulingTagFilter operator&(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs);
TSchedulingTagFilter operator|(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs);
TSchedulingTagFilter operator!(const TSchedulingTagFilter& filter);

void ToProto(TProtoStringType* protoFilter, const TSchedulingTagFilter& filter);
void FromProto(TSchedulingTagFilter* filter, const TProtoStringType& protoFilter);

void Serialize(const TSchedulingTagFilter& filter, NYson::IYsonConsumer* consumer);
void Deserialize(TSchedulingTagFilter& filter, NYTree::INodePtr node);
void Deserialize(TSchedulingTagFilter& filter, NYson::TYsonPullParserCursor* cursor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

template <>
struct THash<NYT::NScheduler::TSchedulingTagFilter>
{
    size_t operator()(const NYT::NScheduler::TSchedulingTagFilter& filter) const;
};

////////////////////////////////////////////////////////////////////////////////
