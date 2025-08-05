#include "scheduling_tag.h"

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/controller_agent/proto/controller_agent_service.pb.h>

namespace NYT::NScheduler {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TSchedulingTagFilter::TSchedulingTagFilter()
    : Hash_(BooleanFormula_.GetHash())
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

bool TSchedulingTagFilter::CanSchedule(const TBooleanFormulaTags& nodeTags) const
{
    return BooleanFormula_.IsSatisfiedBy(nodeTags);
}

bool TSchedulingTagFilter::IsEmpty() const
{
    return BooleanFormula_.IsEmpty();
}

size_t TSchedulingTagFilter::GetHash() const
{
    return Hash_;
}

const TBooleanFormula& TSchedulingTagFilter::GetBooleanFormula() const
{
    return BooleanFormula_;
}

const TSchedulingTagFilter EmptySchedulingTagFilter;

bool operator==(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs)
{
    return lhs.GetBooleanFormula() == rhs.GetBooleanFormula();
}

TSchedulingTagFilter operator&(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs)
{
    return TSchedulingTagFilter(lhs.GetBooleanFormula() & rhs.GetBooleanFormula());
}

TSchedulingTagFilter operator|(const TSchedulingTagFilter& lhs, const TSchedulingTagFilter& rhs)
{
    return TSchedulingTagFilter(lhs.GetBooleanFormula() | rhs.GetBooleanFormula());
}

TSchedulingTagFilter operator!(const TSchedulingTagFilter& filter)
{
    return TSchedulingTagFilter(!filter.GetBooleanFormula());
}

void ToProto(TProtobufString* protoFilter, const TSchedulingTagFilter& filter)
{
    *protoFilter = filter.GetBooleanFormula().GetFormula();
}

void FromProto(TSchedulingTagFilter* filter, const TProtobufString& protoFilter)
{
    *filter = TSchedulingTagFilter(MakeBooleanFormula(protoFilter));
}

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

void Deserialize(TSchedulingTagFilter& filter, NYson::TYsonPullParserCursor* cursor)
{
    TBooleanFormula formula;
    Deserialize(formula, cursor);

    filter.Reload(formula);
}

void FormatValue(TStringBuilderBase* builder, const TSchedulingTagFilter& filter, TStringBuf spec)
{
    FormatValue(builder, filter.GetBooleanFormula().GetFormula(), spec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NScheduler::TSchedulingTagFilter>::operator()(const NYT::NScheduler::TSchedulingTagFilter& filter) const
{
    return filter.GetHash();
}

////////////////////////////////////////////////////////////////////////////////
