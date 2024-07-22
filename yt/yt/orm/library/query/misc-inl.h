#ifndef MISC_INL_H_
#error "Direct inclusion of this file is not allowed; include misc.h"
// For the sake of sane code completion.
#include "misc.h"
#endif

#include <yt/yt/library/query/base/ast.h>

namespace NYT::NOrm::NQuery {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

template <CStringOrLiteralValue TValue>
TString FormatValue(const TValue& value)
{
    if constexpr (std::same_as<TValue, NQueryClient::NAst::TLiteralValue>) {
        return NQueryClient::NAst::FormatLiteralValue(value);
    } else {
        return Format("%Qv", value);
    }
}

template <CStringOrLiteralValue TValue, class TFormatter>
TString FormatListImpl(const std::vector<TValue>& formatttableValues, const TFormatter& formatter)
{
    TStringBuilder builder;
    TDelimitedStringBuilderWrapper wrapper(&builder);

    builder.AppendString("(");
    for (const auto& value : formatttableValues) {
        wrapper->AppendString(formatter(value));
    }
    builder.AppendString(")");

    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

template <CString TReference, CStringOrLiteralValue TValue>
TString GenerateLexicographicalFilter(
    const std::vector<TReference>& references,
    const std::vector<TValue>& formattableValues,
    EOrderRelation orderRelation)
{
    YT_VERIFY(references.size() == formattableValues.size());
    YT_VERIFY(!references.empty());

    const auto* relationSymbol = std::invoke([&] {
        switch (orderRelation) {
            case EOrderRelation::Less:
                return "<";
            case EOrderRelation::LessOrEqual:
                return "<=";
            case EOrderRelation::Equal:
                return "=";
            case EOrderRelation::GreaterOrEqual:
                return ">=";
            case EOrderRelation::Greater:
                return ">";
        }
    });

    if (references.size() == 1) {
        return Format(
            "%v %v %v",
            NQueryClient::NAst::FormatId(references[0]),
            relationSymbol,
            NDetail::FormatValue(formattableValues[0]));
    }

    return Format(
        "%v %v %v",
        NDetail::FormatListImpl(references, NQueryClient::NAst::FormatId),
        relationSymbol,
        FormatList(formattableValues));
}

template <CString TReference, CStringOrLiteralValue TValue>
TString GenerateLexicographicalRangeFilter(
    const std::vector<TReference>& references,
    const std::vector<TValue>& leftBound,
    const std::vector<TValue>& rightBound,
    TRangeFilterOptions options)
{
    return JoinFilters({
        GenerateLexicographicalFilter(
            references,
            leftBound,
            options.IncludeLeft ? EOrderRelation::GreaterOrEqual : EOrderRelation::Greater),
        GenerateLexicographicalFilter(
            references,
            rightBound,
            options.IncludeRight ? EOrderRelation::LessOrEqual : EOrderRelation::Less),
    });
}

////////////////////////////////////////////////////////////////////////////////

template <CStringOrLiteralValue TValue>
TString FormatList(const std::vector<TValue>& formatttableValues)
{
    return NDetail::FormatListImpl(formatttableValues, NDetail::FormatValue<TValue>);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
