#include "lexicographical_order.h"

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

TString GenerateLexicographicalFilter(
    std::span<const TString> references,
    std::span<const TString> formattedValues,
    EOrderRelation orderRelation)
{
    YT_VERIFY(references.size() == formattedValues.size());
    YT_VERIFY(!references.empty());

    if (references.size() == 1) {
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
        return Format("[%v] %v %v", references[0], relationSymbol, formattedValues[0]);
    }

    auto generateForPrefix = [&] (auto orderRelation) {
        return GenerateLexicographicalFilter(
            references.subspan(0, 1),
            formattedValues.subspan(0, 1),
            orderRelation);
    };
    auto generateForSuffix = [&] (auto orderRelation) {
        return GenerateLexicographicalFilter(
            references.subspan(1),
            formattedValues.subspan(1),
            orderRelation);
    };

    if (orderRelation == EOrderRelation::Equal) {
        return Format(
            "%v AND %v",
            generateForPrefix(orderRelation),
            generateForSuffix(orderRelation));
    }

    auto strictRelation = std::invoke([&] {
        switch (orderRelation) {
            case EOrderRelation::Less:
            case EOrderRelation::LessOrEqual:
                return EOrderRelation::Less;
            case EOrderRelation::GreaterOrEqual:
            case EOrderRelation::Greater:
                return EOrderRelation::Greater;
            default:
                YT_ABORT();
        }
    });
    return Format(
        "%v OR (%v AND (%v))",
        generateForPrefix(strictRelation),
        generateForPrefix(EOrderRelation::Equal),
        generateForSuffix(orderRelation));
}

TString GenerateLexicographicalRangeFilter(
    std::span<const TString> references,
    std::optional<std::span<const TString>> formattedLeftBound,
    std::optional<std::span<const TString>> formattedRightBound,
    TRangeFilterOptions options)
{
    std::vector<TString> clauses;
    if (formattedLeftBound.has_value()) {
        clauses.push_back(GenerateLexicographicalFilter(
            references,
            *formattedLeftBound,
            options.IncludeLeft ? EOrderRelation::GreaterOrEqual : EOrderRelation::Greater));
    }
    if (formattedRightBound.has_value()) {
        clauses.push_back(GenerateLexicographicalFilter(
            references,
            *formattedRightBound,
            options.IncludeRight ? EOrderRelation::LessOrEqual : EOrderRelation::Less));
    }

    TStringBuilder builder;
    TDelimitedStringBuilderWrapper wrapper(&builder, " AND ");
    for (const auto& clause : clauses) {
        wrapper->AppendFormat("(%v)", clause);
    }
    return builder.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
