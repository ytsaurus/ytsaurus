#include <library/cpp/yt/string/enum.h>
#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string.h>
#include <library/cpp/yt/string/string_builder.h>

#include <library/cpp/string_utils/levenshtein_diff/levenshtein_diff.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

std::string CalculateEnumSuggestions(
    TStringBuf value,
    const std::span<const TStringBuf>& domainNames)
{
    // [{distance, candidateName} ...].
    std::vector<std::pair<size_t, TStringBuf>> candidates;
    candidates.reserve(domainNames.size());

    for (const auto& candidateName : domainNames) {
        candidates.emplace_back(NLevenshtein::Distance(value, candidateName), candidateName);
    }

    Sort(candidates);

    constexpr size_t MaxCandidatesSize = 5;
    candidates.resize(std::min(candidates.size(), MaxCandidatesSize));

    std::vector<TStringBuf> candidateNames;
    candidateNames.reserve(candidates.size());
    for (const auto& [distance, candidateName] : candidates) {
        candidateNames.push_back(candidateName);
    }

    // TSpecBoundFormatter("Qv") doesn't work as this.
    auto formatter = [] (TStringBuilderBase* builder, const TStringBuf& name) {
        builder->AppendChar('"');
        builder->AppendString(name); // There is nothing to escape in enum name.
        builder->AppendChar('"');
    };
    return JoinToString(candidateNames, formatter, ", ");
}

extern "C" TEnumSuggestionsCalculator TryGetEnumSuggestionsCalculator()
{
    return &CalculateEnumSuggestions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
