#pragma once

#include "public.h"

#include <library/cpp/yt/string/format_arg.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EOrderRelation,
    (Less)
    (LessOrEqual)
    (Equal)
    (GreaterOrEqual)
    (Greater)
);

struct TRangeFilterOptions
{
    bool IncludeLeft = true;
    bool IncludeRight = false;
};

////////////////////////////////////////////////////////////////////////////////

TString GenerateLexicographicalFilter(
    std::span<const TString> references,
    std::span<const TString> formattedValues,
    EOrderRelation orderRelation);

TString GenerateLexicographicalRangeFilter(
    std::span<const TString> references,
    std::optional<std::span<const TString>> formattedLeftBound,
    std::optional<std::span<const TString>> formattedRightBound,
    TRangeFilterOptions options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
