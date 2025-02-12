#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

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

template <class T>
concept CString = std::same_as<TString, T> || std::same_as<std::string, T> || std::same_as<TStringBuf, T>;

template <class T>
concept CStringOrLiteralValue = CString<T> || std::same_as<NQueryClient::NAst::TLiteralValue, T>;

////////////////////////////////////////////////////////////////////////////////

template <CString TReference, CStringOrLiteralValue TValue>
TString GenerateLexicographicalFilter(
    const std::vector<TReference>& references,
    const std::vector<TValue>& values,
    EOrderRelation orderRelation);

template <CString TReference, CStringOrLiteralValue TValue>
TString GenerateLexicographicalRangeFilter(
    const std::vector<TReference>& references,
    const std::vector<TValue>& leftBound,
    const std::vector<TValue>& rigtBound,
    TRangeFilterOptions options = {});

////////////////////////////////////////////////////////////////////////////////

template <CStringOrLiteralValue TValue>
TString FormatList(const std::vector<TValue>& values);

TString JoinFilters(const std::vector<TString>& filters);

bool IsTargetReference(
    const NQueryClient::NAst::TExpressionList& exprs,
    const NQueryClient::NAst::TReference& reference);

bool IsAnyExprATargetReference(
    const NQueryClient::NAst::TExpressionList& exprs,
    const NQueryClient::NAst::TReference& reference);

bool IsSingleConstant(const NQueryClient::NAst::TExpressionList& exprs);

std::vector<NQueryClient::NAst::TReference> ExtractAllReferences(const NQueryClient::NAst::TExpressionList& exprs);

std::optional<NQueryClient::NAst::TReference> TryExtractReference(const NQueryClient::NAst::TExpressionList& exprs);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery

#define MISC_INL_H_
#include "misc-inl.h"
#undef MISC_INL_H_
