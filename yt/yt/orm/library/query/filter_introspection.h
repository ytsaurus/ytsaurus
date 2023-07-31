#pragma once

#include "public.h"

#include <yt/yt/library/query/base/ast.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

//! Simplifies `std::optional<std::variant>` interface.
//! Usages: `Introspect(...).TryMoveAs<i64>()`, `Introspect(...).Value`.
struct TOptionalLiteralValueWrapper
{
    std::optional<NQueryClient::NAst::TLiteralValue> Value;

    TOptionalLiteralValueWrapper(std::nullopt_t)
        : Value(std::nullopt)
    { }

    TOptionalLiteralValueWrapper(NQueryClient::NAst::TLiteralValue value)
        : Value(std::move(value))
    { }

    explicit operator bool() const
    {
        return Value.has_value();
    }

    bool operator==(const TOptionalLiteralValueWrapper& other) const
    {
        return Value == other.Value;
    }

    template <class T>
    std::optional<T> TryMoveAs()
    {
        if (Value) {
            if (auto* typedValue = std::get_if<T>(&Value.value())) {
                return std::move(*typedValue);
            }
        }
        return std::nullopt;
    }
};

//! Introspects #filterQuery, trying to find constraint of the type
/*! "[#attributePath] = <value>" which holds for every object matching the #filterQuery.
 *
 *  Best-effort: return value of null says nothing about #filterQuery.
 *
 *  Throws for an invalid #attributePath or #filterQuery.
 *
 *  Use cases:
 *  - Validate that #filterQuery matches only objects with the given `[/meta/parent_id]` attribute value;
 *  - Infer attribute value `[/meta/parent_id]` to apply the filter to a restricted set of rows.
 */
TOptionalLiteralValueWrapper IntrospectFilterForDefinedAttributeValue(
    const TString& filterQuery,
    const NYPath::TYPath& attributePath);

//! Introspects #filterExpression, trying to find non-trivial reference to #referenceName,
/*! i.e. such reference that affects the result of selection by this filter.
 *
 *  Only reference expressions, binary operations and function `in` are considered for now.
 *
 *  #referenceName is an attribute path or a column name.
 *
 *  If #allowValueRange is false, only operator `=` is considered, which effectively means that
 *  the referenced attribute value must belong to a fixed set of values to match the filter.
 *
 *  Use case: verify whether the index field is present in query and affects it,
 *  so the query can be executed using index without loss of rows, which are not present in the index.
 */
bool IntrospectFilterForDefinedReference(
    NQueryClient::NAst::TExpressionPtr filterExpression,
    const NYPath::TYPath& referenceName,
    const std::optional<TString>& tableName,
    bool allowValueRange);

//! Searches for attribute references in #filterQuery, e.g. expressions
/*! of the form `[attributePath]`.
 *
 *  Throws for an invalid #filterQuery.
 */
void ExtractFilterAttributeReferences(const TString& filterQuery, std::function<void(TString)> inserter);

////////////////////////////////////////////////////////////////////////////////

//! Check whether #exprList is a reference to the attribute specified in #attributePath.
bool IsAttributeReference(
    const NQueryClient::NAst::TExpressionList& exprList,
    const NYPath::TYPath& attributePath) noexcept;

////////////////////////////////////////////////////////////////////////////////

//! If #exprList is a string literal value, return it, otherwise return null.
std::optional<TString> TryCastToStringValue(
    const NQueryClient::NAst::TExpressionList& exprList) noexcept;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
