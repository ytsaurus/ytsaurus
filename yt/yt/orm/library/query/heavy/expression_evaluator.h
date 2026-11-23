#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/library/query/base/public.h>
#include <yt/yt/library/query/base/query_preparer.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct IExpressionEvaluator
    : public TRefCounted
{
    virtual TErrorOr<NQueryClient::TValue> Evaluate(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        const NTableClient::TRowBufferPtr& rowBuffer Y_LIFETIME_BOUND) const = 0;

    //! Shortcut for the input vector of size 1.
    virtual TErrorOr<NQueryClient::TValue> Evaluate(
        const TNonOwningAttributePayload& attributePayload,
        const NTableClient::TRowBufferPtr& rowBuffer Y_LIFETIME_BOUND) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IExpressionEvaluator)

////////////////////////////////////////////////////////////////////////////////

IExpressionEvaluatorPtr CreateExpressionEvaluator(
    TStringBuf query,
    std::vector<NQueryClient::TColumnSchema> columns);

IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(
    std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
    std::vector<TTypedAttributePath> typedAttributePaths,
    EExpressionEvaluatorMode mode = EExpressionEvaluatorMode::SingleThread);

//! Shortcut for paths of type Any.
IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(
    std::unique_ptr<NQueryClient::TParsedSource> parsedQuery,
    std::vector<NYPath::TYPath> attributePaths = {""},
    EExpressionEvaluatorMode mode = EExpressionEvaluatorMode::SingleThread);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
