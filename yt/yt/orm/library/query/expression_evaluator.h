#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct IExpressionEvaluator
    : public TRefCounted
{
    virtual TErrorOr<NQueryClient::TValue> Evaluate(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        const NTableClient::TRowBufferPtr& rowBuffer) = 0;

    //! Shortcut for the input vector of size 1.
    virtual TErrorOr<NQueryClient::TValue> Evaluate(
        const TNonOwningAttributePayload& attributePayload,
        const NTableClient::TRowBufferPtr& rowBuffer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IExpressionEvaluator)

////////////////////////////////////////////////////////////////////////////////

//! Thread-safe; exception-safe.
IExpressionEvaluatorPtr CreateExpressionEvaluator(
    std::string query,
    std::vector<NQueryClient::TColumnSchema> columns);

IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(
    std::string query,
    std::vector<TTypedAttributePath> typedAttributePaths);

//! Shortcut for paths of type Any.
IExpressionEvaluatorPtr CreateOrmExpressionEvaluator(
    std::string query,
    std::vector<TString> attributePaths = {""});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
