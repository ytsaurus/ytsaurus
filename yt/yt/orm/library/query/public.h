#pragma once

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct TTypedAttributePath
{
    TString Path;
    NTableClient::EValueType Type;
};

using TNonOwningAttributePayload = std::variant<TStringBuf, NYson::TYsonStringBuf>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IExpressionEvaluator)
DECLARE_REFCOUNTED_STRUCT(IFilterMatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
