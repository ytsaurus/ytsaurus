#pragma once

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/misc/public.h>

#include <library/cpp/yt/yson_string/public.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct ITypeResolver
{
    virtual ~ITypeResolver() = default;

    virtual NTableClient::EValueType ResolveType(NYPath::TYPathBuf suffixPath = {}) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TTypedAttributePath
{
    NYPath::TYPath Path;
    const ITypeResolver* TypeResolver;
};

using TNonOwningAttributePayload = std::variant<TStringBuf, NYson::TYsonStringBuf>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IExpressionEvaluator)
DECLARE_REFCOUNTED_STRUCT(IFilterMatcher)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
