#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/yson/public.h>

namespace NYT::NOrm::NQuery {

////////////////////////////////////////////////////////////////////////////////

//! Filters objects by the given ORM filter.
/*!
 *  ORM filter is written in the dynamic tables query language,
 *  but contains attributes ([/spec/some/attribute]) instead of columns ([spec]).
 *
 *  It is applied to the list of attributes described by #attributePaths
 *  (e.g.: ["/spec", "/status/values"]; see #CreateFilterMatcher) and
 *  corresponding #attributeYsons (e.g.: ["{a=b}", "{v1=10;v2=30}"]; see #Match).
 */
struct IFilterMatcher
    : public TRefCounted
{
    virtual TErrorOr<bool> Match(
        const std::vector<TNonOwningAttributePayload>& attributePayloads,
        NTableClient::TRowBufferPtr rowBuffer = nullptr) = 0;

    //! Shortcut for the input vector of size 1.
    virtual TErrorOr<bool> Match(
        const TNonOwningAttributePayload& attributePayload,
        NTableClient::TRowBufferPtr rowBuffer = nullptr) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFilterMatcher)

////////////////////////////////////////////////////////////////////////////////

//! Thread-safe; exception-safe.
IFilterMatcherPtr CreateFilterMatcher(
    std::string filterQuery,
    std::vector<TTypedAttributePath> typedAttributePaths);

//! Shortcut for paths of type Any.
IFilterMatcherPtr CreateFilterMatcher(
    std::string filterQuery,
    std::vector<NYPath::TYPath> attributePaths = {""});

////////////////////////////////////////////////////////////////////////////////

IFilterMatcherPtr CreateConstantFilterMatcher(
    bool constant);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NQuery
