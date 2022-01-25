#pragma once

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/yt/yson_string/string.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct ITypeHandler
    : public virtual TRefCounted
{
    virtual std::optional<NObjectClient::TObjectId> CreateObject(
        NObjectClient::EObjectType type,
        const TCreateObjectOptions& options) = 0;
    virtual std::optional<NYson::TYsonString> GetNode(
        const NYPath::TYPath& path,
        const TGetNodeOptions& options) = 0;
    virtual std::optional<NYson::TYsonString> ListNode(
        const NYPath::TYPath& path,
        const TListNodeOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITypeHandler)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative

