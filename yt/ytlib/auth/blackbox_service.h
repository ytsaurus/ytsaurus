#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Abstracts away Blackbox.
//! See https://doc.yandex-team.ru/blackbox/ for API reference.
struct IBlackboxService
    : public virtual TRefCounted
{
    virtual TFuture<NYTree::INodePtr> Call(
        const TString& method,
        const THashMap<TString, TString>& params) = 0;

    virtual TErrorOr<TString> GetLogin(const NYTree::INodePtr& reply) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlackboxService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
