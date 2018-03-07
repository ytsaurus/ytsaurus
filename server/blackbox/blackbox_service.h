#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/node.h>

namespace NYT {
namespace NBlackbox {

////////////////////////////////////////////////////////////////////////////////

//! Abstracts away Blackbox.
//! See https://doc.yandex-team.ru/blackbox/ for API reference.
struct IBlackboxService
    : public virtual TRefCounted
{
    virtual TFuture<NYTree::INodePtr> Call(
        const TString& method,
        const THashMap<TString, TString>& params) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlackboxService)

DEFINE_ENUM(EBlackboxStatusId,
    ((Valid)(0))
    ((NeedReset)(1))
    ((Expired)(2))
    ((NoAuth)(3))
    ((Disabled)(4))
    ((Invalid)(5))
);

DEFINE_ENUM(EBlackboxExceptionId,
    ((Ok)(0))
    ((Unknown)(1))
    ((InvalidParameters)(2))
    ((DbFetchFailed)(9))
    ((DbException)(10))
    ((AccessDenied)(21))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
