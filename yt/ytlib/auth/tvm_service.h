#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

//! Abstracts away TVM daemon.
//! See https://wiki.yandex-team.ru/passport/tvm2/tvm-daemon for API reference.
struct ITvmService
    : public virtual TRefCounted
{
    virtual TFuture<TString> GetTicket(const TString& serviceId) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITvmService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT
