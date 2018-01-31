#pragma once

#include "public.h"

#include <yt/core/misc/public.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error);

//! Catches exception thrown from underlying handler body and
//! translates it into HTTP error.
IHttpHandlerPtr WrapYTException(const IHttpHandlerPtr& underlying);

bool MaybeHandleCors(const IRequestPtr& req, const IResponseWriterPtr& rsp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
