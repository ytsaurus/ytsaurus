#pragma once

#include <yt/core/http/public.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

// We can't put this helpers into core/http because of dependency on json.
void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error);

//! Catches exception thrown from underlying handler body and
//! translates it into HTTP error.
IHttpHandlerPtr WrapYTException(const IHttpHandlerPtr& underlying);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
