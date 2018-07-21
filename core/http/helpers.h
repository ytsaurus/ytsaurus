#pragma once

#include "public.h"

#include <yt/core/misc/error.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error);

TError ParseYTError(const IResponsePtr& rsp, bool fromTrailers = false);

//! Catches exception thrown from underlying handler body and
//! translates it into HTTP error.
IHttpHandlerPtr WrapYTException(const IHttpHandlerPtr& underlying);

bool MaybeHandleCors(const IRequestPtr& req, const IResponseWriterPtr& rsp);

THashMap<TString, TString> ParseCookies(const TStringBuf& cookies);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
