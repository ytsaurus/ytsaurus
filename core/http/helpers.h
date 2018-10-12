#pragma once

#include "public.h"

#include <yt/core/misc/error.h>

#include <yt/core/yson/public.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

void FillYTErrorHeaders(const IResponseWriterPtr& rsp, const TError& error);

TError ParseYTError(const IResponsePtr& rsp, bool fromTrailers = false);

//! Catches exception thrown from underlying handler body and
//! translates it into HTTP error.
IHttpHandlerPtr WrapYTException(const IHttpHandlerPtr& underlying);

bool MaybeHandleCors(const IRequestPtr& req, const IResponseWriterPtr& rsp);

THashMap<TString, TString> ParseCookies(TStringBuf cookies);

void ProtectCsrfToken(const IResponseWriterPtr& rsp);

TNullable<TString> GetBalancerRequestId(const IRequestPtr& req);

TNullable<TString> GetBalancerRealIP(const IRequestPtr& req);

void ReplyJson(const IResponseWriterPtr& rsp, std::function<void(NYson::IYsonConsumer*)> producer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
