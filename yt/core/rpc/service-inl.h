#pragma once
#ifndef SERVICE_INL_H_
#error "Direct inclusion of this file is not allowed, include service.h"
// For the sake of sane code completion.
#include "service.h"
#endif

#include <yt/core/misc/format.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
void IServiceContext::SetRequestInfo(const char* format, const TArgs&... args)
{
    if (GetLogger().IsLevelEnabled(NLogging::ELogLevel::Debug)) {
        SetRawRequestInfo(Format(format, args...));
    }
}

template <class... TArgs>
void IServiceContext::SetResponseInfo(const char* format, const TArgs&... args)
{
    if (GetLogger().IsLevelEnabled(NLogging::ELogLevel::Debug)) {
        SetRawResponseInfo(Format(format, args...));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
