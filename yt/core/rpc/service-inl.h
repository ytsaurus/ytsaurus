#ifndef SERVICE_INL_H_
#error "Direct inclusion of this file is not allowed, include service.h"
#endif
#undef SERVICE_INL_H_

#include <core/misc/format.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class... TArgs>
void IServiceContext::SetRequestInfo(const char* format, const TArgs&... args)
{
    if (GetLogger().IsEnabled(NLog::ELogLevel::Debug)) {
        SetRawRequestInfo(Format(format, args...));
    }
}

template <class... TArgs>
void IServiceContext::SetResponseInfo(const char* format, const TArgs&... args)
{
    if (GetLogger().IsEnabled(NLog::ELogLevel::Debug)) {
        SetRawResponseInfo(Format(format, args...));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
