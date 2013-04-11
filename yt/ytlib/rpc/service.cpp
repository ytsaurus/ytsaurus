#include "stdafx.h"
#include "service.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

void IServiceContext::SetRequestInfo(const char* format, ... )
{
    Stroka info;
    va_list params;
    va_start(params, format);
    vsprintf(info, format, params);
    va_end(params);
    SetRequestInfo(info);
}

void IServiceContext::SetResponseInfo(const char* format, ...)
{
    Stroka info;
    va_list params;
    va_start(params, format);
    vsprintf(info, format, params);
    va_end(params);
    SetResponseInfo(info);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
