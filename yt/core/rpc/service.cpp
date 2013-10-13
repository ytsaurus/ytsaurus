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

TServiceId::TServiceId()
{ }

TServiceId::TServiceId(const Stroka& serviceName, const TRealmId& realmId)
    : ServiceName(serviceName)
    , RealmId(realmId)
{ }

bool operator == (const TServiceId& lhs, const TServiceId& rhs)
{
    return lhs.ServiceName == rhs.ServiceName && lhs.RealmId == rhs.RealmId;
}

bool operator != (const TServiceId& lhs, const TServiceId& rhs)
{
    return !(lhs == rhs);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
