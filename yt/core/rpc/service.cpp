#include "stdafx.h"
#include "service.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

void IServiceContext::SetRequestInfo()
{
    SetRawRequestInfo("");
}

void IServiceContext::SetResponseInfo()
{
    SetRawResponseInfo("");
}

void IServiceContext::ReplyFrom(TFuture<TSharedRefArray> message)
{
    auto this_ = MakeStrong(this);
    message.Subscribe(BIND([this, this_] (const TErrorOr<TSharedRefArray>& result) {
        if (result.IsOK()) {
            Reply(result.Value());
        } else {
            Reply(TError(result));
        }
    }));
}

void IServiceContext::ReplyFrom(TFuture<void> error)
{
    auto this_ = MakeStrong(this);
    error.Subscribe(BIND([this, this_] (const TError& error) {
        Reply(error);
    }));
}

////////////////////////////////////////////////////////////////////////////////

TServiceId::TServiceId()
{ }

TServiceId::TServiceId(const Stroka& serviceName, const TRealmId& realmId)
    : ServiceName(serviceName)
    , RealmId(realmId)
{ }

TServiceId::TServiceId(const char* serviceName, const TRealmId& realmId)
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

Stroka ToString(const TServiceId& serviceId)
{
    auto result = serviceId.ServiceName;
    if (!serviceId.RealmId.IsEmpty()) {
        result.append(':');
        result.append(ToString(serviceId.RealmId));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
