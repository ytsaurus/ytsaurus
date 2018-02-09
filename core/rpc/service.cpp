#include "service.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

void IServiceContext::SetRequestInfo()
{
    SetRawRequestInfo(TString());
}

void IServiceContext::SetResponseInfo()
{
    SetRawResponseInfo(TString());
}

void IServiceContext::ReplyFrom(TFuture<TSharedRefArray> asyncMessage)
{
    asyncMessage.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TSharedRefArray>& result) {
        if (result.IsOK()) {
            Reply(result.Value());
        } else {
            Reply(TError(result));
        }
    }));
    SubscribeCanceled(BIND([asyncMessage = std::move(asyncMessage)] () mutable {
        asyncMessage.Cancel();
    }));
}

void IServiceContext::ReplyFrom(TFuture<void> asyncError)
{
    asyncError.Subscribe(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
        Reply(error);
    }));
    SubscribeCanceled(BIND([asyncError = std::move(asyncError)] () mutable {
        asyncError.Cancel();
    }));
}

////////////////////////////////////////////////////////////////////////////////

TServiceId::TServiceId() = default;

TServiceId::TServiceId(const TString& serviceName, const TRealmId& realmId)
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

TString ToString(const TServiceId& serviceId)
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
