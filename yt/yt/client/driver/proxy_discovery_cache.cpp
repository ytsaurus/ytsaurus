#include "proxy_discovery_cache.h"

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

#include <util/digest/multi.h>

namespace NYT::NDriver {

using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NApi;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool TProxyDiscoveryRequest::operator==(const TProxyDiscoveryRequest& other) const
{
    return
        Type == other.Type &&
        Role == other.Role;
}

bool TProxyDiscoveryRequest::operator!=(const TProxyDiscoveryRequest& other) const
{
    return !(*this == other);
}

TProxyDiscoveryRequest::operator size_t() const
{
    return MultiHash(
        Type,
        Role);
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TProxyDiscoveryRequest& request, TStringBuf /*spec*/)
{
    builder->AppendFormat("{Type: %v, Role: %v}",
        request.Type,
        request.Role);
}

TString ToString(const TProxyDiscoveryRequest& request)
{
    return ToStringViaBuilder(request);
}

////////////////////////////////////////////////////////////////////////////////

class TProxyDiscoveryCache
    : public TAsyncExpiringCache<TProxyDiscoveryRequest, TProxyDiscoveryResponse>
    , public IProxyDiscoveryCache
{
public:
    TProxyDiscoveryCache(
        TAsyncExpiringCacheConfigPtr config,
        IClientPtr client)
        : TAsyncExpiringCache(
            std::move(config),
            DriverLogger.WithTag("Cache: ProxyDiscovery"))
        , Client_(std::move(client))
    { }

    virtual TFuture<TProxyDiscoveryResponse> Discover(
        const TProxyDiscoveryRequest& request) override
    {
        return Get(request);
    }

private:
    const IClientPtr Client_;

    virtual TFuture<TProxyDiscoveryResponse> DoGet(
        const TProxyDiscoveryRequest& request,
        bool /*isPeriodicUpdate*/) noexcept override
    {
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::LocalCache;
        options.Attributes = {BannedAttributeName, RoleAttributeName};

        auto path = GetProxyRegistryPath(request.Type);

        return Client_->GetNode(path, options)
            .Apply(BIND([=] (const TYsonString& yson) {
                TProxyDiscoveryResponse response;
                for (const auto& [proxyAddress, proxyNode] : ConvertTo<THashMap<TString, IMapNodePtr>>(yson)) {
                    if (!proxyNode->FindChild(AliveNodeName)) {
                        continue;
                    }

                    if (proxyNode->Attributes().Get(BannedAttributeName, false)) {
                        continue;
                    }

                    if (proxyNode->Attributes().Get<TString>(RoleAttributeName, DefaultProxyRole) != request.Role) {
                        continue;
                    }

                    response.Addresses.push_back(proxyAddress);
                }
                return response;
            }).AsyncVia(Client_->GetConnection()->GetInvoker()));
    }

    static TYPath GetProxyRegistryPath(EProxyType type)
    {
        switch (type) {
            case EProxyType::Rpc:
                return RpcProxiesPath;
            case EProxyType::Grpc:
                return GrpcProxiesPath;
            default:
                THROW_ERROR_EXCEPTION("Proxy type %Qlv is not supported",
                    type);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IProxyDiscoveryCachePtr CreateProxyDiscoveryCache(
    TAsyncExpiringCacheConfigPtr config,
    IClientPtr client)
{
    return New<TProxyDiscoveryCache>(
        std::move(config),
        std::move(client));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
