#include "rpc_proxy_channel.h"
#include "rpc_proxy_connection.h"

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/roaming_channel.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NRpcProxy {

using namespace NApi;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TProxyChannelProvider
    : public IRoamingChannelProvider
{
public:
    TProxyChannelProvider(
        TRpcProxyConnectionPtr connection,
        const TClientOptions& options)
        : Connection_(std::move(connection))
        , Options_(options)
        , EndpointDescription_("RpcProxy")
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("rpc_proxy").Value(true)
            .EndMap()))
    { }

    ~TProxyChannelProvider()
    {
        if (CachedChannel_) {
            DestroyChannel();
        }
    }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return MakeFuture(GetChannel());
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        IChannelPtr channel;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (CachedChannel_) {
                channel = std::move(CachedChannel_);
                CachedChannel_ = nullptr;
            }
        }
        if (channel) {
            return BIND(&TProxyChannelProvider::DestroyChannel, MakeStrong(this))
                .AsyncVia(Connection_->GetInvoker())
                .Run()
                .Apply(BIND([channel, error] () {
                    return channel->Terminate(error);
                }));
        }
        return VoidFuture;
    }

private:
    const TRpcProxyConnectionPtr Connection_;
    const TClientOptions Options_;
    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

    TSpinLock SpinLock_;
    mutable IChannelPtr CachedChannel_;

    const IChannelPtr& GetChannel() const
    {
        TGuard<TSpinLock> guard(SpinLock_);
        if (!CachedChannel_) {
            CachedChannel_ = CreateChannel();
        }
        return CachedChannel_;
    }

    IChannelPtr CreateChannel() const
    {
        return Connection_->CreateChannelAndRegister(Options_, const_cast<TProxyChannelProvider*>(this));
    }

    void DestroyChannel() const
    {
        Connection_->Unregister(const_cast<TProxyChannelProvider*>(this));
    }
};

////////////////////////////////////////////////////////////////////////////////

IChannelPtr CreateRpcProxyChannel(TRpcProxyConnectionPtr connection, const TClientOptions& options)
{
    auto provider = New<TProxyChannelProvider>(std::move(connection), options);
    auto channel = CreateRoamingChannel(std::move(provider));
    return channel;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
