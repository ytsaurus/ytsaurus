#include "connection.h"

#include "private.h"

#include "client.h"
#include "ground_channel_wrapper.h"
#include "sequoia_reign.h"

#include <yt/yt/ytlib/api/native/client_cache.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NSequoiaClient {

using namespace NApi::NNative;
using namespace NRpc;

///////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = SequoiaClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TSequoiaConnection
    : public ISequoiaConnection
{
public:
    TSequoiaConnection(
        TSequoiaConnectionConfigPtr config,
        TWeakPtr<IConnection> localConnection)
        : LocalConnection_(std::move(localConnection))
        , Config_(std::move(config))
    { }

    void Reconfigure(TSequoiaConnectionConfigPtr config) override
    {
        Config_.Store(std::move(config));
        DoReconfigure();
        YT_LOG_DEBUG("Sequoia connection reconfigured");
    }

    ISequoiaClientPtr CreateClient(const TAuthenticationIdentity& authenticationIdentity) override
    {
        // NB: this lazily initializes the connection.
        // This should not be done sooner because cluster directory synchronizer
        // may take some time to provide us with a ground connection.
        auto groundClientFuture = ReadGroundClientFuture();

        // TODO(shakurov): consider caching Sequoia clients here wholesale.
        return CreateSequoiaClient(
            GetOrCreateAuthenticatedLocalClient(authenticationIdentity),
            std::move(groundClientFuture));
    }

private:
    const TWeakPtr<IConnection> LocalConnection_;
    TAtomicIntrusivePtr<TSequoiaConnectionConfig> Config_;

    // Protects GroundClientFuture_ and ClientCache_.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    TFuture<IClientPtr> GroundClientFuture_;
    TClientCachePtr ClientCache_;

    void Initialize()
    {
        DoReconfigure();
        YT_LOG_DEBUG("Sequoia connection initialized");
    }

    void DoReconfigure()
    {
        auto config = Config_.Acquire();

        auto groundClientFuture = CreateGroundClientFuture(config);
        auto clientCache = CreateClientCache(config);

        {
            auto guard = WriterGuard(Lock_);
            GroundClientFuture_ = std::move(groundClientFuture);
            ClientCache_ = std::move(clientCache);
        }
    }

    IClientPtr GetOrCreateAuthenticatedLocalClient(const TAuthenticationIdentity& identity)
    {
        TClientCachePtr clientCache;
        {
            auto guard = ReaderGuard(Lock_);
            clientCache = ClientCache_;
        }

        if (Y_UNLIKELY(!clientCache)) {
            auto localConnection = LocalConnection_.Lock();
            if (!localConnection) {
                THROW_ERROR_EXCEPTION("Sequoia connection finds local connection destroyed while creating authenticated local client");
            }
            return localConnection->CreateNativeClient(
                TClientOptions::FromAuthenticationIdentity(identity));
        }

        return clientCache->Get(
            identity,
            TClientOptions::FromAuthenticationIdentity(identity));
    }

    TFuture<IClientPtr> ReadGroundClientFuture()
    {
        TFuture<IClientPtr> result;

        {
            auto guard = ReaderGuard(Lock_);
            result = GroundClientFuture_;
        }

        if (result && (!result.IsSet() || result.Get().IsOK())) {
            return result;
        }

        // It's possible for this to be called multiple times if
        //   - we have trouble successfully initializing the ground client, or
        //   - there's a race between multiple lazy initializations.
        // Both cases are ok.
        Initialize();

        {
            auto guard = ReaderGuard(Lock_);
            result = GroundClientFuture_;
        }

        return result;
    }

    TFuture<IClientPtr> CreateGroundClientFuture(const TSequoiaConnectionConfigPtr& config) const
    {
        if (!config) {
            return MakeFuture<IClientPtr>(TError("Sequoia is not configured"));
        }

        auto localConnection = LocalConnection_.Lock();
        if (!localConnection) {
            YT_LOG_INFO("Sequoia connection finds local connection destroyed while creating ground client");
            return MakeFuture<IClientPtr>(TError("Local connection has been destroyed"));
        }

        auto result = InsistentGetRemoteConnection(localConnection, config->GroundClusterName)
            .AsUnique().Apply(BIND([] (IConnectionPtr&& groundConnection) -> IClientPtr {
                auto options = TClientOptions::Root();
                options.ChannelWrapper = BIND_NO_PROPAGATE(NSequoiaClient::WrapGroundChannel);
                options.ChannelFactoryWrapper = BIND_NO_PROPAGATE(NSequoiaClient::WrapGroundChannelFactory);
                return groundConnection->CreateNativeClient(options);
            }));

        if (config->EnableGroundReignValidation) {
            result = result
                .AsUnique().Apply(BIND([config = std::move(config)] (IClientPtr&& client) {
                    return ValidateClusterGroundReign(client, config->SequoiaRootPath)
                        .Apply(BIND([=] { return client; }));
                }));
        }

        return result;
    }

    TClientCachePtr CreateClientCache(const TSequoiaConnectionConfigPtr& config) const
    {
        if (!config) {
            return nullptr;
        }

        auto localConnection = LocalConnection_.Lock();
        if (!localConnection) {
            YT_LOG_INFO("Sequoia connection finds local connection destroyed while creating client cache");
            return nullptr;
        }

        return New<TClientCache>(
            config->ClientCache ? config->ClientCache : New<TSlruCacheConfig>(),
            std::move(localConnection));
    }
};

////////////////////////////////////////////////////////////////////////////////.

ISequoiaConnectionPtr CreateSequoiaConnection(
    TSequoiaConnectionConfigPtr config,
    TWeakPtr<IConnection> localConnection)
 {
    return New<TSequoiaConnection>(std::move(config), std::move(localConnection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
