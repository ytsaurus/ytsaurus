#include "connection.h"

#include "private.h"

#include "client.h"
#include "ground_channel_wrapper.h"
#include "sequoia_reign.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NSequoiaClient {

using namespace NApi::NNative;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

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

        TSequoiaConnectionConfigPtr appliedConfig;
        while (true) {
            appliedConfig = Config_.Acquire();
            auto groundClientFuture = CreateGroundClientFuture(appliedConfig);

            auto guard = WriterGuard(Lock_);
            if (appliedConfig == Config_.Acquire()) {
                DoReconfigure(std::move(groundClientFuture));
                break;
            }
        }

        OnReconfigured_.Fire(appliedConfig);
        YT_TLOG_DEBUG("Sequoia connection reconfigured");
    }

    ISequoiaClientPtr CreateClient(const TAuthenticationIdentity& authenticationIdentity) override
    {
        auto groundClientFuture = GetGroundClientFuture();
        auto localConnection = LocalConnection_.Lock();
        if (!localConnection) {
            THROW_ERROR_EXCEPTION("Local connection has been destroyed");
        }

        return CreateSequoiaClient(
            localConnection->CreateNativeClient(TClientOptions::FromAuthenticationIdentity(authenticationIdentity)),
            std::move(groundClientFuture));
    }

    TInstant GetLastReconfigurationTime() const override
    {
        return LastReconfigurationTime_.load(std::memory_order::relaxed);
    }

    DEFINE_SIGNAL_OVERRIDE(void(const TSequoiaConnectionConfigPtr&), OnReconfigured);

private:
    const TWeakPtr<IConnection> LocalConnection_;
    TAtomicIntrusivePtr<TSequoiaConnectionConfig> Config_;

    YT_DECLARE_SPIN_LOCK(TReaderWriterSpinLock, Lock_);
    TFuture<IClientPtr> GroundClientFuture_;
    std::atomic<TInstant> LastReconfigurationTime_ = {};

    TFuture<IClientPtr> GetGroundClientFuture()
    {
        TFuture<IClientPtr> observedFuture;
        {
            auto guard = ReaderGuard(Lock_);
            if (GroundClientFuture_ && (!GroundClientFuture_.IsSet() || GroundClientFuture_.GetOrCrash().IsOK())) {
                return GroundClientFuture_;
            }
            observedFuture = GroundClientFuture_;
        }

        while (true) {
            auto config = Config_.Acquire();
            auto groundClientFuture = CreateGroundClientFuture(config);

            auto guard = WriterGuard(Lock_);
            if (GroundClientFuture_ != observedFuture) {
                return GroundClientFuture_;
            }
            if (config != Config_.Acquire()) {
                continue;
            }

            DoReconfigure(std::move(groundClientFuture));
            return GroundClientFuture_;
        }
    }

    void DoReconfigure(TFuture<IClientPtr>&& groundClientFuture)
    {
        YT_ASSERT_WRITER_SPINLOCK_AFFINITY(Lock_);
        std::swap(GroundClientFuture_, groundClientFuture);
        LastReconfigurationTime_.store(TInstant::Now(), std::memory_order::relaxed);
    }

    TFuture<IClientPtr> CreateGroundClientFuture(const TSequoiaConnectionConfigPtr& config) const
    {
        if (!config) {
            return MakeFuture<IClientPtr>(TError("Sequoia is not configured"));
        }

        auto localConnection = LocalConnection_.Lock();
        if (!localConnection) {
            YT_TLOG_INFO("Sequoia connection finds local connection destroyed while creating ground client");
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
