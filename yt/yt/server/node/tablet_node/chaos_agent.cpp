#include "chaos_agent.h"

#include "private.h"
#include "tablet.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <util/generic/cast.h>

namespace NYT::NTabletNode {

using namespace NApi;
using namespace NChaosClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TChaosAgent
    : public IChaosAgent
{
public:
    TChaosAgent(
        TTablet* tablet,
        const TReplicationCardToken& replicationCardToken,
        NNative::IConnectionPtr localConnection)
        : Tablet_(tablet)
        , ReplicationCardToken_(replicationCardToken)
        , Connection_(std::move(localConnection))
        , Logger(TabletNodeLogger
            .WithTag("%v", tablet->GetLoggingTag())
            .WithTag("ReplicationCardToken", replicationCardToken))
    { }

    void Enable() override
    {
        FiberFuture_ = BIND(&TChaosAgent::FiberMain, MakeWeak(this))
            .AsyncVia(Tablet_->GetEpochAutomatonInvoker())
            .Run();

        YT_LOG_INFO("Chaos agent fiber started");
    }

    void Disable() override
    {
        if (FiberFuture_) {
            FiberFuture_.Cancel(TError("Chaos agent disabled"));
            YT_LOG_INFO("Chaos agent fiber stopped");
        }
        FiberFuture_.Reset();
    }

private:
    TTablet* const Tablet_;
    const TReplicationCardToken ReplicationCardToken_;
    const NNative::IConnectionPtr Connection_;
    TReplicationCardPtr ReplicationCard_;

    const NLogging::TLogger Logger;

    TFuture<void> FiberFuture_;

    void FiberMain()
    {
        while (true) {
            NProfiling::TWallTimer timer;
            FiberIteration();
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(1) - timer.GetElapsedTime());
        }
    }

    void FiberIteration()
    {
        try {
            YT_LOG_DEBUG("Updating tablet replication card");

            const auto& replicationCardCache = Connection_->GetReplicationCardCache();

            ReplicationCard_ = WaitFor(replicationCardCache->GetReplicationCard({
                    .Token = ReplicationCardToken_,
                    .RequestHistory = true,
                    .RequestProgress = true
                })).ValueOrThrow();

            // TODO(savrus): Handle replica mode switch correctly:
            // If mode is sync check that current progress is greater or equal time the replica became sync. If not, we are in AsyncToSync transition.
            // AsyncToSync: catch up to current era, stop puller, start accepting writes.
            // SyncToAsync: stop accepting writes, check that all transactions from sync era are committed and activate puller.

            Tablet_->ReplicationCard() = ReplicationCard_;
            Tablet_->RuntimeData()->ReplicationEra = ReplicationCard_->Era;

            YT_LOG_DEBUG("Tablet replication card updated (ReplicationCard: %v)",
                *ReplicationCard_);
        } catch (std::exception& ex) {
            YT_LOG_DEBUG(ex, "Failed to update tablet replication card");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosAgentPtr CreateChaosAgent(
    TTablet* tablet,
    const TReplicationCardToken& replicationCardToken,
    NNative::IConnectionPtr localConnection)
{
    return New<TChaosAgent>(
        tablet,
        replicationCardToken,
        std::move(localConnection));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
