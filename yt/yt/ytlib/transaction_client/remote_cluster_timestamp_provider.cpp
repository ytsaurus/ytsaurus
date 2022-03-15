#include "remote_cluster_timestamp_provider.h"

#include "public.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/config.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/transaction_client/config.h>
#include <yt/yt/client/transaction_client/timestamp_provider_base.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTransactionClient {

using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRemoteClusterTimestampProvider
    : public TTimestampProviderBase
{
public:
    TRemoteClusterTimestampProvider(
        IConnectionPtr nativeConnection,
        TCellTag clockClusterTag,
        NLogging::TLogger logger)
        : TTimestampProviderBase(GetLatestTimestampUpdatePeriod(nativeConnection))
        , NativeConnection_(std::move(nativeConnection))
        , ClockClusterTag_(clockClusterTag)
        , Logger(std::move(logger))
    {

        if (ClockClusterTag_ == NativeConnection_->GetClusterTag()) {
            Underlying_.Store(NativeConnection_->GetTimestampProvider());
            return;
        }

        NativeConnection_->GetClusterDirectorySynchronizer()->Sync()
            .Subscribe(BIND(&TRemoteClusterTimestampProvider::OnClusterDirectorySync, MakeWeak(this)));
    }

protected:
    TFuture<TTimestamp> DoGenerateTimestamps(int count) override
    {
        if (auto underlying = Underlying_.Load()) {
            return underlying->GenerateTimestamps(count);
        }

        return MakeFuture<TTimestamp>(TError(
            "Timestamp provider for clock cluster tag %v is unavailable at the moment",
            ClockClusterTag_));
    }

private:
    const IConnectionPtr NativeConnection_;
    const TCellTag ClockClusterTag_;

    const NLogging::TLogger Logger;

    TAtomicObject<ITimestampProviderPtr> Underlying_;

    void InitializeUnderlying()
    {
        if (auto connection = FindRemoteConnection(NativeConnection_, ClockClusterTag_)) {
            Underlying_.Store(connection->GetTimestampProvider());
            return;
        }

        auto retryTime = NativeConnection_->GetConfig()->ClusterDirectorySynchronizer->SyncPeriod;

        YT_LOG_DEBUG("Cannot initialize timestamp provider: cluster is not known; retrying "
            "(ClockClusterTag: %v, RetryTime: %v)",
            ClockClusterTag_,
            retryTime);

        TDelayedExecutor::Submit(
            BIND(&TRemoteClusterTimestampProvider::InitializeUnderlying, MakeWeak(this)),
            retryTime);
    }

    void OnClusterDirectorySync(const TError& /*error*/)
    {
        InitializeUnderlying();
    }

    static TDuration GetLatestTimestampUpdatePeriod(const IConnectionPtr& nativeConnection)
    {
        return GetTimestampProviderConfig(nativeConnection->GetConfig())->LatestTimestampUpdatePeriod;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateRemoteClusterTimestampProvider(
    IConnectionPtr nativeConnection,
    TCellTag clockClusterTag,
    NLogging::TLogger logger)
{
    return New<TRemoteClusterTimestampProvider>(
        std::move(nativeConnection),
        clockClusterTag,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
