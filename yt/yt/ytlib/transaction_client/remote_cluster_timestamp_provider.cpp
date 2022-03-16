#include "remote_cluster_timestamp_provider.h"

#include "public.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/config.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NTransactionClient {

using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TRemoteClusterTimestampProvider
    : public ITimestampProvider
{
public:
    TRemoteClusterTimestampProvider(
        IConnectionPtr nativeConnection,
        TCellTag clockClusterTag,
        NLogging::TLogger logger)
        : NativeConnection_(nativeConnection)
        , ClockClusterTag_(clockClusterTag)
        , Logger(std::move(logger))
    {
        if (ClockClusterTag_ == nativeConnection->GetClusterTag()) {
            Underlying_.Store(nativeConnection->GetTimestampProvider());
            return;
        }

        nativeConnection->GetClusterDirectorySynchronizer()->Sync()
            .Subscribe(BIND(&TRemoteClusterTimestampProvider::OnClusterDirectorySync, MakeWeak(this)));
    }

    TFuture<TTimestamp> GenerateTimestamps(int count) override
    {
        if (auto underlying = Underlying_.Load()) {
            return underlying->GenerateTimestamps(count);
        }

        return MakeFuture<TTimestamp>(TError(
            "Timestamp provider for clock cluster tag %v is unavailable at the moment",
            ClockClusterTag_));
    }

    TTimestamp GetLatestTimestamp() override
    {
        if (auto underlying = Underlying_.Load()) {
            return underlying->GetLatestTimestamp();
        }

        return MinTimestamp;
    }

private:
    const TWeakPtr<IConnection> NativeConnection_;
    const TCellTag ClockClusterTag_;

    const NLogging::TLogger Logger;

    TAtomicObject<ITimestampProviderPtr> Underlying_;

    void InitializeUnderlying()
    {
        auto nativeConnection = NativeConnection_.Lock();
        if (!nativeConnection) {
            YT_LOG_DEBUG("Cannot initialize timestamp provider: connection terminated (ClockClusterTag: %v)",
                ClockClusterTag_);
            return;
        } 

        if (auto connection = FindRemoteConnection(nativeConnection, ClockClusterTag_)) {
            Underlying_.Store(connection->GetTimestampProvider());
            return;
        }

        auto retryTime = nativeConnection->GetConfig()->ClusterDirectorySynchronizer->SyncPeriod;

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
