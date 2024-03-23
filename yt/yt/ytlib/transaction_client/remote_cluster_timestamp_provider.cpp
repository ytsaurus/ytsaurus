#include "remote_cluster_timestamp_provider.h"

#include "public.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/config.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/logging/log.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

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
        , ClockClusterTag_(clockClusterTag == nativeConnection->GetClusterTag()
            ? InvalidCellTag
            : clockClusterTag)
        , Logger(std::move(logger))
    {
        Underlying_.Store(nativeConnection->GetTimestampProvider());

        nativeConnection->GetClusterDirectorySynchronizer()->Sync()
            .Subscribe(BIND(&TRemoteClusterTimestampProvider::OnClusterDirectorySync, MakeWeak(this)));
    }

    TFuture<TTimestamp> GenerateTimestamps(int count, TCellTag clockClusterTag) override
    {
        if (clockClusterTag == InvalidCellTag) {
            clockClusterTag = ClockClusterTag_;
        }
        if (auto underlying = Underlying_.Acquire()) {
            return underlying->GenerateTimestamps(count, clockClusterTag);
        }

        auto nativeConnection = NativeConnection_.Lock();
        if (!nativeConnection) {
            THROW_ERROR_EXCEPTION("Cannot generate timestamps: connection terminated")
                << TErrorAttribute("clock_cluster_tag", clockClusterTag);
        }

        return nativeConnection->GetClusterDirectorySynchronizer()->Sync()
            .Apply(BIND(&TRemoteClusterTimestampProvider::OnClusterDirectorySync, MakeWeak(this)))
            .Apply(BIND([this, this_ = MakeStrong(this), count, clockClusterTag] {
                if (auto underlying = Underlying_.Acquire()) {
                    return underlying->GenerateTimestamps(count, clockClusterTag);
                }
                return MakeFuture<TTimestamp>(TError(
                    "Timestamp provider for clock cluster tag %v is unavailable at the moment",
                    clockClusterTag));
            }));
    }

    TTimestamp GetLatestTimestamp(TCellTag clockClusterTag) override
    {
        if (auto underlying = Underlying_.Acquire()) {
            return underlying->GetLatestTimestamp(clockClusterTag == InvalidCellTag
                ? ClockClusterTag_
                : clockClusterTag);
        }

        return MinTimestamp;
    }

private:
    const TWeakPtr<IConnection> NativeConnection_;
    const TCellTag ClockClusterTag_;

    const NLogging::TLogger Logger;

    TAtomicIntrusivePtr<ITimestampProvider> Underlying_;

    void OnClusterDirectorySync(const TError& /*error*/)
    {
        auto nativeConnection = NativeConnection_.Lock();
        if (!nativeConnection) {
            YT_LOG_DEBUG("Cannot initialize timestamp provider: connection terminated");
            return;
        }

        Underlying_.Store(nativeConnection->GetTimestampProvider());
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
