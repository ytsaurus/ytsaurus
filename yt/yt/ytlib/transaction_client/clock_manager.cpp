#include "clock_manager.h"

#include "config.h"
#include "remote_cluster_timestamp_provider.h"

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <atomic>

namespace NYT::NTransactionClient {

using namespace NApi::NNative;
using namespace NObjectClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TClockManager
    : public IClockManager
{
public:
    TClockManager(
        TClockManagerConfigPtr config,
        IConnectionPtr connection,
        NLogging::TLogger logger)
        : NativeConnection_(connection)
        , NativeCellTag_(connection->GetClusterTag())
        , Logger(std::move(logger))
    {
        Reconfigure(std::move(config));
    }

    const ITimestampProviderPtr& GetTimestampProviderOrThrow(TCellTag clockClusterTag) override
    {
        if (clockClusterTag == InvalidCellTag || clockClusterTag == NativeCellTag_) {
            return GetNativeConnectionOrThrow()->GetTimestampProvider();
        }

        // Fast path.
        {
            auto guard = ReaderGuard(SpinLock_);
            if (auto it = TimestampProviderMap_.find(clockClusterTag)) {
                return it->second;
            }
        }

        // Slow path.
        {
            auto connection = GetNativeConnectionOrThrow();
            auto provider = CreateRemoteClusterTimestampProvider(connection, clockClusterTag, Logger);

            auto guard = WriterGuard(SpinLock_);
            if (auto it = TimestampProviderMap_.find(clockClusterTag)) {
                return it->second;
            }
            auto it = EmplaceOrCrash(TimestampProviderMap_, clockClusterTag, std::move(provider));
            return it->second;
        }
    }

    void ValidateDefaultClock(TStringBuf message) override
    {
        auto clockClusterTag = ClockClusterTag_.load();
        if (clockClusterTag != NativeCellTag_ && clockClusterTag != InvalidCellTag)
        {
            THROW_ERROR_EXCEPTION("%v: clock source is configured to non-native clock", message)
                << TErrorAttribute("clock_cluster_tag", clockClusterTag);
        }
    }

    TCellTag GetCurrentClockTag() override
    {
        return ClockClusterTag_.load();
    }

    void Reconfigure(TClockManagerConfigPtr newConfig) override
    {
        auto clockClusterTag = newConfig->ClockClusterTag;
        ClockClusterTag_ = clockClusterTag;

        YT_LOG_DEBUG("Clock manager reconfigured (ClockClusterTag: %v)", clockClusterTag);
    }

private:
    const TWeakPtr<IConnection> NativeConnection_;
    const TCellTag NativeCellTag_;
    const NLogging::TLogger Logger;

    std::atomic<TCellTag> ClockClusterTag_;

    THashMap<TCellTag, ITimestampProviderPtr> TimestampProviderMap_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);

    IConnectionPtr GetNativeConnectionOrThrow()
    {
        if (auto connection = NativeConnection_.Lock()) {
            return connection;
        }

        THROW_ERROR_EXCEPTION("Unable to get timestamp provider: connection has been closed");
    }
};

////////////////////////////////////////////////////////////////////////////////

IClockManagerPtr CreateClockManager(
    TClockManagerConfigPtr config,
    IConnectionPtr connection,
    NLogging::TLogger logger)
{
    return New<TClockManager>(
        std::move(config),
        std::move(connection),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
