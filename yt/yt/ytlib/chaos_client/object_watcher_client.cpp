#include "object_watcher_client.h"

#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NChaosClient {

using namespace NApi::NNative;
using namespace NThreading;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

TObjectWatcherClientBase::TObjectWatcherClientBase(
    TWeakPtr<IConnection> connection,
    NRpc::IChannelPtr chaosCacheChannel)
    : Connection_(std::move(connection))
    , ChaosCacheChannel_(std::move(chaosCacheChannel))
{ }

void TObjectWatcherClientBase::WatchObject(TChaosObjectId objectId)
{
    auto guard = Guard(Lock_);
    auto& watchState = WatchStates_[objectId];
    if (watchState.Future) {
        return;
    }

    watchState.Timestamp = MinTimestamp;
    watchState.Future = WatchUpstream(objectId, watchState.Timestamp);
}

bool TObjectWatcherClientBase::StopWatchingObject(TChaosObjectId objectId)
{
    TFuture<void> future;
    {
        auto guard = Guard(Lock_);
        auto it = WatchStates_.find(objectId);
        if (it == WatchStates_.end()) {
            return false;
        }

        future = std::move(it->second.Future);
        WatchStates_.erase(it);
    }

    future.Cancel(TError("Stopped watching"));
    return true;
}

bool TObjectWatcherClientBase::RemoveWatch(TChaosObjectId objectId)
{
    auto guard = Guard(Lock_);
    return WatchStates_.erase(objectId) != 0;
}

bool TObjectWatcherClientBase::RearmWatch(TChaosObjectId objectId)
{
    auto guard = Guard(Lock_);
    auto it = WatchStates_.find(objectId);
    if (it == WatchStates_.end()) {
        return false;
    }

    try {
        it->second.Future = WatchUpstream(objectId, it->second.Timestamp);
    } catch (...) {
        WatchStates_.erase(it);
        throw;
    }
    return true;
}

bool TObjectWatcherClientBase::RearmWatch(
    TChaosObjectId objectId,
    TTimestamp timestamp)
{
    auto guard = Guard(Lock_);
    auto it = WatchStates_.find(objectId);
    if (it == WatchStates_.end()) {
        return false;
    }

    it->second.Timestamp = timestamp;
    try {
        it->second.Future = WatchUpstream(objectId, timestamp);
    } catch (...) {
        WatchStates_.erase(it);
        throw;
    }
    return true;
}

IConnectionPtr TObjectWatcherClientBase::GetConnection() const
{
    return Connection_.Lock();
}

NRpc::IChannelPtr TObjectWatcherClientBase::GetWatchChannel(
    const IConnectionPtr& connection,
    TChaosObjectId objectId) const
{
    if (ChaosCacheChannel_) {
        return ChaosCacheChannel_;
    }

    return connection->GetChaosChannelByObjectIdOrThrow(objectId, NHydra::EPeerKind::Leader);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
