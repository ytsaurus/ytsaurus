#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/ytree/node.h>

#include <yt/core/profiling/profiler.h>

#include <yt/core/misc/ref.h>

#include <yt/core/misc/small_set.h>

#include <yt/core/net/address.h>

#include <map>

namespace NYT {
namespace NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EAnnounceState,
    ((Seeding) (2))
    ((Stopped) (3))
);

////////////////////////////////////////////////////////////////////////////////

class TTrackerConnection
    : public TRefCounted
{
public:
    TTrackerConnection(
        NNet::IPacketConnectionPtr connection,
        NNet::TNetworkAddress trackerAddress,
        TString peerId);

    TFuture<TDuration> Connect(ui16 dataPort);
    TFuture<TNullable<TDuration>> Announce(TResourceId resource, EAnnounceState state);

    void OnTrackerPacket(const TSharedRef& packet);
    void ExpireRequests();

private:
    const NNet::IPacketConnectionPtr Connection_;
    const NNet::TNetworkAddress TrackerAddress_;
    const TString PeerId_;

    typedef ui32 TTrackerTransactionId;
    struct TTrackerRequestContext
    {
        TInstant Deadline;
        TPromise<NYTree::INodePtr> Reply;
    };

    THashMap<TTrackerTransactionId, TTrackerRequestContext> ActiveRequests_;
    TTrackerTransactionId NextTransactionId_;

    TFuture<NYTree::INodePtr> Send(TTrackerTransactionId id, NYTree::INodePtr request);

    NProfiling::TMonotonicCounter SuccessfullPackets_;
    NProfiling::TMonotonicCounter FailedPackets_;
    NProfiling::TMonotonicCounter TimedoutPackets_;
};

DEFINE_REFCOUNTED_TYPE(TTrackerConnection)

////////////////////////////////////////////////////////////////////////////////

typedef int TTrackerId;

class TAnnounceScheduler
{
public:
    std::vector<std::pair<TResourceId, TTrackerId>> GetNextBatch();

    void PutRestored(TResourceId resourceId, TTrackerId trackerCount);
    void PutRetry(TResourceId resourceId, TTrackerId failedTrackerId);
    void PutWithTTL(TResourceId resourceId, TTrackerId trackerId, TDuration ttl);
    size_t QueueSize() const;

private:
    std::multimap<TInstant, std::pair<TResourceId, TTrackerId>> Timers_;
};

////////////////////////////////////////////////////////////////////////////////

class TResourceLock
{
public:
    TResourceLock(const TResourceLock&) = delete;

    TResourceLock(TAnnouncer* announcer, const TResourceId& resourceId);
    ~TResourceLock();

    bool TryAcquire();

private:
    TAnnouncer* Announcer_;
    const TResourceId ResourceId_;
    bool Locked_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TAnnouncer
    : public TRefCounted
{
public:
    TAnnouncer(
        const IInvokerPtr& invoker,
        const NConcurrency::IPollerPtr& poller,
        const TAnnouncerConfigPtr& config,
        const TString peerId,
        ui16 selfDataPort);

    TFuture<void> AddOutOfOrderAnnounce(const TString& cluster, const TResourceId& resource);

    void SyncResourceList(
        const TString& cluster,
        THashSet<TResourceId> resources);

    bool TryAcquireLock(const TResourceId& resourceId);
    void ReleaseLock(const TResourceId& resourceId);

    TString FindResourceCluster(const TResourceId& resourceId);

    bool IsHealthy();

    void Start();

private:
    const IInvokerPtr Invoker_;
    const ui16 SelfDataPort_;
    const TAnnouncerConfigPtr Config_;
    const NConcurrency::TPeriodicExecutorPtr ExpireRequests_;
    const NNet::IPacketConnectionPtr Connection_;

    std::atomic<int> ConnectedTrackerCount_ = {0};

    struct TResourceState
    {
        SmallSet<TString, 2> Clusters;

        TNullable<std::pair<TString, TInstant>> LastOutOfOrderUpdate;

        bool IsLocked = false;

        bool IsAlive() const;
    };

    THashMap<TResourceId, TResourceState> Resources_;
    TAnnounceScheduler Scheduler_;

    TSpinLock Lock_;
    THashSet<TResourceId> LockedResources_;

    std::vector<std::pair<NNet::TNetworkAddress, TTrackerConnectionPtr>> Trackers_;

    void RunReceiveLoop();
    void RunConnectLoop(TTrackerConnectionPtr tracker);
    void RunAnnounceLoop();
    void ExpireRequests();
};

DEFINE_REFCOUNTED_TYPE(TAnnouncer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
