#include "announcer.h"
#include "private.h"
#include "msgpack.h"
#include "config.h"

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/net/packet_connection.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/utilex/random.h>

namespace NYT {
namespace NSkynetManager {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NYTree;
using namespace NNet;

static auto& Logger = SkynetManagerLogger;

////////////////////////////////////////////////////////////////////////////////

const ui32 ClientPacketMagic = 4079332072;

TTrackerConnection::TTrackerConnection(
    IPacketConnectionPtr connection,
    TNetworkAddress trackerAddress,
    TString peerId)
    : Connection_(connection)
    , TrackerAddress_(trackerAddress)
    , PeerId_(peerId)
    , NextTransactionId_(RandomNumber<TTrackerTransactionId>())
    , SuccessfullPackets_("/successfull_packets", {
        TProfileManager::Get()->RegisterTag({"tracker_address", ToString(trackerAddress)})
    })
    , FailedPackets_("/failed_packets", {
        TProfileManager::Get()->RegisterTag({"tracker_address", ToString(trackerAddress)})
    })
    , TimedoutPackets_("/timedout_packets", {
        TProfileManager::Get()->RegisterTag({"tracker_address", ToString(trackerAddress)})
    })
{ }

TFuture<TDuration> TTrackerConnection::Connect(ui16 dataPort)
{
    auto transactionId = NextTransactionId_++;
    auto request = BuildYsonNodeFluently()
        .BeginList()
            .Item().Value(ClientPacketMagic)
            .Item().Value(0) // ACTION_CONNECT
            .Item().Value(transactionId)
            .Item().Value(PeerId_)
            .Item().BeginList() // List of backbone and fastbone v6/v4 addresses.
                .Item().Entity()
                .Item().Entity()
                .Item().Entity()
                .Item().Entity()
            .EndList()
            .Item().Value(dataPort)
            .Item().BeginList()
                .Item().Value("peer_types")
                .Item().Value("dfs")
            .EndList()
        .EndList();

    return Send(transactionId, request)
        .Apply(BIND([] (const TErrorOr<INodePtr>& reply) {
            return TDuration::Seconds(ConvertTo<int>(reply.ValueOrThrow()->AsList()->FindChild(2)));
        }));
}

TFuture<TNullable<TDuration>> TTrackerConnection::Announce(TResourceId resource, EAnnounceState state)
{
    auto transactionId = NextTransactionId_++;
    auto request = BuildYsonNodeFluently()
        .BeginList()
            .Item().Value(ClientPacketMagic)
            .Item().Value(10) // ACTION_ANNOUNCE
            .Item().Value(transactionId)
            .Item().Value(PeerId_)
            .Item().Value(resource)
            .Item().Value(static_cast<int>(state))
            .Item().Value(0) // NETWORK_MODE_AUTO
        .EndList();

    return Send(transactionId, request)
        .Apply(BIND([state] (const TErrorOr<INodePtr>& reply) -> TNullable<TDuration> {
            if (state == EAnnounceState::Seeding) {
                return TDuration::Seconds(ConvertTo<int>(reply.ValueOrThrow()->AsList()->FindChild(2)));
            }

            return Null;
        }));
}

void TTrackerConnection::OnTrackerPacket(const TSharedRef& packet)
{
    auto msg = ParseFromMsgpack(packet);
    auto transactionId = ConvertTo<TTrackerTransactionId>(msg->AsList()->FindChild(0));

    auto request = ActiveRequests_.find(transactionId);
    if (request == ActiveRequests_.end()) {
        LOG_WARNING("Received packet with unknown transaction id (TransactionId: %v)", transactionId);
        return;
    }

    auto errorCode = ConvertTo<int>(msg->AsList()->FindChild(1));
    if (errorCode != 0) {
        SkynetManagerProfiler.Increment(FailedPackets_);
        request->second.Reply.TrySet(TError("Tracker returned an error")
            << TErrorAttribute("tracker_error_code", errorCode)
            << TErrorAttribute("tracker_message", msg));
    } else {
        SkynetManagerProfiler.Increment(SuccessfullPackets_);
        request->second.Reply.TrySet(msg);
    }
    ActiveRequests_.erase(request);
}

TFuture<INodePtr> TTrackerConnection::Send(TTrackerTransactionId id, INodePtr request)
{
    auto packet = SerializeToMsgpack(request);
    auto& context = ActiveRequests_[id];
    context.Reply = NewPromise<INodePtr>();
    context.Deadline = TInstant::Now() + TDuration::Seconds(5);

    LOG_DEBUG("Sending packet to tracker (TrackerAddress: %v)", TrackerAddress_);
    Connection_->SendTo(packet, TrackerAddress_);
    return context.Reply.ToFuture();
}

void TTrackerConnection::ExpireRequests()
{
    std::vector<TTrackerTransactionId> toRemove;
    for (auto&& context : ActiveRequests_) {
        if (context.second.Deadline > TInstant::Now()) {
            continue;
        }
        context.second.Reply.TrySet(TError("Tracker request timed out"));
        toRemove.push_back(context.first);
    }

    for (auto id : toRemove) {
        LOG_ERROR("Tracker request expired (TrackerAddress: %v, TransactionId: %d)",
            TrackerAddress_,
            id);
        ActiveRequests_.erase(id);
        SkynetManagerProfiler.Increment(TimedoutPackets_);
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<std::pair<TResourceId, TTrackerId>> TAnnounceScheduler::GetNextBatch()
{
    constexpr int MaxBatchSize = 128;
    std::vector<std::pair<TResourceId, TTrackerId>> batch;
    auto now = TInstant::Now();

    auto it = Timers_.begin();
    while (it != Timers_.end() && batch.size() < MaxBatchSize) {
        if (it->first > now) {
            break;
        }

        auto removed = it++;
        batch.push_back(removed->second);
        Timers_.erase(removed);
    }

    return batch;
}

void TAnnounceScheduler::PutRestored(TResourceId resourceId, TTrackerId trackerCount)
{
    const TDuration StartupSplay = TDuration::Hours(3);
    for (int i = 0; i < trackerCount; i++) {
        PutWithTTL(resourceId, i, RandomDuration(StartupSplay));
    }
}

void TAnnounceScheduler::PutRetry(TResourceId resourceId, TTrackerId failedTrackerId)
{
    const TDuration ErrorBackoff = TDuration::Seconds(5);
    PutWithTTL(resourceId, failedTrackerId, RandomDuration(ErrorBackoff));
}

void TAnnounceScheduler::PutWithTTL(TResourceId resourceId, TTrackerId trackerId, TDuration ttl)
{
    Timers_.emplace(TInstant::Now() + ttl, std::make_pair(resourceId, trackerId));
}

size_t TAnnounceScheduler::QueueSize() const
{
    return Timers_.size();
}

////////////////////////////////////////////////////////////////////////////////

TResourceLock::TResourceLock(TAnnouncer* announcer, const TString& resourceId)
    : Announcer_(announcer)
    , ResourceId_(resourceId)
{ }

TResourceLock::~TResourceLock()
{
    if (Locked_) {
        Announcer_->ReleaseLock(ResourceId_);
    }
}

bool TResourceLock::TryAcquire()
{
    if (Locked_) {
        return true;
    }

    Locked_ = Announcer_->TryAcquireLock(ResourceId_);
    return Locked_;
}

////////////////////////////////////////////////////////////////////////////////

TAnnouncer::TAnnouncer(
    const IInvokerPtr& invoker,
    const IPollerPtr& poller,
    const TAnnouncerConfigPtr& config,
    TString peerId,
    ui16 selfDataPort)
    : Invoker_(invoker)
    , SelfDataPort_(selfDataPort)
    , Config_(config)
    , ExpireRequests_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TAnnouncer::ExpireRequests, MakeStrong(this)),
        TDuration::Seconds(1)))
    , Connection_(CreatePacketConnection(
        TNetworkAddress::CreateIPv6Any(config->PeerUdpPort),
        poller))
{
    for (auto tracker : config->Trackers) {
        auto trackerAddress = TAddressResolver::Get()->Resolve(TString(GetServiceHostName(tracker)))
            .Get()
            .ValueOrThrow();
        trackerAddress = TNetworkAddress(trackerAddress, GetServicePort(tracker));
        Trackers_.emplace_back(
            trackerAddress,
            New<TTrackerConnection>(Connection_, trackerAddress, peerId));
    }
}

bool TAnnouncer::IsHealthy()
{
    return ConnectedTrackerCount_ == Trackers_.size();
}

void TAnnouncer::Start()
{
    for (auto&& tracker : Trackers_) {
        BIND(&TAnnouncer::RunConnectLoop, MakeStrong(this), tracker.second).AsyncVia(Invoker_).Run();
    }

    BIND(&TAnnouncer::RunReceiveLoop, MakeStrong(this)).AsyncVia(Invoker_).Run();

    ExpireRequests_->Start();

    BIND(&TAnnouncer::RunAnnounceLoop, MakeStrong(this)).AsyncVia(Invoker_).Run();
}

TFuture<void> TAnnouncer::AddOutOfOrderAnnounce(const TString& cluster, const TResourceId& resourceId)
{
    return BIND([this, this_ = MakeStrong(this), cluster, resourceId] () {
        auto& state = Resources_[resourceId];
        bool needsAnnouncer = !state.IsAlive();
        
        state.LastOutOfOrderUpdate = std::make_pair(cluster, TInstant::Now() + Config_->OutOfOrderUpdateTtl);

        if (!needsAnnouncer) {
            return;
        }

        SkynetManagerProfiler.Update(ResourceCount_, Resources_.size());
        for (TTrackerId trackerId = 0; trackerId < Trackers_.size(); ++trackerId) {
            auto& tracker = Trackers_[trackerId].second;
            auto result = WaitFor(tracker->Announce(resourceId, EAnnounceState::Seeding));
            if (result.IsOK()) {
                Scheduler_.PutWithTTL(resourceId, trackerId, *result.Value());
            } else {
                Scheduler_.PutRetry(resourceId, trackerId);
            }
        }
    })
        .AsyncVia(Invoker_)
        .Run();
}

void TAnnouncer::SyncResourceList(
    const TString& cluster,
    THashSet<TResourceId> resources)
{
    auto result = BIND([this, this_ = MakeStrong(this), cluster, updated = std::move(resources)] () mutable {
        std::vector<TResourceId> toRemove;
        for (auto& resource : Resources_) {
            if (updated.find(resource.first) == updated.end()) {
                resource.second.Clusters.erase(cluster);
                if (!resource.second.IsAlive()) {
                    toRemove.push_back(resource.first);
                }
            } else {
                resource.second.Clusters.insert(cluster);
                updated.erase(resource.first);
            }
        }

        for (auto&& resourceId : updated) {
            auto& state = Resources_[resourceId];
            state.Clusters.insert(cluster);

            Scheduler_.PutRestored(resourceId, Trackers_.size());
        }

        SkynetManagerProfiler.Update(ResourceCount_, Resources_.size());
        LOG_INFO("Finished synchronizing resource list (Cluster: %s, Added: %d, Removed: %d)",
            cluster,
            updated.size(),
            toRemove.size());
        for (auto&& resourceId : toRemove) {
            Resources_.erase(resourceId);
            for (auto& tracker : Trackers_) {
                tracker.second->Announce(resourceId, EAnnounceState::Stopped);
            }
        }
    })
        .AsyncVia(Invoker_)
        .Run();

    WaitFor(result)
        .ThrowOnError();
}


bool TAnnouncer::TryAcquireLock(const TResourceId& resourceId)
{
    auto guard = Guard(Lock_);
    return LockedResources_.insert(resourceId).second;
}

void TAnnouncer::ReleaseLock(const TResourceId& resourceId)
{
    auto guard = Guard(Lock_);
    LockedResources_.erase(resourceId);
}

bool TAnnouncer::TResourceState::IsAlive() const
{
    if (!Clusters.empty()) {
        return true;
    }

    if (!LastOutOfOrderUpdate) {
        return false;
    }

    return TInstant::Now() < LastOutOfOrderUpdate->second;
}

TString TAnnouncer::FindResourceCluster(const TResourceId& resourceId)
{
    auto resourceCluster = BIND([this, this_ = MakeStrong(this), resourceId] () {
        auto it = Resources_.find(resourceId);
        if (it == Resources_.end()) {
            THROW_ERROR_EXCEPTION("Resource not found in announcer cache; Unable to determine resource cluster")
                << TErrorAttribute("resource_id", resourceId);
        }

        if (it->second.LastOutOfOrderUpdate) {
            return it->second.LastOutOfOrderUpdate->first;
        }

        YCHECK(!it->second.Clusters.empty());
        return it->second.Clusters.front();
    })
        .AsyncVia(Invoker_)
        .Run();

    return WaitFor(resourceCluster)
        .ValueOrThrow();
}

void TAnnouncer::RunAnnounceLoop()
{
    while (true) {
        auto batch = Scheduler_.GetNextBatch();
        LOG_INFO("Started backgroud announce iteration (QueueSize: %d, BatchSize: %d)",
            Scheduler_.QueueSize(),
            batch.size());
        for (auto&& resource : batch) {
            if (Resources_.find(resource.first) == Resources_.end()) {
                continue;
            }
        
            const auto& tracker = Trackers_[resource.second].second;
            auto reply = tracker->Announce(resource.first, EAnnounceState::Seeding);
            auto result = WaitFor(reply);

            if (result.IsOK()) {
                Scheduler_.PutWithTTL(resource.first, resource.second, *result.Value());
            } else {
                LOG_ERROR(result, "Error is background announcer (ResourceId: %v)", resource.first);
                Scheduler_.PutRetry(resource.first, resource.second);
            }
        }

        LOG_INFO("Finished backgroud announce iteration (QueueSize: %d)",
            Scheduler_.QueueSize());
        TDelayedExecutor::WaitForDuration(TDuration::Seconds(5));
    }
}

void TAnnouncer::RunConnectLoop(TTrackerConnectionPtr tracker)
{
    while (true) {
        bool connected = false;
        try {
            LOG_INFO("Sending connect message to tracker");
            auto ttl = WaitFor(tracker->Connect(SelfDataPort_)).ValueOrThrow();
            LOG_INFO("Connected to tracker (Ttl: %v)", ttl);
            if (!connected) {
                connected = true;
                ConnectedTrackerCount_ += 1;
            }

            TDelayedExecutor::WaitForDuration(ttl);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error connecting to tracker");
            if (connected) {
                connected = false;
                ConnectedTrackerCount_ -= 1;
            }
            TDelayedExecutor::WaitForDuration(TDuration::Seconds(5));
        }
    }
}

void TAnnouncer::RunReceiveLoop()
{
    auto buffer = TSharedMutableRef::Allocate(4096);
    while (true) {
        auto msg = WaitFor(Connection_->ReceiveFrom(buffer));

        if (!msg.IsOK()) {
            LOG_ERROR(msg, "Error receiving packet from tracker");
            continue;
        }

        LOG_DEBUG("Received tracker packet (TrackerAddress: %v)", msg.ValueOrThrow().second);
        bool found = true;
        for (auto&& trackerConnection : Trackers_) {
            if (trackerConnection.first != msg.ValueOrThrow().second) {
                continue;
            }

            found = true;

            try {
                trackerConnection.second->OnTrackerPacket(buffer.Slice(0, msg.ValueOrThrow().first));
            } catch (const std::exception& ex) {
                LOG_ERROR(ex, "Error in tracker packet handler");
            }
            break;
        }

        if (!found) {
            LOG_ERROR("Received packet from unknown address (Address: %v)", msg.ValueOrThrow().second);
        }
    }
}

void TAnnouncer::ExpireRequests()
{
    for (auto&& tracker : Trackers_) {
        LOG_INFO("Expiring requests from tracker (TrackerAddress: %v)", tracker.first);
        tracker.second->ExpireRequests();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSkynetManager
} // namespace NYT
