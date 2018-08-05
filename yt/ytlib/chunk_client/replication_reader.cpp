#include "replication_reader.h"
#include "private.h"
#include "traffic_meter.h"
#include "block_cache.h"
#include "block_id.h"
#include "chunk_reader.h"
#include "config.h"
#include "data_node_service_proxy.h"
#include "dispatcher.h"
#include "helpers.h"
#include "chunk_reader_allowing_repair.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/api/config.h>

#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/replication_reader.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/client/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/client/object_client/helpers.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/protobuf_helpers.h>
#include <yt/core/misc/string.h>

#include <yt/core/net/local_address.h>

#include <util/generic/ymath.h>

#include <util/random/shuffle.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NRpc;
using namespace NApi;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NNet;

using NYT::ToProto;
using NYT::FromProto;
using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static const double MaxBackoffMultiplier = 1000.0;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPeerType,
    (Peer)
    (Seed)
);

////////////////////////////////////////////////////////////////////////////////

struct TPeer
{
    TPeer(
        const TString& address,
        TNodeDescriptor nodeDescriptor,
        EPeerType peerType,
        EAddressLocality locality)
        : Address(address)
        , NodeDescriptor(nodeDescriptor)
        , Type(peerType)
        , Locality(locality)
    { }

    TString Address;
    TNodeDescriptor NodeDescriptor;
    EPeerType Type;
    EAddressLocality Locality;
};

TString ToString(const TPeer& peer)
{
    return peer.Address;
}

////////////////////////////////////////////////////////////////////////////////

struct TPeerQueueEntry
{
    TPeerQueueEntry(const TPeer& peer, int banCount)
        : Peer(peer)
        , BanCount(banCount)
    { }

    TPeer Peer;
    int BanCount = 0;
    ui32 Random = RandomNumber<ui32>();
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationReader
    : public IChunkReaderAllowingRepair
{
public:
    TReplicationReader(
        TReplicationReaderConfigPtr config,
        TRemoteReaderOptionsPtr options,
        NNative::IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const TNodeDescriptor& localDescriptor,
        const TChunkId& chunkId,
        const TChunkReplicaList& seedReplicas,
        IBlockCachePtr blockCache,
        IThroughputThrottlerPtr throttler,
        TTrafficMeterPtr trafficMeter)
        : Config_(config)
        , Options_(options)
        , Client_(client)
        , NodeDirectory_(nodeDirectory)
        , LocalDescriptor_(localDescriptor)
        , ChunkId_(chunkId)
        , BlockCache_(blockCache)
        , Throttler_(throttler)
        , Networks_(client->GetNativeConnection()->GetNetworks())
        , TrafficMeter_(trafficMeter)
        , LocateChunksInvoker_(CreateFixedPriorityInvoker(
            TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
            // We locate chunks with batch workload category.
            TWorkloadDescriptor(EWorkloadCategory::UserBatch).GetPriority()))
        , Logger(NLogging::TLogger(ChunkClientLogger)
            .AddTag("ChunkId: %v", ChunkId_))
        , InitialSeedReplicas_(seedReplicas)
    { }

    void Initialize()
    {
        if (!Options_->AllowFetchingSeedsFromMaster && InitialSeedReplicas_.empty()) {
            THROW_ERROR_EXCEPTION(
                "Cannot read chunk %v: master seeds retries are disabled and no initial seeds are given",
                ChunkId_);
        }

        if (!InitialSeedReplicas_.empty()) {
            SeedsPromise_ = MakePromise(InitialSeedReplicas_);
        }

        LOG_DEBUG("Reader initialized (InitialSeedReplicas: %v, FetchPromPeers: %v, LocalDescriptor: %v, PopulateCache: %v, "
            "AllowFetchingSeedsFromMaster: %v, Networks: %v)",
            MakeFormattableRange(InitialSeedReplicas_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            Config_->FetchFromPeers,
            LocalDescriptor_,
            Config_->PopulateCache,
            Options_->AllowFetchingSeedsFromMaster,
            Networks_);
    }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        const std::vector<int>& blockIndexes) override;

    virtual TFuture<std::vector<TBlock>> ReadBlocks(
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount) override;

    virtual TFuture<NProto::TChunkMeta> GetMeta(
        const TClientBlockReadOptions& options,
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>>& extensionTags) override;

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    virtual bool IsValid() const override
    {
        return !Failed_;
    }

    void SetFailed()
    {
        Failed_ = true;
    }

    virtual void SetSlownessChecker(TCallback<TError(i64, TDuration)> slownessChecker) override
    {
        SlownessChecker_ = slownessChecker;
    }

    TError RunSlownessChecker(i64 bytesReceived, TInstant startTimestamp)
    {
        if (!SlownessChecker_) {
            return TError();
        }
        auto timePassed = TInstant::Now() - startTimestamp;
        return SlownessChecker_(bytesReceived, timePassed);
    }

private:
    class TSessionBase;
    class TReadBlockSetSession;
    class TReadBlockRangeSession;
    class TGetMetaSession;
    class TAsyncGetSeedsSession;

    const TReplicationReaderConfigPtr Config_;
    const TRemoteReaderOptionsPtr Options_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TNodeDescriptor LocalDescriptor_;
    const TChunkId ChunkId_;
    const IBlockCachePtr BlockCache_;
    const IThroughputThrottlerPtr Throttler_;
    const TNetworkPreferenceList Networks_;
    const TTrafficMeterPtr TrafficMeter_;

    const IInvokerPtr LocateChunksInvoker_;

    const NLogging::TLogger Logger;

    TSpinLock SeedsSpinLock_;
    TChunkReplicaList InitialSeedReplicas_;
    TInstant SeedsTimestamp_;
    TPromise<TChunkReplicaList> SeedsPromise_;

    TSpinLock PeersSpinLock_;
    //! Peers returning NoSuchChunk error are banned forever.
    THashSet<TString> BannedForeverPeers_;
    //! Every time peer fails (e.g. time out occurs), we increase ban counter.
    THashMap<TString, int> PeerBanCountMap_;

    std::atomic<bool> Failed_ = {false};

    TCallback<TError(i64, TDuration)> SlownessChecker_;

    void DiscardSeeds(TFuture<TChunkReplicaList> result)
    {
        YCHECK(result);
        YCHECK(result.IsSet());

        TGuard<TSpinLock> guard(SeedsSpinLock_);

        if (!Options_->AllowFetchingSeedsFromMaster) {
            // We're not allowed to ask master for seeds.
            // Better keep the initial ones.
            return;
        }

        if (SeedsPromise_.ToFuture() != result) {
            return;
        }

        YCHECK(SeedsPromise_.IsSet());
        SeedsPromise_.Reset();
    }

    void ExcludeFreshSeedsFromBannedForeverPeers(const TChunkReplicaList& seedReplicas)
    {
        TGuard<TSpinLock> guard(PeersSpinLock_);
        for (auto replica : seedReplicas) {
            const auto* nodeDescriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
            if (!nodeDescriptor) {
                LOG_WARNING("Skipping replica with unresolved node id (NodeId: %v)", replica.GetNodeId());
                continue;
            }
            auto address = nodeDescriptor->FindAddress(Networks_);
            if (address) {
                BannedForeverPeers_.erase(*address);
            }
        }
    }

    //! Notifies reader about peer banned inside one of the sessions.
    void OnPeerBanned(const TString& peerAddress)
    {
        TGuard<TSpinLock> guard(PeersSpinLock_);
        auto pair = PeerBanCountMap_.insert(std::make_pair(peerAddress, 1));
        if (!pair.second) {
            ++pair.first->second;
        }

        if (pair.first->second > Config_->MaxBanCount) {
            BannedForeverPeers_.insert(peerAddress);
        }
    }

    void BanPeerForever(const TString& peerAddress)
    {
        TGuard<TSpinLock> guard(PeersSpinLock_);
        BannedForeverPeers_.insert(peerAddress);
    }

    int GetBanCount(const TString& peerAddress) const
    {
        TGuard<TSpinLock> guard(PeersSpinLock_);
        auto it = PeerBanCountMap_.find(peerAddress);
        if (it == PeerBanCountMap_.end()) {
            return 0;
        } else {
            return it->second;
        }
    }

    bool IsPeerBannedForever(const TString& peerAddress) const
    {
        TGuard<TSpinLock> guard(PeersSpinLock_);
        return BannedForeverPeers_.has(peerAddress);
    }

    void AccountTraffic(i64 transferredByteCount, const TNodeDescriptor& srcDescriptor)
    {
        if (TrafficMeter_) {
            TrafficMeter_->IncrementInboundByteCount(srcDescriptor.GetDataCenter(), transferredByteCount);
        }
    }
};

using TReplicationReaderPtr = TIntrusivePtr<TReplicationReader>;

////////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TAsyncGetSeedsSession
    : public TRefCounted
{
public:
    TAsyncGetSeedsSession(
        TReplicationReaderPtr reader,
        const NLogging::TLogger& logger)
        : Reader_(std::move(reader))
        , Logger(logger)
        , Config_(Reader_->Config_)
        , Client_(Reader_->Client_)
        , NodeDirectory_(Reader_->NodeDirectory_)
        , ChunkId_(Reader_->ChunkId_)
        , LocateChunksInvoker_(Reader_->LocateChunksInvoker_)
    { }

    TFuture<TChunkReplicaList> Run()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(Reader_->SeedsSpinLock_);

        if (!Reader_->SeedsPromise_) {
            LOG_DEBUG("Need fresh chunk seeds");
            Reader_->SeedsPromise_ = NewPromise<TChunkReplicaList>();
            auto locateChunk = BIND(&TAsyncGetSeedsSession::LocateChunk, MakeStrong(this))
                .Via(Reader_->LocateChunksInvoker_);

            if (Reader_->SeedsTimestamp_ + Config_->SeedsTimeout > TInstant::Now()) {
                // Don't ask master for fresh seeds too often.
                TDelayedExecutor::Submit(
                    locateChunk,
                    Reader_->SeedsTimestamp_ + Config_->SeedsTimeout);
            } else {
                locateChunk.Run();
            }
        }

        return Reader_->SeedsPromise_;
    }

private:
    const TReplicationReaderPtr Reader_;
    const NLogging::TLogger Logger;

    const TReplicationReaderConfigPtr Config_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TChunkId ChunkId_;
    const IInvokerPtr LocateChunksInvoker_;

    void LocateChunk()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_DEBUG("Requesting chunk seeds from master");

        try {
            auto channel = Client_->GetMasterChannelOrThrow(
                EMasterChannelKind::Follower,
                CellTagFromId(ChunkId_));

            TChunkServiceProxy proxy(channel);

            auto req = proxy.LocateChunks();
            req->SetHeavy(true);
            ToProto(req->add_subrequests(), ChunkId_);
            req->Invoke().Subscribe(
                BIND(&TAsyncGetSeedsSession::OnLocateChunkResponse, MakeStrong(this))
                    .Via(LocateChunksInvoker_));
        } catch (const std::exception& ex) {
            Reader_->SeedsPromise_.Set(TError(
                "Failed to request seeds for chunk %v from master",
                ChunkId_)
                << ex);
        }
    }

    void OnLocateChunkResponse(const TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(Reader_->SeedsPromise_);

        {
            TGuard<TSpinLock> guard(Reader_->SeedsSpinLock_);
            Reader_->SeedsTimestamp_ = TInstant::Now();
        }

        if (!rspOrError.IsOK()) {
            YCHECK(!Reader_->SeedsPromise_.IsSet());
            Reader_->SeedsPromise_.Set(TError(rspOrError));
            return;
        }

        const auto& rsp = rspOrError.Value();
        YCHECK(rsp->subresponses_size() == 1);
        const auto& subresponse = rsp->subresponses(0);
        if (subresponse.missing()) {
            YCHECK(!Reader_->SeedsPromise_.IsSet());
            Reader_->SeedsPromise_.Set(TError(
                NChunkClient::EErrorCode::NoSuchChunk,
                "No such chunk %v",
                ChunkId_));
            return;
        }

        NodeDirectory_->MergeFrom(rsp->node_directory());
        auto seedReplicas = FromProto<TChunkReplicaList>(subresponse.replicas());
        Reader_->ExcludeFreshSeedsFromBannedForeverPeers(seedReplicas);

        LOG_DEBUG("Chunk seeds received (SeedReplicas: %v)",
            MakeFormattableRange(seedReplicas, TChunkReplicaAddressFormatter(NodeDirectory_)));

        YCHECK(!Reader_->SeedsPromise_.IsSet());
        Reader_->SeedsPromise_.Set(seedReplicas);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TSessionBase
    : public TRefCounted
{
protected:
    //! Reference to the owning reader.
    const TWeakPtr<TReplicationReader> Reader_;

    const TReplicationReaderConfigPtr ReaderConfig_;
    const TRemoteReaderOptionsPtr ReaderOptions_;
    const TChunkId ChunkId_;

    const TClientBlockReadOptions SessionOptions_;

    //! The workload descriptor from the config with instant field updated
    //! properly.
    const TWorkloadDescriptor WorkloadDescriptor_;

    //! Translates node ids to node descriptors.
    const TNodeDirectoryPtr NodeDirectory_;

    //! List of the networks to use from descriptor.
    const TNetworkPreferenceList Networks_;

    NLogging::TLogger Logger;

    //! Zero based retry index (less than |ReaderConfig_->RetryCount|).
    int RetryIndex_ = 0;

    //! Zero based pass index (less than |ReaderConfig_->PassCount|).
    int PassIndex_ = 0;

    //! Seed replicas for the current retry.
    TChunkReplicaList SeedReplicas_;

    //! Set of peer addresses banned for the current retry.
    THashSet<TString> BannedPeers_;

    //! List of candidates addresses to try during current pass, prioritized by:
    //! locality, ban counter, random number.
    typedef std::priority_queue<TPeerQueueEntry, std::vector<TPeerQueueEntry>, std::function<bool(const TPeerQueueEntry&, const TPeerQueueEntry&)>> TPeerQueue;
    TPeerQueue PeerQueue_;

    //! Catalogue of peers, seen on current pass.
    THashMap<TString, TPeer> Peers_;

    //! Fixed priority invoker build upon CompressionPool.
    IInvokerPtr SessionInvoker_;

    TInstant StartTimestamp_;

    i64 TotalBytesReceived_;


    TSessionBase(
        TReplicationReader* reader,
        const TClientBlockReadOptions& options)
        : Reader_(reader)
        , ReaderConfig_(reader->Config_)
        , ReaderOptions_(reader->Options_)
        , ChunkId_(reader->ChunkId_)
        , SessionOptions_(options)
        , WorkloadDescriptor_(ReaderConfig_->EnableWorkloadFifoScheduling ? options.WorkloadDescriptor.SetCurrentInstant() : options.WorkloadDescriptor)
        , NodeDirectory_(reader->NodeDirectory_)
        , Networks_(reader->Networks_)
        , Logger(NLogging::TLogger(ChunkClientLogger)
            .AddTag("SessionId: %v, ReadSessionId: %v, ChunkId: %v",
                TGuid::Create(),
                options.ReadSessionId,
                reader->ChunkId_))
        , SessionInvoker_(CreateFixedPriorityInvoker(
            TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
            WorkloadDescriptor_.GetPriority()))
    {
        ResetPeerQueue();
    }

    EAddressLocality GetNodeLocality(const TNodeDescriptor& descriptor)
    {
        auto reader = Reader_.Lock();
        auto locality = EAddressLocality::None;

        if (reader) {
            locality = ComputeAddressLocality(descriptor, reader->LocalDescriptor_);
        }
        return locality;
    }

    void BanPeer(const TString& address, bool forever)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        if (forever && !reader->IsPeerBannedForever(address)) {
            LOG_DEBUG("Node is banned until the next seeds fetching from master (Address: %v)", address);
            reader->BanPeerForever(address);
        }

        if (BannedPeers_.insert(address).second) {
            reader->OnPeerBanned(address);
            LOG_DEBUG("Node is banned for the current retry (Address: %v, BanCount: %v)",
                address,
                reader->GetBanCount(address));
        }
    }

    const TNodeDescriptor& GetPeerDescriptor(const TString& address)
    {
        auto it = Peers_.find(address);
        YCHECK(it != Peers_.end());
        return it->second.NodeDescriptor;
    }

    //! Register peer and install into the peer queue if neccessary.
    bool AddPeer(const TString& address, const TNodeDescriptor& descriptor, EPeerType type)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return false;
        }

        TPeer peer(address, descriptor, type, GetNodeLocality(descriptor));
        auto pair = Peers_.insert({address, peer});
        if (!pair.second) {
            // Peer was already handled on current pass.
            return false;
        }

        if (IsPeerBanned(address)) {
            // Peer is banned.
            return false;
        }

        PeerQueue_.push(TPeerQueueEntry(peer, reader->GetBanCount(address)));
        return true;
    }

    //! Reinstall peer in the peer queue.
    void ReinstallPeer(const TString& address)
    {
        auto reader = Reader_.Lock();
        if (!reader || IsPeerBanned(address)) {
            return;
        }

        auto it = Peers_.find(address);
        YCHECK(it != Peers_.end());

        LOG_DEBUG("Reinstall peer into peer queue (Address: %v)", address);
        PeerQueue_.push(TPeerQueueEntry(it->second, reader->GetBanCount(address)));
    }

    bool IsSeed(const TString& address)
    {
        auto it = Peers_.find(address);
        YCHECK(it != Peers_.end());

        return it->second.Type == EPeerType::Seed;
    }

    bool IsPeerBanned(const TString& address)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return false;
        }

        return BannedPeers_.find(address) != BannedPeers_.end() || reader->IsPeerBannedForever(address);
    }

    IChannelPtr GetChannel(const TString& address)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return nullptr;
        }

        IChannelPtr channel;
        try {
            const auto& channelFactory = reader->Client_->GetChannelFactory();
            channel = channelFactory->CreateChannel(address);
        } catch (const std::exception& ex) {
            RegisterError(ex);
            BanPeer(address, false);
        }

        return channel;
    }

    template <class TResponsePtr>
    void ProcessError(TErrorOr<TResponsePtr> rspOrError, const TString& peerAddress, TError wrappingError)
    {
        auto error = wrappingError << rspOrError;
        if (rspOrError.GetCode() != NRpc::EErrorCode::Unavailable &&
            rspOrError.GetCode() != NRpc::EErrorCode::RequestQueueSizeLimitExceeded)
        {
            BanPeer(peerAddress, rspOrError.GetCode() == NChunkClient::EErrorCode::NoSuchChunk);
            RegisterError(error);
        } else {
            LOG_DEBUG(error);
        }
    }

    std::vector<TPeer> PickPeerCandidates(
        int count,
        std::function<bool(const TString&)> filter,
        const TReplicationReaderPtr& reader)
    {
        std::vector<TPeer> candidates;
        while (!PeerQueue_.empty() && candidates.size() < count) {
            const auto& top = PeerQueue_.top();
            if (top.BanCount != reader->GetBanCount(top.Peer.Address)) {
                auto queueEntry = top;
                PeerQueue_.pop();
                queueEntry.BanCount = reader->GetBanCount(queueEntry.Peer.Address);
                PeerQueue_.push(queueEntry);
                continue;
            }

            if (!candidates.empty()) {
                if (candidates.front().Type == EPeerType::Peer) {
                    // If we have peer candidate, ask it first.
                    break;
                }

                // Ensure that peers with best locality are always asked first.
                // Locality is compared w.r.t. config options.
                if (ComparePeerLocality(top.Peer, candidates.front()) < 0) {
                    break;
                }
            }

            if (filter(top.Peer.Address) && !IsPeerBanned(top.Peer.Address)) {
                candidates.push_back(top.Peer);
            }
            PeerQueue_.pop();
        }

        return candidates;
    }

    void NextRetry()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        if (IsCanceled()) {
            return;
        }

        if (ShouldStop()) {
            return;
        }

        YCHECK(!SeedsFuture_);

        LOG_DEBUG("Retry started: %v of %v",
            RetryIndex_ + 1,
            ReaderConfig_->RetryCount);

        PassIndex_ = 0;
        BannedPeers_.clear();

        auto getSeedsSession = New<TAsyncGetSeedsSession>(reader, Logger);
        SeedsFuture_ = getSeedsSession->Run();
        SeedsFuture_.Subscribe(
            BIND(&TSessionBase::OnGotSeeds, MakeStrong(this))
                .Via(SessionInvoker_));
    }

    void OnRetryFailed()
    {
        DiscardSeeds();
        
        int retryCount = ReaderConfig_->RetryCount;
        LOG_DEBUG("Retry failed: %v of %v",
            RetryIndex_ + 1,
            retryCount);

        ++RetryIndex_;
        if (RetryIndex_ >= retryCount) {
            OnSessionFailed(/* fatal */ true);
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TSessionBase::NextRetry, MakeStrong(this))
                .Via(SessionInvoker_),
            GetBackoffDuration(RetryIndex_));
    }
    
    void DiscardSeeds()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        YCHECK(SeedsFuture_);
        reader->DiscardSeeds(SeedsFuture_);
        SeedsFuture_.Reset();
    }

    void SetReaderFailed()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }
        
        reader->SetFailed();
    }

    bool PrepareNextPass()
    {
        if (IsCanceled()) {
            return false;
        }

        LOG_DEBUG("Pass started: %v of %v",
            PassIndex_ + 1,
            ReaderConfig_->PassCount);

        ResetPeerQueue();
        Peers_.clear();

        for (auto replica : SeedReplicas_) {
            const auto* descriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
            if (!descriptor) {
                RegisterError(TError(
                    NNodeTrackerClient::EErrorCode::NoSuchNode,
                    "Unresolved node id %v in node directory",
                    replica.GetNodeId()));
                continue;
            }

            auto address = descriptor->FindAddress(Networks_);
            if (!address) {
                RegisterError(TError(
                    NNodeTrackerClient::EErrorCode::NoSuchNetwork,
                    "Cannot find any of %v addresses for seed %v",
                    Networks_,
                    descriptor->GetDefaultAddress()));
                OnSessionFailed(/* fatal */ true);
                return false;
            } else {
                AddPeer(*address, *descriptor, EPeerType::Seed);
            }
        }

        if (PeerQueue_.empty()) {
            RegisterError(TError("No feasible seeds to start a pass"));
            if (ReaderOptions_->AllowFetchingSeedsFromMaster) {
                OnRetryFailed();
            } else {
                OnSessionFailed(/* fatal */ true);
            }
            return false;
        }

        return true;
    }

    void OnPassCompleted()
    {
        if (ShouldStop()) {
            return;
        }

        int passCount = ReaderConfig_->PassCount;
        LOG_DEBUG("Pass completed: %v of %v",
            PassIndex_ + 1,
            passCount);

        ++PassIndex_;
        if (PassIndex_ >= passCount) {
            OnRetryFailed();
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TSessionBase::NextPass, MakeStrong(this))
                .Via(SessionInvoker_),
            GetBackoffDuration(PassIndex_));
    }

    template <class TResponsePtr>
    void BanSeedIfUncomplete(const TResponsePtr& rsp, const TString& address)
    {
        if (IsSeed(address) && !rsp->has_complete_chunk()) {
            LOG_DEBUG("Seed does not contain the chunk (Address: %v)", address);
            BanPeer(address, false);
        }
    }

    void RegisterError(const TError& error)
    {
        LOG_ERROR(error);
        InnerErrors_.push_back(error);
    }

    TError BuildCombinedError(const TError& error)
    {
        return error << InnerErrors_;
    }

    virtual bool IsCanceled() const = 0;

    virtual void NextPass() = 0;

    virtual void OnSessionFailed(bool fatal) = 0;

private:
    //! Errors collected by the session.
    std::vector<TError> InnerErrors_;

    TFuture<TChunkReplicaList> SeedsFuture_;

    int ComparePeerLocality(const TPeer& lhs, const TPeer& rhs) const
    {
        if (lhs.Locality > rhs.Locality) {
            if (ReaderConfig_->PreferLocalHost && rhs.Locality < EAddressLocality::SameHost) {
                return 1;
            }

            if (ReaderConfig_->PreferLocalRack && rhs.Locality < EAddressLocality::SameRack) {
                return 1;
            }

            if (ReaderConfig_->PreferLocalDataCenter && rhs.Locality < EAddressLocality::SameDataCenter) {
                return 1;
            }
        } else if (lhs.Locality < rhs.Locality) {
            return -ComparePeerLocality(rhs, lhs);
        }

        return 0;
    }

    int ComparePeerQueueEntries(const TPeerQueueEntry& lhs, const TPeerQueueEntry rhs) const
    {
        int result = ComparePeerLocality(lhs.Peer, rhs.Peer);
        if (result != 0) {
            return result;
        }

        if (lhs.Peer.Type != rhs.Peer.Type) {
            // Prefer Peers to Seeds to make most use of P2P.
            if (lhs.Peer.Type == EPeerType::Peer) {
                return 1;
            } else {
                YCHECK(lhs.Peer.Type == EPeerType::Seed);
                return -1;
            }
        }

        if (lhs.BanCount != rhs.BanCount) {
            // The less - the better.
            return rhs.BanCount - lhs.BanCount;
        }

        if (lhs.Random != rhs.Random) {
            return lhs.Random < rhs.Random ? -1 : 1;
        }

        return 0;
    }

    TDuration GetBackoffDuration(int index) const
    {
        auto backoffMultiplier = std::min(
            std::pow(ReaderConfig_->BackoffTimeMultiplier, index - 1),
            MaxBackoffMultiplier);

        auto backoffDuration = ReaderConfig_->MinBackoffTime * backoffMultiplier;
        backoffDuration = std::min(backoffDuration, ReaderConfig_->MaxBackoffTime);
        return backoffDuration;
    }

    void ResetPeerQueue()
    {
        PeerQueue_ = TPeerQueue([&] (const TPeerQueueEntry& lhs, const TPeerQueueEntry& rhs) {
            return ComparePeerQueueEntries(lhs, rhs) < 0;
        });
    }

    void OnGotSeeds(const TErrorOr<TChunkReplicaList>& result)
    {
        if (!result.IsOK()) {
            DiscardSeeds();
            RegisterError(TError(
                NChunkClient::EErrorCode::MasterCommunicationFailed,
                "Error requesting seeds from master")
                << result);
            OnSessionFailed(/* fatal */ true);
            return;
        }

        SeedReplicas_ = result.Value();
        if (SeedReplicas_.empty()) {
            RegisterError(TError("Chunk is lost"));
            OnRetryFailed();
            return;
        }

        NextPass();
    }

    bool ShouldStop()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return true;
        }

        auto error = reader->RunSlownessChecker(TotalBytesReceived_, StartTimestamp_);
        if (!error.IsOK()) {
            RegisterError(TError("Read session of chunk %v is slow; may attempting repair",
                reader->GetChunkId())
                << error);
            OnSessionFailed(/* fatal */ false);
            return true;
        }

        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TReadBlockSetSession
    : public TSessionBase
{
public:
    TReadBlockSetSession(
        TReplicationReader* reader,
        const TClientBlockReadOptions& options,
        const std::vector<int>& blockIndexes)
        : TSessionBase(reader, options)
        , BlockIndexes_(blockIndexes)
    {
        Logger.AddTag("Blocks: %v", blockIndexes);
    }

    ~TReadBlockSetSession()
    {
        Promise_.TrySet(TError("Reader terminated"));
    }

    TFuture<std::vector<TBlock>> Run()
    {
        // TODO: maybe it's better to set in reader
        StartTimestamp_ = TInstant::Now();
        TotalBytesReceived_ = 0;
        NextRetry();
        return Promise_;
    }

private:
    //! Block indexes to read during the session.
    const std::vector<int> BlockIndexes_;

    //! Promise representing the session.
    TPromise<std::vector<TBlock>> Promise_ = NewPromise<std::vector<TBlock>>();

    //! Blocks that are fetched so far.
    THashMap<int, TBlock> Blocks_;

    //! Maps peer addresses to block indexes.
    THashMap<TString, THashSet<int>> PeerBlocksMap_;

    virtual bool IsCanceled() const override
    {
        return Promise_.IsCanceled();
    }

    virtual void NextPass() override
    {
        if (!PrepareNextPass())
            return;

        PeerBlocksMap_.clear();
        auto blockIndexes = GetUnfetchedBlockIndexes();
        for (const auto& pair : Peers_) {
            PeerBlocksMap_[pair.first] = THashSet<int>(blockIndexes.begin(), blockIndexes.end());
        }

        RequestBlocks();
    }

    std::vector<int> GetUnfetchedBlockIndexes()
    {
        std::vector<int> result;
        result.reserve(BlockIndexes_.size());
        for (int blockIndex : BlockIndexes_) {
            if (Blocks_.find(blockIndex) == Blocks_.end()) {
                result.push_back(blockIndex);
            }
        }
        return result;
    }

    bool HasUnfetchedBlocks(const TString& address, const std::vector<int>& indexesToFetch) const
    {
        auto it = PeerBlocksMap_.find(address);
        YCHECK(it != PeerBlocksMap_.end());
        const auto& peerBlockIndexes = it->second;

        for (int blockIndex : indexesToFetch) {
            if (peerBlockIndexes.find(blockIndex) != peerBlockIndexes.end()) {
                return true;
            }
        }

        return false;
    }

    void FetchBlocksFromCache(const TReplicationReaderPtr& reader)
    {
        for (int blockIndex : BlockIndexes_) {
            if (Blocks_.find(blockIndex) == Blocks_.end()) {
                TBlockId blockId(reader->ChunkId_, blockIndex);
                auto block = reader->BlockCache_->Find(blockId, EBlockType::CompressedData);
                if (block) {
                    LOG_DEBUG("Block is fetched from cache (Block: %v)", blockIndex);
                    YCHECK(Blocks_.insert(std::make_pair(blockIndex, block)).second);
                }
            }
        }
    }

    TNullable<TPeer> SelectBestPeer(
        const std::vector<TPeer>& candidates,
        const std::vector<int>& blockIndexes,
        const TReplicationReaderPtr& reader)
    {
        LOG_DEBUG("Gathered candidate peers (Addresses: %v)", candidates);

        if (candidates.empty()) {
            return Null;
        }

        // Multiple candidates - send probing requests.
        std::vector<TFuture<TDataNodeServiceProxy::TRspGetBlockSetPtr>> asyncResults;
        std::vector<TPeer> probePeers;

        for (const auto& peer : candidates) {
            auto channel = GetChannel(peer.Address);
            if (!channel) {
                continue;
            }

            TDataNodeServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(ReaderConfig_->ProbeRpcTimeout);

            auto req = proxy.GetBlockSet();
            req->set_fetch_from_cache(false);
            req->set_fetch_from_disk(false);
            ToProto(req->mutable_chunk_id(), reader->ChunkId_);
            ToProto(req->mutable_workload_descriptor(), WorkloadDescriptor_);
            ToProto(req->mutable_block_indexes(), blockIndexes);

            probePeers.push_back(peer);
            asyncResults.push_back(req->Invoke());
        }

        auto errorOrResults = WaitFor(CombineAll(asyncResults));
        if (!errorOrResults.IsOK()) {
            return Null;
        }

        const auto& results = errorOrResults.Value();

        TDataNodeServiceProxy::TRspGetBlockSetPtr bestRsp;
        TNullable<TPeer> bestPeer;

        auto getLoad = [&] (const TDataNodeServiceProxy::TRspGetBlockSetPtr& rsp) {
            return ReaderConfig_->NetQueueSizeFactor * rsp->net_queue_size() +
                ReaderConfig_->DiskQueueSizeFactor * rsp->disk_queue_size();
        };

        bool receivedNewPeers = false;
        for (int i = 0; i < probePeers.size(); ++i) {
            const auto& peer = probePeers[i];
            const auto& rspOrError = results[i];
            if (!rspOrError.IsOK()) {
                ProcessError(rspOrError, peer.Address, TError("Error probing node %v queue length", peer.Address));
                continue;
            }

            const auto& rsp = rspOrError.Value();
            if (UpdatePeerBlockMap(rsp, reader)) {
                receivedNewPeers = true;
            }

            // Exclude throttling peers from current pass.
            if (rsp->net_throttling() || rsp->disk_throttling()) {
                LOG_DEBUG("Peer is throttling (Address: %v)", peer.Address);
                continue;
            }

            if (!bestPeer) {
                bestRsp = rsp;
                bestPeer = peer;
                continue;
            }

            if (getLoad(rsp) < getLoad(bestRsp)) {
                ReinstallPeer(bestPeer->Address);

                bestRsp = rsp;
                bestPeer = peer;
            } else {
                ReinstallPeer(peer.Address);
            }
        }

        if (bestPeer) {
            if (receivedNewPeers) {
                LOG_DEBUG("Discard best peer since p2p was activated (Address: %v, PeerType: %v)",
                    bestPeer->Address,
                    bestPeer->Type);
                ReinstallPeer(bestPeer->Address);
                bestPeer = Null;
            } else {
                LOG_DEBUG("Best peer selected (Address: %v, DiskQueueSize: %v, NetQueueSize: %v)",
                    bestPeer->Address,
                    bestRsp->disk_queue_size(),
                    bestRsp->net_queue_size());
            }
        } else {
            LOG_DEBUG("All peer candidates were discarded");
        }

        return bestPeer;
    }

    void RequestBlocks()
    {
        BIND(&TReadBlockSetSession::DoRequestBlocks, MakeStrong(this))
            .Via(SessionInvoker_)
            .Run();
    }

    bool UpdatePeerBlockMap(
        const TDataNodeServiceProxy::TRspGetBlockSetPtr& rsp,
        const TReplicationReaderPtr& reader)
    {
        if (!ReaderConfig_->FetchFromPeers && rsp->peer_descriptors_size() > 0) {
            LOG_DEBUG("Peer suggestions received but ignored");
            return false;
        }

        bool addedNewPeers = false;
        for (const auto& peerDescriptor : rsp->peer_descriptors()) {
            int blockIndex = peerDescriptor.block_index();
            TBlockId blockId(reader->ChunkId_, blockIndex);
            for (const auto& protoPeerDescriptor : peerDescriptor.node_descriptors()) {
                auto suggestedDescriptor = FromProto<TNodeDescriptor>(protoPeerDescriptor);
                auto suggestedAddress = suggestedDescriptor.FindAddress(Networks_);
                if (suggestedAddress) {
                    if (AddPeer(*suggestedAddress, suggestedDescriptor, EPeerType::Peer)) {
                        addedNewPeers = true;
                    }
                    PeerBlocksMap_[*suggestedAddress].insert(blockIndex);
                    LOG_DEBUG("Peer descriptor received (Block: %v, SuggestedAddress: %v)",
                        blockIndex,
                        *suggestedAddress);
                } else {
                    LOG_WARNING("Peer suggestion ignored, required network is missing (Block: %v, SuggestedAddress: %v)",
                        blockIndex,
                        suggestedDescriptor.GetDefaultAddress());
                }
            }
        }

        return addedNewPeers;
    }

    void DoRequestBlocks()
    {
        auto reader = Reader_.Lock();
        if (!reader || IsCanceled())
            return;

        FetchBlocksFromCache(reader);

        auto blockIndexes = GetUnfetchedBlockIndexes();
        if (blockIndexes.empty()) {
            OnSessionSucceeded();
            return;
        }

        auto getBestPeer = [&] () -> TNullable<TPeer> {
            auto candidates = PickPeerCandidates(
                ReaderConfig_->ProbePeerCount,
                [&] (const TString& address) {
                    return HasUnfetchedBlocks(address, blockIndexes);
                },
                reader);
            if (candidates.empty()) {
                return Null;
            }

            return SelectBestPeer(candidates, blockIndexes, reader);
        };

        auto maybePeer = getBestPeer();
        if (!maybePeer) {
            OnPassCompleted();
            return;
        }

        const auto& peerAddress = maybePeer->Address;
        auto channel = GetChannel(peerAddress);
        if (!channel) {
            RequestBlocks();
            return;
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(ReaderConfig_->BlockRpcTimeout);

        auto req = proxy.GetBlockSet();
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_chunk_id(), reader->ChunkId_);
        ToProto(req->mutable_block_indexes(), blockIndexes);
        req->set_populate_cache(ReaderConfig_->PopulateCache);
        ToProto(req->mutable_workload_descriptor(), WorkloadDescriptor_);
        if (ReaderOptions_->EnableP2P) {
            auto expirationTime = TInstant::Now() + ReaderConfig_->PeerExpirationTimeout;
            ToProto(req->mutable_peer_descriptor(), reader->LocalDescriptor_);
            req->set_peer_expiration_time(expirationTime.GetValue());
        }

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            ProcessError(
                rspOrError,
                peerAddress,
                TError("Error fetching blocks from node %v", peerAddress));

            RequestBlocks();
            return;
        }

        const auto& rsp = rspOrError.Value();

        reader->AccountTraffic(
            rsp->GetTotalSize(),
            maybePeer->NodeDescriptor);

        UpdatePeerBlockMap(rsp, reader);

        if (rsp->net_throttling() || rsp->disk_throttling()) {
            LOG_DEBUG("Peer is throttling (Address: %v)", peerAddress);
        }

        if (rsp->has_chunk_reader_statistics()) {
            UpdateFromProto(&SessionOptions_.ChunkReaderStatistics, rsp->chunk_reader_statistics());
        }

        i64 bytesReceived = 0;
        std::vector<int> receivedBlockIndexes;

        auto blocks = GetRpcAttachedBlocks(rsp);
        for (int index = 0; index < blocks.size(); ++index) {
            const auto& block = blocks[index];
            if (!block)
                continue;

            int blockIndex = req->block_indexes(index);
            auto blockId = TBlockId(reader->ChunkId_, blockIndex);

            try {
                block.ValidateChecksum();
            } catch (const TBlockChecksumValidationException& ex) {
                RegisterError(TError("Failed to validate received block checksum")
                    << TErrorAttribute("block_id", ToString(blockId))
                    << TErrorAttribute("peer", peerAddress)
                    << TErrorAttribute("actual", ex.GetActual())
                    << TErrorAttribute("expected", ex.GetExpected()));

                BanPeer(peerAddress, false);
                RequestBlocks();
                return;
            }

            auto sourceDescriptor = ReaderOptions_->EnableP2P
                ? TNullable<TNodeDescriptor>(GetPeerDescriptor(peerAddress))
                : TNullable<TNodeDescriptor>(Null);

            reader->BlockCache_->Put(blockId, EBlockType::CompressedData, block, sourceDescriptor);

            YCHECK(Blocks_.insert(std::make_pair(blockIndex, block)).second);
            bytesReceived += block.Size();
            TotalBytesReceived_ += block.Size();
            receivedBlockIndexes.push_back(blockIndex);
        }

        BanSeedIfUncomplete(rsp, peerAddress);

        if (bytesReceived > 0) {
            // Reinstall peer into peer queue, if some data was received.
            ReinstallPeer(peerAddress);
        }

        LOG_DEBUG("Finished processing block response (Address: %v, PeerType: %v, BlocksReceived: %v, BytesReceived: %v, PeersSuggested: %v)",
              peerAddress,
              maybePeer->Type,
              receivedBlockIndexes,
              bytesReceived,
              rsp->peer_descriptors_size());


        if (peerAddress != GetLocalHostName()) {
            auto throttleResult = WaitFor(reader->Throttler_->Throttle(bytesReceived));
            if (!throttleResult.IsOK()) {
                auto error = TError(
                    NChunkClient::EErrorCode::BandwidthThrottlingFailed,
                    "Failed to throttle bandwidth in reader")
                    << throttleResult;
                LOG_WARNING(error, "Chunk reader failed");
                OnSessionFailed(true, error);
                return;
            }
        }

        RequestBlocks();
    }

    void OnSessionSucceeded()
    {
        LOG_DEBUG("All requested blocks are fetched");

        std::vector<TBlock> blocks;
        blocks.reserve(BlockIndexes_.size());
        for (int blockIndex : BlockIndexes_) {
            const auto& block = Blocks_[blockIndex];
            YCHECK(block.Data);
            blocks.push_back(block);
        }
        Promise_.TrySet(std::vector<TBlock>(blocks));
    }

    virtual void OnSessionFailed(bool fatal) override
    {
        auto error = BuildCombinedError(TError(
            "Error fetching blocks for chunk %v",
            ChunkId_));
        OnSessionFailed(fatal, error);
    }

    void OnSessionFailed(bool fatal, const TError& error)
    {
        if (fatal) {
            SetReaderFailed();
        }

        Promise_.TrySet(error);
    }
};

TFuture<std::vector<TBlock>> TReplicationReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    const std::vector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadBlockSetSession>(this, options, blockIndexes);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TReadBlockRangeSession
    : public TSessionBase
{
public:
    TReadBlockRangeSession(
        TReplicationReader* reader,
        const TClientBlockReadOptions& options,
        int firstBlockIndex,
        int blockCount)
        : TSessionBase(reader, options)
        , FirstBlockIndex_(firstBlockIndex)
        , BlockCount_(blockCount)
    {
        Logger.AddTag("Blocks: %v-%v",
            FirstBlockIndex_,
            FirstBlockIndex_ + BlockCount_ - 1);
    }

    TFuture<std::vector<TBlock>> Run()
    {
        if (BlockCount_ == 0) {
            return MakeFuture(std::vector<TBlock>());
        }

        StartTimestamp_ = TInstant::Now();
        TotalBytesReceived_ = 0;
        NextRetry();
        return Promise_;
    }

private:
    //! First block index to fetch.
    const int FirstBlockIndex_;

    //! Number of blocks to fetch.
    const int BlockCount_;

    //! Promise representing the session.
    TPromise<std::vector<TBlock>> Promise_ = NewPromise<std::vector<TBlock>>();

    //! Blocks that are fetched so far.
    std::vector<TBlock> FetchedBlocks_;

    virtual bool IsCanceled() const override
    {
        return Promise_.IsCanceled();
    }

    virtual void NextPass() override
    {
        if (!PrepareNextPass())
            return;

        RequestBlocks();
    }

    void RequestBlocks()
    {
        BIND(&TReadBlockRangeSession::DoRequestBlocks, MakeStrong(this))
            .Via(SessionInvoker_)
            .Run();
    }

    void DoRequestBlocks()
    {
        auto reader = Reader_.Lock();
        if (!reader || IsCanceled())
            return;

        YCHECK(FetchedBlocks_.empty());

        auto candidates = PickPeerCandidates(
            1,
            [] (const TString& address) {
                return true;
            },
            reader);

        if (candidates.empty()) {
            OnPassCompleted();
            return;
        }

        const auto& peerAddress = candidates.front().Address;
        auto channel = GetChannel(peerAddress);
        if (!channel) {
            RequestBlocks();
            return;
        }

        LOG_DEBUG("Requesting blocks from peer (Address: %v, Blocks: %v-%v)",
            peerAddress,
            FirstBlockIndex_,
            FirstBlockIndex_ + BlockCount_ - 1);

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(ReaderConfig_->BlockRpcTimeout);

        auto req = proxy.GetBlockRange();
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        ToProto(req->mutable_chunk_id(), reader->ChunkId_);
        req->set_first_block_index(FirstBlockIndex_);
        req->set_block_count(BlockCount_);
        ToProto(req->mutable_workload_descriptor(), WorkloadDescriptor_);

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            ProcessError(
                rspOrError,
                peerAddress,
                TError("Error fetching blocks from node %v", peerAddress));

            RequestBlocks();
            return;
        }

        const auto& rsp = rspOrError.Value();

        if (rsp->has_chunk_reader_statistics()) {
            UpdateFromProto(&SessionOptions_.ChunkReaderStatistics, rsp->chunk_reader_statistics());
        }

        auto blocks = GetRpcAttachedBlocks(rsp);

        int blocksReceived = 0;
        i64 bytesReceived = 0;

        for (const auto& block : blocks) {
            if (!block) {
                break;
            }

            blocksReceived += 1;
            bytesReceived += block.Size();
            TotalBytesReceived_ += block.Size();

            try {
                block.ValidateChecksum();
            } catch (const TBlockChecksumValidationException& ex) {
                RegisterError(TError("Failed to validate received block checksum")
                    << TErrorAttribute("block_id", ToString(TBlockId(reader->ChunkId_, FirstBlockIndex_ + blocksReceived)))
                    << TErrorAttribute("peer", peerAddress)
                    << TErrorAttribute("actual", ex.GetActual())
                    << TErrorAttribute("expected", ex.GetExpected()));

                BanPeer(peerAddress, false);
                RequestBlocks();
                return;
             }

             FetchedBlocks_.emplace_back(std::move(block));
         }

        BanSeedIfUncomplete(rsp, peerAddress);

        if (rsp->net_throttling() || rsp->disk_throttling()) {
            LOG_DEBUG("Peer is throttling (Address: %v)", peerAddress);
        } else if (blocksReceived == 0) {
            LOG_DEBUG("Peer has no relevant blocks (Address: %v)", peerAddress);
            BanPeer(peerAddress, false);
        } else {
            ReinstallPeer(peerAddress);
        }

        LOG_DEBUG("Finished processing block response (Address: %v, BlocksReceived: %v-%v, BytesReceived: %v)",
            peerAddress,
            FirstBlockIndex_,
            FirstBlockIndex_ + blocksReceived - 1,
            bytesReceived);
        
        if (peerAddress != GetLocalHostName()) {
            auto throttleResult = WaitFor(reader->Throttler_->Throttle(bytesReceived));
            if (!throttleResult.IsOK()) {
                auto error = TError(
                    NChunkClient::EErrorCode::BandwidthThrottlingFailed,
                    "Failed to throttle bandwidth in reader")
                    << throttleResult;
                LOG_WARNING(error, "Chunk reader failed");
                OnSessionFailed(true, error);
                return;
            }
        }

        if (blocksReceived > 0) {
            OnSessionSucceeded();
        } else {
            RequestBlocks();
        }
    }

    void OnSessionSucceeded()
    {
        LOG_DEBUG("Some blocks are fetched (Blocks: %v-%v)",
            FirstBlockIndex_,
            FirstBlockIndex_ + FetchedBlocks_.size() - 1);

        Promise_.TrySet(std::vector<TBlock>(FetchedBlocks_));
    }

    virtual void OnSessionFailed(bool fatal) override
    {
        auto error = BuildCombinedError(TError(
            "Error fetching blocks for chunk %v",
            ChunkId_));
        OnSessionFailed(fatal, error);
    }

    void OnSessionFailed(bool fatal, const TError& error)
    {
        if (fatal) {
            SetReaderFailed();
        }

        Promise_.TrySet(error);
    }
};

TFuture<std::vector<TBlock>> TReplicationReader::ReadBlocks(
    const TClientBlockReadOptions& options,
    int firstBlockIndex,
    int blockCount)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadBlockRangeSession>(this, options, firstBlockIndex, blockCount);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TGetMetaSession
    : public TSessionBase
{
public:
    TGetMetaSession(
        TReplicationReader* reader,
        const TClientBlockReadOptions& options,
        const TNullable<int> partitionTag,
        const TNullable<std::vector<int>>& extensionTags)
        : TSessionBase(reader, options)
        , PartitionTag_(partitionTag)
        , ExtensionTags_(extensionTags)
    { }

    ~TGetMetaSession()
    {
        Promise_.TrySet(TError("Reader terminated"));
    }

    TFuture<NProto::TChunkMeta> Run()
    {
        StartTimestamp_ = TInstant::Now();
        TotalBytesReceived_ = 0;
        NextRetry();
        return Promise_;
    }

private:
    const TNullable<int> PartitionTag_;
    const TNullable<std::vector<int>> ExtensionTags_;

    //! Promise representing the session.
    TPromise<NProto::TChunkMeta> Promise_ = NewPromise<NProto::TChunkMeta>();

    virtual bool IsCanceled() const override
    {
        return Promise_.IsCanceled();
    }

    virtual void NextPass()
    {
        if (!PrepareNextPass())
            return;

        RequestMeta();
    }

    void RequestMeta()
    {
        // NB: strong ref here is the only reference that keeps session alive.
        BIND(&TGetMetaSession::DoRequestMeta, MakeStrong(this))
            .Via(SessionInvoker_)
            .Run();
    }

    void DoRequestMeta()
    {
        auto reader = Reader_.Lock();
        if (!reader || IsCanceled())
            return;

        auto candidates = PickPeerCandidates(
            1,
            [] (const TString& address) {
                return true;
            },
            reader);

        if (candidates.empty()) {
            OnPassCompleted();
            return;
        }

        const auto& peerAddress = candidates.front().Address;
        auto channel = GetChannel(peerAddress);
        if (!channel) {
            RequestMeta();
            return;
        }

        LOG_DEBUG("Requesting chunk meta (Address: %v)", peerAddress);

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(ReaderConfig_->MetaRpcTimeout);

        auto req = proxy.GetChunkMeta();
        // TODO(babenko): consider using light band instead when all metas become thin
        // CC: psushin@
        req->SetMultiplexingBand(EMultiplexingBand::Heavy);
        req->set_enable_throttling(true);
        ToProto(req->mutable_chunk_id(), reader->ChunkId_);
        req->set_all_extension_tags(!ExtensionTags_);
        if (PartitionTag_) {
            req->set_partition_tag(PartitionTag_.Get());
        }
        if (ExtensionTags_) {
            ToProto(req->mutable_extension_tags(), *ExtensionTags_);
        }
        ToProto(req->mutable_workload_descriptor(), WorkloadDescriptor_);

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            ProcessError(
                rspOrError,
                peerAddress,
                TError("Error fetching meta from node %v", peerAddress));

            RequestMeta();
            return;
        }

        const auto& rsp = rspOrError.Value();
        if (rsp->net_throttling()) {
            LOG_DEBUG("Peer is throttling (Address: %v)", peerAddress);
            RequestMeta();
        } else {
            if (rsp->has_chunk_reader_statistics()) {
                UpdateFromProto(&SessionOptions_.ChunkReaderStatistics, rsp->chunk_reader_statistics());
            }

            TotalBytesReceived_ += rsp->ByteSize();
            OnSessionSucceeded(rsp->chunk_meta());
        }
    }

    void OnSessionSucceeded(const NProto::TChunkMeta& chunkMeta)
    {
        LOG_DEBUG("Chunk meta obtained");
        Promise_.TrySet(chunkMeta);
    }

    virtual void OnSessionFailed(bool fatal) override
    {
        auto error = BuildCombinedError(TError(
            "Error fetching meta for chunk %v",
            ChunkId_));
        OnSessionFailed(fatal, error);
    }
    
    void OnSessionFailed(bool fatal, const TError& error)
    {
        if (fatal) {
            SetReaderFailed();
        }

        Promise_.TrySet(error);
    }
};

TFuture<NProto::TChunkMeta> TReplicationReader::GetMeta(
    const TClientBlockReadOptions& options,
    const TNullable<int>& partitionTag,
    const TNullable<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TGetMetaSession>(this, options, partitionTag, extensionTags);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    const TNodeDescriptor& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler)
{
    YCHECK(config);
    YCHECK(blockCache);
    YCHECK(client);
    YCHECK(nodeDirectory);

    auto reader = New<TReplicationReader>(
        std::move(config),
        std::move(options),
        std::move(client),
        std::move(nodeDirectory),
        localDescriptor,
        chunkId,
        seedReplicas,
        std::move(blockCache),
        std::move(throttler),
        std::move(trafficMeter));
    reader->Initialize();
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
