#include "replication_reader.h"

#include "private.h"
#include "traffic_meter.h"
#include "block_cache.h"
#include "block_id.h"
#include "chunk_reader.h"
#include "chunk_reader_host.h"
#include "config.h"
#include "data_node_service_proxy.h"
#include "helpers.h"
#include "chunk_reader_allowing_repair.h"
#include "chunk_reader_options.h"
#include "chunk_replica_cache.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/table_client/versioned_offloading_reader.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>
#include <yt/yt/ytlib/node_tracker_client/node_status_directory.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>
#include <yt/yt/client/table_client/wire_protocol.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/hedging_manager.h>
#include <yt/yt/core/misc/protobuf_helpers.h>
#include <yt/yt/core/misc/memory_usage_tracker.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/hedging_channel.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/ymath.h>

#include <util/random/shuffle.h>

#include <algorithm>
#include <cmath>

namespace NYT::NChunkClient {
namespace {

using namespace NConcurrency;
using namespace NHydra;
using namespace NRpc;
using namespace NApi;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NNet;
using namespace NTableClient;

using NNodeTrackerClient::TNodeId;
using NYT::ToProto;
using NYT::FromProto;

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
    TChunkReplicaWithMedium Replica;
    std::string Address;
    const TNodeDescriptor* NodeDescriptor;
    EPeerType Type;
    EAddressLocality Locality;
    int MediumPriority = 0;
    std::optional<TInstant> NodeSuspicionMarkTime;
};

void FormatValue(TStringBuilderBase* builder, const TPeer& peer, TStringBuf format)
{
    FormatValue(builder, peer.Address, format);
    if (peer.Replica.GetMediumIndex() != GenericMediumIndex) {
        builder->AppendFormat("@%v", peer.Replica.GetMediumIndex());
    }
}

using TPeerList = TCompactVector<TPeer, 3>;

////////////////////////////////////////////////////////////////////////////////

struct TPeerQueueEntry
{
    TPeer Peer;
    int BanCount = 0;
    ui32 Random = RandomNumber<ui32>();
};

////////////////////////////////////////////////////////////////////////////////

class TRequestBatcher;
class TSessionBase;
class TReadBlockSetSession;
class TReadBlockRangeSession;
class TGetMetaSession;
class TLookupRowsSession;

DECLARE_REFCOUNTED_CLASS(TRequestBatcher)
DECLARE_REFCOUNTED_CLASS(TReplicationReader)

////////////////////////////////////////////////////////////////////////////////

struct IRequestBatcher
    : public TRefCounted
{
    using TPeerResponsePtr = TDataNodeServiceProxy::TRspProbeBlockSetPtr;
    using TGetBlocksResponsePtr = TDataNodeServiceProxy::TRspGetBlockSetPtr;

    struct TRequest
    {
        std::string Address;
        IChannelPtr Channel;
        std::vector<int> BlockIndexes;
        TIntrusivePtr<TSessionBase> Session;

        std::vector<NProto::TP2PBarrier> Barriers;
        TPeerList Peers;
    };

    struct TGetBlocksResult
    {
        std::vector<int> RequestedBlocks;
        TErrorOr<TGetBlocksResponsePtr> Response;
    };

    virtual TFuture<TPeerResponsePtr> ProbeBlockSet(const TRequest& request) = 0;

    virtual TFuture<TGetBlocksResult> GetBlockSet(const TRequest& request, bool hedgingEnabled) = 0;
};

DECLARE_REFCOUNTED_STRUCT(IRequestBatcher)
DEFINE_REFCOUNTED_TYPE(IRequestBatcher)

////////////////////////////////////////////////////////////////////////////////

IRequestBatcherPtr CreateRequestBatcher(TWeakPtr<TReplicationReader> reader);

////////////////////////////////////////////////////////////////////////////////

class TReplicationReader
    : public IChunkReaderAllowingRepair
    , public IOffloadingReader
{
public:
    TReplicationReader(
        TReplicationReaderConfigPtr config,
        TRemoteReaderOptionsPtr options,
        TChunkReaderHostPtr chunkReaderHost,
        TChunkId chunkId,
        TChunkReplicaWithMediumList seedReplicas)
        : Config_(std::move(config))
        , Options_(std::move(options))
        , Client_(chunkReaderHost->Client)
        , NodeDirectory_(Client_->GetNativeConnection()->GetNodeDirectory())
        , MediumDirectory_(Client_->GetNativeConnection()->GetMediumDirectory())
        , LocalDescriptor_(chunkReaderHost->LocalDescriptor)
        , ChunkId_(chunkId)
        , BlockCache_(chunkReaderHost->BlockCache)
        , ChunkMetaCache_(chunkReaderHost->ChunkMetaCache)
        , TrafficMeter_(chunkReaderHost->TrafficMeter)
        , NodeStatusDirectory_(chunkReaderHost->NodeStatusDirectory)
        , BandwidthThrottler_(chunkReaderHost->BandwidthThrottler)
        , RpsThrottler_(chunkReaderHost->RpsThrottler)
        , MediumThrottler_(chunkReaderHost->MediumThrottler)
        , Networks_(Client_->GetNativeConnection()->GetNetworks())
        , Logger(ChunkClientLogger().WithTag("ChunkId: %v",
            ChunkId_))
        , RequestBatcher_(CreateRequestBatcher(MakeWeak(this)))
        , InitialSeeds_(std::move(seedReplicas))
    {
        YT_VERIFY(NodeDirectory_);

        if (!Options_->AllowFetchingSeedsFromMaster && InitialSeeds_.empty()) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::NoChunkSeedsGiven,
                "Cannot read chunk %v: master seeds retries are disabled and no initial seeds are given",
                ChunkId_);
        }

        YT_LOG_DEBUG("Replication reader initialized "
            "(InitialSeedReplicas: %v, FetchPromPeers: %v, LocalDescriptor: %v, PopulateCache: %v, "
            "AllowFetchingSeedsFromMaster: %v, Networks: %v)",
            MakeFormattableView(InitialSeeds_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            Config_->FetchFromPeers,
            LocalDescriptor_,
            Config_->PopulateCache,
            Options_->AllowFetchingSeedsFromMaster,
            Networks_);
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override;

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override;

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TGetMetaOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override;

    TFuture<TSharedRef> LookupRows(
        TOffloadingReaderOptionsPtr options,
        TSharedRange<TLegacyKey> lookupKeys,
        std::optional<i64> estimatedSize,
        NCompression::ECodec codecId,
        IInvokerPtr sessionInvoker) override;

    TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

    TInstant GetLastFailureTime() const override
    {
        return LastFailureTime_;
    }

    void SetFailed()
    {
        LastFailureTime_ = NProfiling::GetInstant();
    }

    void SetSlownessChecker(TCallback<TError(i64, TDuration)> slownessChecker) override
    {
        SlownessChecker_ = slownessChecker;
    }

    TError RunSlownessChecker(i64 bytesReceived, TInstant startTimestamp)
    {
        if (!SlownessChecker_) {
            return {};
        }
        auto timePassed = TInstant::Now() - startTimestamp;
        return SlownessChecker_(bytesReceived, timePassed);
    }

    //! Looks for chunk meta in ChunkMetaCache if the cache is enabled.
    TFuture<TRefCountedChunkMetaPtr> FindMetaInCache(
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags,
        const ICachedChunkMeta::TMetaFetchCallback& callback);

private:
    friend class TRequestBatcher;
    friend class TSessionBase;
    friend class TReadBlockSetSession;
    friend class TReadBlockRangeSession;
    friend class TGetMetaSession;
    friend class TLookupRowsSession;

    const TReplicationReaderConfigPtr Config_;
    const TRemoteReaderOptionsPtr Options_;
    const NNative::IClientPtr Client_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TMediumDirectoryPtr MediumDirectory_;
    const TNodeDescriptor LocalDescriptor_;
    const TChunkId ChunkId_;
    const IBlockCachePtr BlockCache_;
    const IClientChunkMetaCachePtr ChunkMetaCache_;
    const TTrafficMeterPtr TrafficMeter_;
    const INodeStatusDirectoryPtr NodeStatusDirectory_;
    const IThroughputThrottlerPtr BandwidthThrottler_;
    const IThroughputThrottlerPtr RpsThrottler_;
    const IThroughputThrottlerPtr MediumThrottler_;
    const TNetworkPreferenceList Networks_;

    const NLogging::TLogger Logger;

    const IRequestBatcherPtr RequestBatcher_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PeersSpinLock_);
    NHydra::TRevision FreshSeedsRevision_ = NHydra::NullRevision;
    //! Peers returning NoSuchChunk error are banned forever.
    THashSet<std::string> BannedForeverPeers_;
    //! Every time peer fails (e.g. time out occurs), we increase ban counter.
    THashMap<std::string, int> PeerBanCountMap_;
    //! If AllowFetchingSeedsFromMaster is |true| InitialSeeds_ (if present) are used
    //! until 'DiscardSeeds' is called for the first time.
    //! If AllowFetchingSeedsFromMaster is |false| InitialSeeds_ must be given and cannot be discarded.
    TChunkReplicaWithMediumList InitialSeeds_;

    std::atomic<TInstant> LastFailureTime_ = TInstant();
    TCallback<TError(i64, TDuration)> SlownessChecker_;


    bool CanFetchSeedsFromMaster() const
    {
        return Options_->AllowFetchingSeedsFromMaster && !Options_->UseProxyingDataNodeService;
    }

    TFuture<TAllyReplicasInfo> GetReplicasFuture()
    {
        {
            auto guard = Guard(PeersSpinLock_);

            if (!InitialSeeds_.empty()) {
                auto seeds = InitialSeeds_;
                guard.Release();
                return MakeFuture(TAllyReplicasInfo::FromChunkReplicas(seeds));
            }
        }

        YT_VERIFY(CanFetchSeedsFromMaster());
        const auto& chunkReplicaCache = Client_->GetNativeConnection()->GetChunkReplicaCache();
        auto futures = chunkReplicaCache->GetReplicas({DecodeChunkId(ChunkId_).Id});
        YT_VERIFY(futures.size() == 1);
        return futures[0];
    }

    void DiscardSeeds(const TFuture<TAllyReplicasInfo>& future)
    {
        if (!CanFetchSeedsFromMaster()) {
            // We're not allowed to ask master for seeds.
            // Better keep the initial ones.
            return;
        }

        {
            auto guard = Guard(PeersSpinLock_);
            InitialSeeds_.clear();
        }

        Client_->GetNativeConnection()->GetChunkReplicaCache()->DiscardReplicas(
            DecodeChunkId(ChunkId_).Id,
            future);
    }

    void OnChunkReplicasLocated(const TAllyReplicasInfo& seedReplicas)
    {
        auto guard = Guard(PeersSpinLock_);

        if (FreshSeedsRevision_ >= seedReplicas.Revision) {
            return;
        }

        FreshSeedsRevision_ = seedReplicas.Revision;

        for (auto replica : seedReplicas.Replicas) {
            const auto* nodeDescriptor = NodeDirectory_->FindDescriptor(replica.GetNodeId());
            if (!nodeDescriptor) {
                YT_LOG_WARNING("Skipping replica with unresolved node id (NodeId: %v)", replica.GetNodeId());
                continue;
            }
            if (auto address = nodeDescriptor->FindAddress(Networks_)) {
                BannedForeverPeers_.erase(*address);
            }
        }
    }

    //! Notifies reader about peer banned inside one of the sessions.
    void OnPeerBanned(const std::string& peerAddress)
    {
        auto guard = Guard(PeersSpinLock_);
        auto [it, inserted] = PeerBanCountMap_.emplace(peerAddress, 1);
        if (!inserted) {
            ++it->second;
        }

        if (it->second > Config_->MaxBanCount) {
            BannedForeverPeers_.insert(peerAddress);
        }
    }

    void BanPeerForever(const std::string& peerAddress)
    {
        auto guard = Guard(PeersSpinLock_);
        BannedForeverPeers_.insert(peerAddress);
    }

    int GetBanCount(const std::string& peerAddress) const
    {
        auto guard = Guard(PeersSpinLock_);
        auto it = PeerBanCountMap_.find(peerAddress);
        return it == PeerBanCountMap_.end() ? 0 : it->second;
    }

    bool IsPeerBannedForever(const std::string& peerAddress) const
    {
        if (!Config_->BanPeersPermanently) {
            return false;
        }

        auto guard = Guard(PeersSpinLock_);
        return BannedForeverPeers_.contains(peerAddress);
    }

    void AccountTraffic(i64 transferredByteCount, const TNodeDescriptor& srcDescriptor)
    {
        if (TrafficMeter_) {
            TrafficMeter_->IncrementInboundByteCount(srcDescriptor.GetDataCenter(), transferredByteCount);
        }
    }

};

DEFINE_REFCOUNTED_TYPE(TReplicationReader)

////////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public TRefCounted
{
protected:
    struct TBlockInfo
    {
        i32 BlockIndex;
        i64 BlockSize;
    };

    struct TPeerProbeResult
    {
        bool NetThrottling = false;
        bool DiskThrottling = false;
        i64 NetQueueSize = 0;
        i64 DiskQueueSize = 0;
        ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TPeerDescriptor> PeerDescriptors;
        TAllyReplicasInfo AllyReplicas;
        bool HasCompleteChunk = false;
        std::vector<TBlockInfo> CachedBlocks;
        i64 CachedBlockCount = 0;
        i64 CachedBlockSize = 0;

        double PeerRating = 0.0;
    };

    using TErrorOrPeerProbeResult = TErrorOr<TPeerProbeResult>;

    template <class TRspPtr>
    static TPeerProbeResult ParseProbeResponse(const TRspPtr& rsp)
    {
        std::vector<TBlockInfo> blockInfos;
        blockInfos.reserve(rsp->cached_blocks_size());

        for (int index = 0; index < rsp->cached_blocks_size(); index++) {
            blockInfos.emplace_back(
                rsp->cached_blocks()[index].block_index(),
                rsp->cached_blocks()[index].block_size());
        }

        return {
            .NetThrottling = rsp->net_throttling(),
            .DiskThrottling = rsp->disk_throttling(),
            .NetQueueSize = rsp->net_queue_size(),
            .DiskQueueSize = rsp->disk_queue_size(),
            .PeerDescriptors = rsp->peer_descriptors(),
            .AllyReplicas = FromProto<TAllyReplicasInfo>(rsp->ally_replicas()),
            .HasCompleteChunk = rsp->has_complete_chunk(),
            .CachedBlocks = std::move(blockInfos),
        };
    }

    virtual bool UpdatePeerBlockMap(
        const TPeer& /*suggestorPeer*/,
        const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TPeerDescriptor>& /*peerDescriptors*/,
        const TAllyReplicasInfo& /*allyReplicas*/)
    {
        // P2P is not supported by default.
        return false;
    }

    //! Reference to the owning reader.
    const TWeakPtr<TReplicationReader> Reader_;

    const TReplicationReaderConfigPtr ReaderConfig_;
    const TRemoteReaderOptionsPtr ReaderOptions_;
    const TChunkId ChunkId_;

    const TClientChunkReadOptions SessionOptions_;

    const INodeStatusDirectoryPtr NodeStatusDirectory_;

    //! The workload descriptor from the config with instant field updated
    //! properly.
    const TWorkloadDescriptor WorkloadDescriptor_;

    //! Either fixed priority invoker build upon CompressionPool or fair share thread pool invoker assigned to
    //! compression fair share tag from the workload descriptor.
    const IInvokerPtr SessionInvoker_;

    //! Translates node ids to node descriptors.
    const TNodeDirectoryPtr NodeDirectory_;

    //! Translates medium indices to medium descriptors.
    const TMediumDirectoryPtr MediumDirectory_;

    //! List of the networks to use from descriptor.
    const TNetworkPreferenceList Networks_;

    const IThroughputThrottlerPtr BandwidthThrottler_;
    const IThroughputThrottlerPtr RpsThrottler_;
    const IThroughputThrottlerPtr MediumThrottler_;
    const IThroughputThrottlerPtr CombinedDataByteThrottler_;

    const IRequestBatcherPtr RequestBatcher_;

    NLogging::TLogger Logger;

    //! Zero based retry index (less than |ReaderConfig_->RetryCount|).
    int RetryIndex_ = 0;

    //! Zero based pass index (less than |ReaderConfig_->PassCount|).
    int PassIndex_ = 0;

    //! Seed replicas for the current retry.
    TAllyReplicasInfo SeedReplicas_;

    //! Set of peer addresses banned for the current retry.
    THashSet<std::string> BannedPeers_;

    //! List of candidates addresses to try during current pass, prioritized by:
    //! locality, ban counter, random number.
    using TPeerQueue = std::priority_queue<
        TPeerQueueEntry,
        std::vector<TPeerQueueEntry>,
        std::function<bool(const TPeerQueueEntry&, const TPeerQueueEntry&)>
    >;
    TPeerQueue PeerQueue_;

    //! Catalogue of peers, seen on current pass.
    THashMap<std::string, TPeer> Peers_;

    //! The instant this session was started.
    TInstant StartTime_ = TInstant::Now();

    //! The instant current retry was started.
    TInstant RetryStartTime_;

    //! Total number of bytes received in this session; used to detect slow reads.
    i64 TotalBytesReceived_ = 0;

    //! Total number of bytes read from disk in this session.
    i64 TotalBytesReadFromDisk_ = 0;

    TSessionBase(
        TReplicationReader* reader,
        TClientChunkReadOptions options,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        IThroughputThrottlerPtr mediumThrottler,
        IInvokerPtr sessionInvoker = {})
        : Reader_(reader)
        , ReaderConfig_(reader->Config_)
        , ReaderOptions_(reader->Options_)
        , ChunkId_(reader->ChunkId_)
        , SessionOptions_(std::move(options))
        , NodeStatusDirectory_(reader->NodeStatusDirectory_)
        , WorkloadDescriptor_(ReaderConfig_->EnableWorkloadFifoScheduling
            ? SessionOptions_.WorkloadDescriptor.SetCurrentInstant()
            : SessionOptions_.WorkloadDescriptor)
        , SessionInvoker_(sessionInvoker ? std::move(sessionInvoker) : GetCompressionInvoker(WorkloadDescriptor_))
        , NodeDirectory_(reader->NodeDirectory_)
        , MediumDirectory_(reader->MediumDirectory_)
        , Networks_(reader->Networks_)
        , BandwidthThrottler_(std::move(bandwidthThrottler))
        , RpsThrottler_(std::move(rpsThrottler))
        , MediumThrottler_(std::move(mediumThrottler))
        , CombinedDataByteThrottler_(CreateCombinedDataByteThrottler())
        , RequestBatcher_(reader->RequestBatcher_)
        , Logger(ChunkClientLogger().WithTag("SessionId: %v, ReadSessionId: %v, ChunkId: %v",
            TGuid::Create(),
            SessionOptions_.ReadSessionId,
            ChunkId_))
    {
        if (WorkloadDescriptor_.CompressionFairShareTag) {
            Logger.AddTag("CompressionFairShareTag: %v", WorkloadDescriptor_.CompressionFairShareTag);
        }

        SessionOptions_.ChunkReaderStatistics->RecordSession();

        ResetPeerQueue();
    }

    EAddressLocality GetNodeLocality(const TNodeDescriptor& descriptor)
    {
        auto reader = Reader_.Lock();
        return reader ? ComputeAddressLocality(descriptor, reader->LocalDescriptor_) : EAddressLocality::None;
    }

    int GetMediumPriority(TChunkReplicaWithMedium replica)
    {
        const auto* descriptor = MediumDirectory_->FindByIndex(replica.GetMediumIndex());
        return descriptor ? descriptor->Priority : 0;
    }

    IThroughputThrottlerPtr CreateCombinedDataByteThrottler() const
    {
        return CreateCombinedThrottler({
            BandwidthThrottler_,
            MediumThrottler_,
        });
    }

    bool SyncThrottle(const IThroughputThrottlerPtr& throttler, i64 count)
    {
        auto throttlerFuture = throttler->Throttle(count);
        SetSessionFuture(throttlerFuture);

        auto throttleResult = WaitForFast(throttlerFuture);
        if (!throttleResult.IsOK()) {
            auto error = TError(
                NChunkClient::EErrorCode::ReaderThrottlingFailed,
                "Failed to apply throttling in reader")
                << throttleResult;
            OnSessionFailed(true, error);
            return false;
        }

        return true;
    }

    void AsyncThrottle(const IThroughputThrottlerPtr& throttler, i64 count, TClosure onSuccess)
    {
        auto throttlerFuture = throttler->Throttle(count);
        SetSessionFuture(throttlerFuture);
        throttlerFuture
            .Subscribe(BIND([=, this, this_ = MakeStrong(this), onSuccess = std::move(onSuccess)] (const TError& throttleResult) {
                if (!throttleResult.IsOK()) {
                    auto error = TError(
                        NChunkClient::EErrorCode::ReaderThrottlingFailed,
                        "Failed to apply throttling in reader")
                        << throttleResult;
                    OnSessionFailed(true, error);
                    return;
                }
                onSuccess();
            }).Via(SessionInvoker_));
    }

    // NB: Now we use this method only in case of failed session.
    void ReleaseThrottledBytesExcess(const IThroughputThrottlerPtr& throttler, i64 throttledBytes)
    {
        if (throttledBytes > TotalBytesReceived_) {
            YT_LOG_DEBUG("Releasing excess throttled bytes (ThrottledBytes: %v, ReceivedBytes: %v)",
                throttledBytes,
                TotalBytesReceived_);

            throttler->Release(throttledBytes - TotalBytesReceived_);
        }
    }

    void HandleChunkReaderStatistics(const NProto::TChunkReaderStatistics& protoChunkReaderStatistics)
    {
        UpdateFromProto(&SessionOptions_.ChunkReaderStatistics, protoChunkReaderStatistics);
        TotalBytesReadFromDisk_ += protoChunkReaderStatistics.data_bytes_read_from_disk();
    }

    void AccountExtraMediumBandwidth(i64 throttledBytes)
    {
        auto extraBytesToThrottle = TotalBytesReadFromDisk_ - throttledBytes;

        if (extraBytesToThrottle <= 0) {
            return;
        }

        YT_LOG_DEBUG("Accounting for extra medium bandwidth (TotalBytesReadFromDisk: %v, ThrottledBytes: %v)",
            TotalBytesReadFromDisk_,
            throttledBytes);

        MediumThrottler_->Acquire(extraBytesToThrottle);
    }

    void BanPeer(const std::string& address, bool forever)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        if (forever && !reader->IsPeerBannedForever(address)) {
            YT_LOG_DEBUG("Node is banned until seeds are re-fetched from master (Address: %v)", address);
            reader->BanPeerForever(address);
        }

        if (BannedPeers_.insert(address).second) {
            reader->OnPeerBanned(address);
            YT_LOG_DEBUG("Node is banned for the current retry (Address: %v, BanCount: %v)",
                address,
                reader->GetBanCount(address));
        }
    }

    const TNodeDescriptor& GetPeerDescriptor(const std::string& address)
    {
        return *GetOrCrash(Peers_, address).NodeDescriptor;
    }

    //! Register peer and install it into the peer queue if necessary.
    bool AddPeer(
        TChunkReplicaWithMedium replica,
        const std::string& address,
        const TNodeDescriptor& descriptor,
        EPeerType type,
        std::optional<TInstant> nodeSuspicionMarkTime)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return false;
        }

        auto optionalAddress = descriptor.FindAddress(Networks_);
        if (!optionalAddress) {
            YT_LOG_WARNING("Skipping peer since no suitable address could be found (NodeDescriptor: %v, Networks: %v)",
                descriptor,
                Networks_);
            return false;
        }

        TPeer peer{
            .Replica = replica,
            .Address = std::move(*optionalAddress),
            .NodeDescriptor = &descriptor,
            .Type = type,
            .Locality = GetNodeLocality(descriptor),
            .MediumPriority = GetMediumPriority(replica),
            .NodeSuspicionMarkTime = nodeSuspicionMarkTime
        };
        if (!Peers_.emplace(address, peer).second) {
            // Peer was already handled on the current pass.
            return false;
        }

        if (IsPeerBanned(address)) {
            // Peer is banned.
            return false;
        }

        PeerQueue_.push(TPeerQueueEntry{
            .Peer = std::move(peer),
            .BanCount = reader->GetBanCount(address)
        });
        return true;
    }

    //! Reinstall peer in the peer queue.
    void ReinstallPeer(const std::string& address)
    {
        auto reader = Reader_.Lock();
        if (!reader || IsPeerBanned(address)) {
            return;
        }

        YT_LOG_DEBUG("Reinstall peer into peer queue (Address: %v)", address);

        const auto& peer = GetOrCrash(Peers_, address);
        PeerQueue_.push(TPeerQueueEntry{
            .Peer = peer,
            .BanCount = reader->GetBanCount(address)
        });
    }

    bool IsSeed(const std::string& address)
    {
        return GetOrCrash(Peers_, address).Type == EPeerType::Seed;
    }

    bool IsPeerBanned(const std::string& address)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return false;
        }

        return BannedPeers_.find(address) != BannedPeers_.end() || reader->IsPeerBannedForever(address);
    }

    IChannelPtr GetChannel(const std::string& address)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return nullptr;
        }

        try {
            IChannelFactoryPtr channelFactory = reader->Client_->GetChannelFactory();

            if (reader->Options_->UseProxyingDataNodeService) {
                channelFactory = CreateRealmChannelFactory(
                    channelFactory,
                    ProxyingDataNodeServiceRealmId);
            }

            // TODO(akozhikhov): Don't catch here.
            return channelFactory->CreateChannel(address);
        } catch (const std::exception& ex) {
            RegisterError(ex);
            BanPeer(address, false);
            return nullptr;
        }
    }

    template <class T>
    void ProcessError(const TErrorOr<T>& rspOrError, const TPeer& peer, const TError& wrappingError)
    {
        auto code = rspOrError.GetCode();
        if (code == NYT::EErrorCode::Canceled) {
            return;
        }

        if (NodeStatusDirectory_ &&
            !peer.NodeSuspicionMarkTime &&
            NodeStatusDirectory_->ShouldMarkNodeSuspicious(rspOrError))
        {
            YT_LOG_WARNING("Node is marked as suspicious (Replica: %v, Address: %v, Error: %v)",
                peer.Replica,
                peer.Address,
                rspOrError);
            NodeStatusDirectory_->UpdateSuspicionMarkTime(
                peer.Replica.GetNodeId(),
                peer.Address,
                /*suspicious*/ true,
                std::nullopt);
        }

        auto error = wrappingError << rspOrError;
        if (code == NRpc::EErrorCode::Unavailable ||
            code == NRpc::EErrorCode::RequestQueueSizeLimitExceeded ||
            code == NHydra::EErrorCode::InvalidChangelogState)
        {
            YT_LOG_DEBUG(error);
            return;
        }

        BanPeer(peer.Address, code == NChunkClient::EErrorCode::NoSuchChunk);
        RegisterError(error);
    }

    TPeerList PickPeerCandidates(
        const TReplicationReaderPtr& reader,
        int count,
        bool enableEarlyExit,
        std::function<bool(const std::string&)> filter = {})
    {
        TPeerList candidates;
        while (!PeerQueue_.empty() && std::ssize(candidates) < count) {
            const auto& top = PeerQueue_.top();
            if (top.BanCount != reader->GetBanCount(top.Peer.Address)) {
                auto queueEntry = top;
                PeerQueue_.pop();
                queueEntry.BanCount = reader->GetBanCount(queueEntry.Peer.Address);
                PeerQueue_.push(queueEntry);
                continue;
            }

            if (!candidates.empty() && enableEarlyExit) {
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

            if ((!filter || filter(top.Peer.Address)) && !IsPeerBanned(top.Peer.Address)) {
                candidates.push_back(top.Peer);
            }

            PeerQueue_.pop();
        }
        return candidates;
    }

    IChannelPtr MakePeersChannel(
        const TPeerList& peers,
        const std::optional<THedgingChannelOptions>& hedgingOptions)
    {
        if (peers.empty()) {
            return nullptr;
        }

        if (peers.size() != 1 && hedgingOptions) {
            return CreateHedgingChannel(
                GetChannel(peers[0].Address),
                GetChannel(peers[1].Address),
                *hedgingOptions);
        }

        return GetChannel(peers[0].Address);
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

        YT_VERIFY(!SeedsFuture_);

        YT_LOG_DEBUG("Retry started (RetryIndex: %v/%v)",
            RetryIndex_ + 1,
            ReaderConfig_->RetryCount);

        SessionOptions_.ChunkReaderStatistics->RecordRetry();

        PassIndex_ = 0;
        BannedPeers_.clear();

        RetryStartTime_ = TInstant::Now();

        SeedsFuture_ = reader->GetReplicasFuture();
        SeedsFuture_.Subscribe(BIND(&TSessionBase::OnGotSeeds, MakeStrong(this))
            .Via(SessionInvoker_));
    }

    void OnRetryFailed()
    {
        DiscardSeeds();

        int retryCount = ReaderConfig_->RetryCount;
        YT_LOG_DEBUG("Retry failed (RetryIndex: %v/%v)",
            RetryIndex_ + 1,
            retryCount);

        ++RetryIndex_;
        if (RetryIndex_ >= retryCount) {
            OnSessionFailed(/*fatal*/ true);
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TSessionBase::NextRetry, MakeStrong(this))
                .Via(SessionInvoker_),
            GetBackoffDuration(RetryIndex_));
    }

    void MaybeUpdateSeeds(const TAllyReplicasInfo& allyReplicas)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        if (!allyReplicas) {
            return;
        }

        // NB: We could have changed current pass seeds,
        // but for the sake of simplicity that will be done upon next retry within standard reading pipeline.
        reader->Client_->GetNativeConnection()->GetChunkReplicaCache()->UpdateReplicas(
            DecodeChunkId(ChunkId_).Id,
            allyReplicas);
    }

    void DiscardSeeds()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        YT_VERIFY(SeedsFuture_);
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

        YT_LOG_DEBUG("Pass started (PassIndex: %v/%v)",
            PassIndex_ + 1,
            ReaderConfig_->PassCount);

        SessionOptions_.ChunkReaderStatistics->RecordPass();

        ResetPeerQueue();
        Peers_.clear();

        const auto& seedReplicas = SeedReplicas_.Replicas;

        std::vector<const TNodeDescriptor*> peerDescriptors;
        std::vector<TChunkReplicaWithMedium> replicas;
        std::vector<TNodeId> nodeIds;
        std::vector<std::string> peerAddresses;
        peerDescriptors.reserve(seedReplicas.size());
        replicas.reserve(seedReplicas.size());
        nodeIds.reserve(seedReplicas.size());
        peerAddresses.reserve(seedReplicas.size());

        for (auto replica : seedReplicas) {
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
                OnSessionFailed(/*fatal*/ true);
                return false;
            }

            peerDescriptors.push_back(descriptor);
            replicas.push_back(replica);
            nodeIds.push_back(replica.GetNodeId());
            peerAddresses.push_back(*address);
        }

        auto nodeSuspicionMarkTimes = NodeStatusDirectory_
            ? NodeStatusDirectory_->RetrieveSuspicionMarkTimes(nodeIds)
            : std::vector<std::optional<TInstant>>();
        for (int i = 0; i < std::ssize(peerDescriptors); ++i) {
            auto suspicionMarkTime = NodeStatusDirectory_
                ? nodeSuspicionMarkTimes[i]
                : std::nullopt;
            AddPeer(
                replicas[i],
                std::move(peerAddresses[i]),
                *peerDescriptors[i],
                EPeerType::Seed,
                suspicionMarkTime);
        }

        if (PeerQueue_.empty()) {
            RegisterError(TError(
                NChunkClient::EErrorCode::NoChunkSeedsKnown,
                "No feasible seeds to start a pass"));

            auto reader = Reader_.Lock();
            if (reader && reader->CanFetchSeedsFromMaster()) {
                OnRetryFailed();
            } else {
                OnSessionFailed(/*fatal*/ true);
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
        YT_LOG_DEBUG("Pass completed (PassIndex: %v/%v)",
            PassIndex_ + 1,
            passCount);

        ++PassIndex_;
        if (PassIndex_ >= passCount) {
            OnRetryFailed();
            return;
        }

        if (RetryStartTime_ + ReaderConfig_->RetryTimeout < TInstant::Now()) {
            RegisterError(TError(
                EErrorCode::ReaderTimeout,
                "Replication reader retry %v out of %v timed out",
                RetryIndex_,
                ReaderConfig_->RetryCount)
                << TErrorAttribute("retry_start_time", RetryStartTime_)
                << TErrorAttribute("retry_timeout", ReaderConfig_->RetryTimeout));
            OnRetryFailed();
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TSessionBase::NextPass, MakeStrong(this))
                .Via(SessionInvoker_),
            GetBackoffDuration(PassIndex_));
    }

    template <class TResponsePtr>
    void BanSeedIfIncomplete(const TResponsePtr& rsp, const std::string& address)
    {
        if (IsSeed(address) && !rsp->has_complete_chunk()) {
            YT_LOG_DEBUG("Seed does not contain the chunk (Address: %v)", address);
            BanPeer(address, false);
        }
    }

    void RegisterError(const TError& error, bool raiseAlert = false)
    {
        if (raiseAlert) {
            YT_LOG_ALERT(error);
        } else {
            YT_LOG_ERROR(error);
        }
        InnerErrors_.push_back(error);
    }

    TError BuildCombinedError(const TError& error)
    {
        return error << InnerErrors_;
    }

    virtual void NextPass() = 0;
    virtual void OnSessionFailed(bool fatal) = 0;
    virtual void OnSessionFailed(bool fatal, const TError& error) = 0;

    TPeerList ProbeAndSelectBestPeers(
        const TPeerList& candidates,
        int count,
        const std::vector<int>& blockIndexes)
    {
        if (count <= 0) {
            return {};
        }

        if (candidates.size() <= 1) {
            return {candidates.begin(), candidates.end()};
        }

        auto peerAndProbeResultsOrError = WaitFor(DoProbeAndSelectBestPeers(candidates, blockIndexes));
        YT_VERIFY(peerAndProbeResultsOrError.IsOK());

        return OnPeersProbed(std::move(peerAndProbeResultsOrError.Value()), count, blockIndexes);
    }

    TFuture<TPeerList> AsyncProbeAndSelectBestPeers(
        const TPeerList& candidates,
        int count,
        const std::vector<int>& blockIndexes)
    {
        if (count <= 0) {
            return {};
        }
        if (candidates.size() <= 1) {
            return MakeFuture<TPeerList>({candidates.begin(), candidates.end()});
        }

        return DoProbeAndSelectBestPeers(candidates, blockIndexes)
            .ApplyUnique(BIND(
                [=, this, this_ = MakeStrong(this)]
                (TErrorOr<std::vector<std::pair<TPeer, TErrorOrPeerProbeResult>>>&& peerAndProbeResultsOrError)
            {
                YT_VERIFY(peerAndProbeResultsOrError.IsOK());
                return OnPeersProbed(std::move(peerAndProbeResultsOrError.Value()), count, blockIndexes);
            }).AsyncVia(SessionInvoker_));
    }

    bool ShouldThrottle(const std::string& address, bool condition) const
    {
        return condition &&
            (ReaderConfig_->EnableLocalThrottling || !IsAddressLocal(address));
    }

    bool IsCanceled() const
    {
        auto guard = Guard(CancelationSpinLock_);
        return CancelationError_.has_value();
    }

    virtual void OnCanceled(const TError& error)
    {
        TFuture<void> sessionFuture;
        {
            auto guard = Guard(CancelationSpinLock_);

            if (CancelationError_) {
                return;
            }

            CancelationError_ = error;

            sessionFuture = SessionFuture_;
        }

        sessionFuture.Cancel(error);
    }

    void SetSessionFuture(TFuture<void> sessionFuture)
    {
        auto guard = Guard(CancelationSpinLock_);

        if (CancelationError_) {
            guard.Release();

            sessionFuture.Cancel(*CancelationError_);
            return;
        }

        SessionFuture_ = std::move(sessionFuture);
    }

private:
    friend class TRequestBatcher;

    //! Errors collected by the session.
    std::vector<TError> InnerErrors_;

    TFuture<TAllyReplicasInfo> SeedsFuture_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, CancelationSpinLock_);
    //! Sets the value upon cancelation.
    std::optional<TError> CancelationError_;
    //! Future of the previous cancellable action within session (e.g. Throttle, GetBlockSet).
    TFuture<void> SessionFuture_ = VoidFuture;


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
        if (lhs.Peer.MediumPriority < rhs.Peer.MediumPriority) {
            return -1;
        } else if (lhs.Peer.MediumPriority > rhs.Peer.MediumPriority) {
            return +1;
        }

        if (int result = ComparePeerLocality(lhs.Peer, rhs.Peer); result != 0) {
            return result;
        }

        if (lhs.Peer.Type != rhs.Peer.Type) {
            // Prefer Peers to Seeds to make most use of P2P.
            if (lhs.Peer.Type == EPeerType::Peer) {
                return 1;
            } else {
                YT_VERIFY(lhs.Peer.Type == EPeerType::Seed);
                return -1;
            }
        }

        if (lhs.Peer.NodeSuspicionMarkTime || rhs.Peer.NodeSuspicionMarkTime) {
            // Prefer peer that is not suspicious.
            // If both are suspicious prefer one that was suspicious for less time.
            auto lhsMarkTime = lhs.Peer.NodeSuspicionMarkTime.value_or(TInstant::Max());
            auto rhsMarkTime = rhs.Peer.NodeSuspicionMarkTime.value_or(TInstant::Max());
            return lhsMarkTime < rhsMarkTime
                ? -1
                : 1;
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

    void OnGotSeeds(const TErrorOr<TAllyReplicasInfo>& resultOrError)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        if (!resultOrError.IsOK()) {
            if (resultOrError.FindMatching(NChunkClient::EErrorCode::NoSuchChunk)) {
                RegisterError(resultOrError);
            } else {
                DiscardSeeds();
                RegisterError(TError(
                    NChunkClient::EErrorCode::MasterCommunicationFailed,
                    "Error requesting seeds from master")
                    << resultOrError);
            }
            OnSessionFailed(/*fatal*/ true);
            return;
        }

        SeedReplicas_ = resultOrError.Value();
        if (IsErasureChunkPartId(ChunkId_)) {
            auto replicaIndex = ReplicaIndexFromErasurePartId(ChunkId_);
            EraseIf(
                SeedReplicas_.Replicas,
                [&] (const auto& replica) {
                    return replica.GetReplicaIndex() != replicaIndex;
                });
        }

        reader->OnChunkReplicasLocated(SeedReplicas_);

        if (!SeedReplicas_) {
            RegisterError(TError(
                NChunkClient::EErrorCode::ChunkIsLost,
                "Chunk is lost"));
            if (ReaderConfig_->FailOnNoSeeds) {
                DiscardSeeds();
                OnSessionFailed(/*fatal*/ true);
            } else {
                OnRetryFailed();
            }
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

        if (StartTime_ + ReaderConfig_->SessionTimeout < TInstant::Now()) {
            RegisterError(TError(
                NChunkClient::EErrorCode::ReaderTimeout,
                "Replication reader session timed out")
                << TErrorAttribute("session_start_time", StartTime_)
                << TErrorAttribute("session_timeout", ReaderConfig_->SessionTimeout));
            OnSessionFailed(/*fatal*/ false);
            return true;
        }

        auto error = reader->RunSlownessChecker(TotalBytesReceived_, StartTime_);
        if (!error.IsOK()) {
            RegisterError(TError(
                NChunkClient::EErrorCode::ChunkReadSessionSlow,
                "Read session of chunk %v is slow; may attempt repair",
                ChunkId_)
                << error);
            OnSessionFailed(/*fatal*/ false);
            return true;
        }

        return false;
    }

    template <class TRspPtr>
    static std::pair<TPeer, TErrorOrPeerProbeResult> ParsePeerAndProbeResponse(
        TPeer peer,
        const TErrorOr<TRspPtr>& rspOrError)
    {
        if (rspOrError.IsOK()) {
            return {
                std::move(peer),
                TErrorOrPeerProbeResult(ParseProbeResponse(rspOrError.Value()))
            };
        } else {
            return {
                std::move(peer),
                TErrorOrPeerProbeResult(TError(rspOrError))
            };
        }
    }

    template <class TRspPtr>
    std::pair<TPeer, TErrorOrPeerProbeResult> ParseSuspiciousPeerAndProbeResponse(
        TPeer peer,
        const TErrorOr<TRspPtr>& rspOrError,
        const TFuture<TAllyReplicasInfo>& allyReplicasFuture,
        int totalPeerCount)
    {
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

        YT_VERIFY(peer.NodeSuspicionMarkTime && NodeStatusDirectory_);

        if (rspOrError.IsOK()) {
            NodeStatusDirectory_->UpdateSuspicionMarkTime(
                peer.Replica.GetNodeId(),
                peer.Address,
                /*suspicious*/ false,
                peer.NodeSuspicionMarkTime);
            peer.NodeSuspicionMarkTime.reset();

            return {
                std::move(peer),
                TErrorOrPeerProbeResult(ParseProbeResponse(rspOrError.Value()))
            };
        }

        if (ReaderConfig_->SuspiciousNodeGracePeriod && totalPeerCount > 1) {
            auto now = TInstant::Now();
            if (*peer.NodeSuspicionMarkTime + *ReaderConfig_->SuspiciousNodeGracePeriod < now) {
                if (auto reader = Reader_.Lock()) {
                    // NB(akozhikhov): In order not to call DiscardSeeds too often, we implement additional delay.
                    YT_LOG_WARNING("Discarding seeds due to node being suspicious (NodeAddress: %v, SuspicionTime: %v)",
                        peer.Address,
                        now - *peer.NodeSuspicionMarkTime);
                    reader->DiscardSeeds(allyReplicasFuture);
                }
            }
        }

        return {
            std::move(peer),
            TErrorOrPeerProbeResult(TError(rspOrError))
        };
    }

    TFuture<std::pair<TPeer, TErrorOrPeerProbeResult>> ProbePeer(
        const IChannelPtr& channel,
        const std::string& address,
        const TPeer& peer,
        const std::vector<int>& blockIndexes)
    {
        auto probeBlockSetResponseFuture = RequestBatcher_->ProbeBlockSet(
            IRequestBatcher::TRequest{
                .Address = address,
                .Channel = channel,
                .BlockIndexes = blockIndexes,
                .Session = MakeStrong(this),
            });

        if (peer.NodeSuspicionMarkTime) {
            return probeBlockSetResponseFuture.Apply(BIND(
                [=, this, this_ = MakeStrong(this), seedsFuture = SeedsFuture_, totalPeerCount = std::ssize(Peers_)]
                (const TDataNodeServiceProxy::TErrorOrRspProbeBlockSetPtr& rspOrError)
                {
                    return ParseSuspiciousPeerAndProbeResponse(
                        std::move(peer),
                        rspOrError,
                        seedsFuture,
                        totalPeerCount);
                })
                .AsyncVia(SessionInvoker_));
        } else {
            return probeBlockSetResponseFuture.Apply(BIND([=] (const TDataNodeServiceProxy::TErrorOrRspProbeBlockSetPtr& rspOrError) {
                return ParsePeerAndProbeResponse(std::move(peer), rspOrError);
            }));
        }
    }

    TFuture<std::vector<std::pair<TPeer, TErrorOrPeerProbeResult>>> DoProbeAndSelectBestPeers(
        const TPeerList& candidates,
        const std::vector<int>& blockIndexes)
    {
        // Multiple candidates - send probing requests.
        std::vector<TFuture<std::pair<TPeer, TErrorOrPeerProbeResult>>> asyncResults;
        std::vector<TFuture<std::pair<TPeer, TErrorOrPeerProbeResult>>> asyncSuspiciousResults;
        asyncResults.reserve(candidates.size());
        for (const auto& peer : candidates) {
            auto channel = GetChannel(peer.Address);
            if (!channel) {
                continue;
            }

            if (peer.NodeSuspicionMarkTime) {
                asyncSuspiciousResults.push_back(ProbePeer(
                    channel,
                    peer.Address,
                    peer,
                    blockIndexes));
            } else {
                asyncResults.push_back(ProbePeer(
                    channel,
                    peer.Address,
                    peer,
                    blockIndexes));
            }
        }

        YT_LOG_DEBUG("Gathered candidate peers for probing (Addresses: %v, SuspiciousNodeCount: %v)",
            candidates,
            asyncSuspiciousResults.size());

        if (asyncSuspiciousResults.empty()) {
            return AllSucceeded(std::move(asyncResults));
        } else if (asyncResults.empty()) {
            return AllSucceeded(std::move(asyncSuspiciousResults));
        } else {
            return AllSucceeded(std::move(asyncResults))
                .Apply(BIND(
                [
                    this,
                    this_ = MakeStrong(this),
                    asyncSuspiciousResults = std::move(asyncSuspiciousResults)
                ] (const TErrorOr<std::vector<std::pair<TPeer, TErrorOrPeerProbeResult>>>& resultsOrError) {
                    YT_VERIFY(resultsOrError.IsOK());
                    auto results = resultsOrError.Value();
                    int totalCandidateCount = results.size() + asyncSuspiciousResults.size();

                    for (const auto& asyncSuspiciousResult : asyncSuspiciousResults) {
                        auto maybeSuspiciousResult = asyncSuspiciousResult.TryGet();
                        if (!maybeSuspiciousResult) {
                            continue;
                        }

                        YT_VERIFY(maybeSuspiciousResult->IsOK());

                        auto suspiciousResultValue = std::move(maybeSuspiciousResult->Value());
                        if (suspiciousResultValue.second.IsOK()) {
                            results.push_back(std::move(suspiciousResultValue));
                        } else {
                            ProcessError(
                                suspiciousResultValue.second,
                                suspiciousResultValue.first,
                                TError(
                                    NChunkClient::EErrorCode::NodeProbeFailed,
                                    "Error probing suspicious node %v",
                                    suspiciousResultValue.first.Address));
                        }
                    }

                    auto omittedSuspiciousNodeCount = totalCandidateCount - results.size();
                    if (omittedSuspiciousNodeCount > 0) {
                        SessionOptions_.ChunkReaderStatistics->OmittedSuspiciousNodeCount.fetch_add(
                            omittedSuspiciousNodeCount,
                            std::memory_order::relaxed);
                    }

                    return results;
                })
                .AsyncVia(SessionInvoker_));
        }
    }

    TPeerList OnPeersProbed(
        std::vector<std::pair<TPeer, TErrorOrPeerProbeResult>> peerAndProbeResults,
        int count,
        const std::vector<int>& blockIndexes)
    {
        std::vector<std::pair<TPeer, TPeerProbeResult>> peerAndSuccessfulProbeResults;
        bool receivedNewPeers = false;
        for (auto& [peer, probeResultOrError] : peerAndProbeResults) {
            if (!probeResultOrError.IsOK()) {
                ProcessError(
                    probeResultOrError,
                    peer,
                    TError(
                        NChunkClient::EErrorCode::NodeProbeFailed,
                        "Error probing node %v queue length",
                        peer.Address));
                continue;
            }

            auto& probeResult = probeResultOrError.Value();

            if (UpdatePeerBlockMap(peer, probeResult.PeerDescriptors, probeResult.AllyReplicas)) {
                receivedNewPeers = true;
            }

            // Exclude throttling peers from current pass.
            if (probeResult.NetThrottling || probeResult.DiskThrottling) {
                YT_LOG_DEBUG("Peer is throttling (Address: %v, NetThrottling: %v, DiskThrottling: %v)",
                    peer.Address,
                    probeResult.NetThrottling,
                    probeResult.DiskThrottling);
                continue;
            }

            if (!probeResult.HasCompleteChunk && peer.Type == EPeerType::Seed) {
                YT_LOG_DEBUG("Peer has no complete chunk (Address: %v)",
                    peer.Address);
                BanPeer(peer.Address, /*forever*/ false);
                continue;
            }

            peerAndSuccessfulProbeResults.emplace_back(std::move(peer), std::move(probeResult));
        }

        if (peerAndSuccessfulProbeResults.empty()) {
            YT_LOG_DEBUG("All peer candidates were discarded");
            return {};
        }

        if (receivedNewPeers) {
            YT_LOG_DEBUG("P2P was activated");
            for (const auto& [peer, probeResult] : peerAndSuccessfulProbeResults) {
                ReinstallPeer(peer.Address);
            }
            return {};
        }

        THashSet<int> requestedBlocksSet(blockIndexes.begin(), blockIndexes.end());
        auto netQueueSizeFactor = ReaderConfig_->NetQueueSizeFactor;
        auto diskQueueSizeFactor = ReaderConfig_->DiskQueueSizeFactor;
        auto cachedBlockCountFactor = ReaderConfig_->CachedBlockCountFactor;
        auto cachedBlockSizeFactor = ReaderConfig_->CachedBlockSizeFactor;

        auto calculateCacheHits = [&] (const auto& cachedBlocks) -> std::pair<i32, i64> {
            i32 cachedBlockCount = 0;
            i64 cachedBlockSize = 0;

            for (const auto& cachedBlock : cachedBlocks) {
                if (requestedBlocksSet.contains(cachedBlock.BlockIndex)) {
                    cachedBlockCount += 1;
                    cachedBlockSize += cachedBlock.BlockSize;
                }
            }

            return {cachedBlockCount, cachedBlockSize};
        };

        for (auto& [peer, probeResult] : peerAndSuccessfulProbeResults) {
            auto cacheHits = calculateCacheHits(probeResult.CachedBlocks);
            probeResult.CachedBlockCount = cacheHits.first;
            probeResult.CachedBlockSize = cacheHits.second;
        }

        std::sort(
            peerAndSuccessfulProbeResults.begin(),
            peerAndSuccessfulProbeResults.end(),
            [&] (const auto& first, const auto& second) {
                const auto& [firstPeer, firstProbeResult] = first;
                const auto& [secondPeer, secondProbeResult] = second;

                auto firstNetQueueSize = firstProbeResult.NetQueueSize;
                auto secondNetQueueSize = secondProbeResult.NetQueueSize;

                auto firstDiskQueueSize = firstProbeResult.DiskQueueSize;
                auto secondDiskQueueSize = secondProbeResult.DiskQueueSize;

                auto netQueueSize = firstNetQueueSize + secondNetQueueSize;
                auto diskQueueSize = firstDiskQueueSize + secondDiskQueueSize;

                double firstHit = 0;
                double secondHit = 0;

                if (netQueueSize) {
                    firstHit += (netQueueSizeFactor * firstNetQueueSize) / netQueueSize;
                    secondHit += (netQueueSizeFactor * secondNetQueueSize) / netQueueSize;
                }

                if (diskQueueSize) {
                    firstHit += (diskQueueSizeFactor * firstDiskQueueSize) / diskQueueSize;
                    secondHit += (diskQueueSizeFactor * secondDiskQueueSize) / diskQueueSize;
                }

                auto firstCachedBlockCount = firstProbeResult.CachedBlockCount;
                auto firstCachedBlockSize = firstProbeResult.CachedBlockSize;

                auto secondCachedBlockCount = secondProbeResult.CachedBlockCount;
                auto secondCachedBlockSize = secondProbeResult.CachedBlockSize;

                auto cachedBlockCount = firstCachedBlockCount + secondCachedBlockCount;
                auto cachedBlockSize = firstCachedBlockSize + secondCachedBlockSize;

                if (cachedBlockSize != 0 && cachedBlockCount != 0) {
                    auto firstBlockCountHit = (cachedBlockCountFactor * firstCachedBlockCount) / cachedBlockCount;
                    auto secondBlockCountHit = (cachedBlockCountFactor * secondCachedBlockCount) / cachedBlockCount;

                    auto firstBlockSizeHit = (cachedBlockSizeFactor * firstCachedBlockSize) / cachedBlockSize;
                    auto secondBlockSizeHit = (cachedBlockSizeFactor * secondCachedBlockSize) / cachedBlockSize;

                    firstHit -= firstBlockCountHit + firstBlockSizeHit;
                    secondHit -= secondBlockCountHit + secondBlockSizeHit;
                }

                return firstHit < secondHit;
            });

        count = std::min<int>(count, std::ssize(peerAndSuccessfulProbeResults));
        TPeerList bestPeers;
        for (int index = 0; index < std::ssize(peerAndSuccessfulProbeResults); ++index) {
            const auto& [peer, probeResult] = peerAndSuccessfulProbeResults[index];
            if (index < count) {
                bestPeers.push_back(peer);
            } else {
                ReinstallPeer(peer.Address);
            }
        }

        YT_LOG_DEBUG("Best peers selected (Peers: %v)",
            MakeFormattableView(
                TRange(peerAndSuccessfulProbeResults.begin(), peerAndSuccessfulProbeResults.begin() + count),
                [] (auto* builder, const auto& peerAndSuccessfulProbeResult) {
                    const auto& [peer, probeResult] = peerAndSuccessfulProbeResult;
                    builder->AppendFormat("{Address: %v, DiskQueueSize: %v, NetQueueSize: %v, CachedBlockCount: %v, CachedBlockSize: %v}",
                        peer.Address,
                        probeResult.DiskQueueSize,
                        probeResult.NetQueueSize,
                        probeResult.CachedBlockCount,
                        probeResult.CachedBlockSize);
                }));

        return bestPeers;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadBlockSetSession
    : public TSessionBase
{
public:
    TReadBlockSetSession(
        TReplicationReader* reader,
        const IChunkReader::TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        IThroughputThrottlerPtr mediumThrottler)
        : TSessionBase(
            reader,
            options.ClientOptions,
            std::move(options.DisableBandwidthThrottler
                ? GetUnlimitedThrottler()
                : std::move(bandwidthThrottler)),
            std::move(rpsThrottler),
            std::move(mediumThrottler),
            options.SessionInvoker)
        , BlockIndexes_(blockIndexes)
        , EstimatedSize_(options.EstimatedSize)
    {
        YT_LOG_DEBUG("Will read block set (Blocks: %v)",
            MakeCompactIntervalView(blockIndexes));
    }

    ~TReadBlockSetSession()
    {
        Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Reader destroyed"));
    }

    TFuture<std::vector<TBlock>> Run()
    {
        if (BlockIndexes_.empty()) {
            return MakeFuture(std::vector<TBlock>());
        }
        Promise_.OnCanceled(BIND(&TReadBlockSetSession::OnCanceled, MakeWeak(this)));
        StartTime_ = TInstant::Now();
        NextRetry();

        return Promise_.ToFuture()
            .Apply(BIND([
                startTime = StartTime_,
                readerStatistics = SessionOptions_.ChunkReaderStatistics
            ] (const TErrorOr<std::vector<TBlock>>& result) {
                readerStatistics->RecordDataWaitTime(TInstant::Now() - startTime);
                return result.ValueOrThrow();
            }));
    }

private:
    friend class TRequestBatcher;

    //! Block indexes to read during the session.
    const std::vector<int> BlockIndexes_;
    const std::optional<i64> EstimatedSize_;

    //! Promise representing the session.
    const TPromise<std::vector<TBlock>> Promise_ = NewPromise<std::vector<TBlock>>();

    i64 BytesThrottled_ = 0;

    //! Blocks that are fetched so far.
    THashMap<int, TBlock> Blocks_;

    //! Maps peer addresses to block indexes.
    THashMap<std::string, THashSet<int>> PeerBlocksMap_;

    //! address -> block_index -> (session_id, iteration).
    THashMap<TNodeId, THashMap<int, NChunkClient::NProto::TP2PBarrier>> P2PDeliveryBarrier_;

    struct TBlockWithCookie
    {
        int BlockIndex;
        std::unique_ptr<ICachedBlockCookie> Cookie;

        TBlockWithCookie(int blockIndex, std::unique_ptr<ICachedBlockCookie> cookie)
            : BlockIndex(blockIndex)
            , Cookie(std::move(cookie))
        { }
    };


    void NextPass() override
    {
        if (!PrepareNextPass()) {
            return;
        }

        PeerBlocksMap_.clear();
        P2PDeliveryBarrier_.clear();
        auto blockIndexes = GetUnfetchedBlockIndexes();
        for (const auto& [address, peer] : Peers_) {
            PeerBlocksMap_[address] = THashSet<int>(blockIndexes.begin(), blockIndexes.end());
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

    void RequestBlocks()
    {
        SessionInvoker_->Invoke(
            BIND(&TReadBlockSetSession::DoRequestBlocks, MakeStrong(this)));
    }

    bool UpdatePeerBlockMap(
        const TPeer& suggestorPeer,
        const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TPeerDescriptor>& peerDescriptors,
        const TAllyReplicasInfo& allyReplicas) override
    {
        if (!ReaderConfig_->FetchFromPeers && !peerDescriptors.empty()) {
            YT_LOG_DEBUG("Peer suggestions received but ignored (SuggestorAddress: %v)",
                suggestorPeer.Address);
            return false;
        }

        bool addedNewPeers = false;
        for (const auto& peerDescriptor : peerDescriptors) {
            int blockIndex = peerDescriptor.block_index();
            for (auto protoPeerNodeId : peerDescriptor.node_ids()) {
                auto peerNodeId = NNodeTrackerClient::TNodeId(protoPeerNodeId);
                auto maybeSuggestedDescriptor = NodeDirectory_->FindDescriptor(peerNodeId);
                if (!maybeSuggestedDescriptor) {
                    YT_LOG_DEBUG("Cannot resolve peer descriptor (SuggestedNodeId: %v, SuggestorAddress: %v)",
                        peerNodeId,
                        suggestorPeer.Address);
                    continue;
                }

                if (auto suggestedAddress = maybeSuggestedDescriptor->FindAddress(Networks_)) {
                    if (AddPeer(
                        TChunkReplicaWithMedium(peerNodeId, GenericChunkReplicaIndex, suggestorPeer.Replica.GetMediumIndex()),
                        *suggestedAddress,
                        *maybeSuggestedDescriptor,
                        EPeerType::Peer,
                        /*nodeSuspicionMarkTime*/ std::nullopt))
                    {
                        addedNewPeers = true;
                    }

                    PeerBlocksMap_[*suggestedAddress].insert(blockIndex);
                    YT_LOG_DEBUG("Block peer descriptor received (Block: %v, SuggestedAddress: %v, SuggestorAddress: %v)",
                        blockIndex,
                        *suggestedAddress,
                        suggestorPeer.Address);

                    if (peerDescriptor.has_delivery_barier()) {
                        P2PDeliveryBarrier_[peerNodeId].emplace(
                            blockIndex,
                            peerDescriptor.delivery_barier());
                    }
                } else {
                    YT_LOG_WARNING("Peer suggestion ignored, required network is missing "
                        "(SuggestedAddress: %v, Networks: %v, SuggestorAddress: %v)",
                        maybeSuggestedDescriptor->GetDefaultAddress(),
                        Networks_,
                        suggestorPeer.Address);
                }
            }
        }

        if (allyReplicas) {
            MaybeUpdateSeeds(allyReplicas);
        }

        if (addedNewPeers) {
            SessionOptions_.ChunkReaderStatistics->P2PActivationCount.fetch_add(1, std::memory_order::relaxed);
        }

        return addedNewPeers;
    }

    void DoRequestBlocks()
    {
        auto reader = Reader_.Lock();
        if (!reader || IsCanceled()) {
            return;
        }

        auto blockIndexes = GetUnfetchedBlockIndexes();
        if (blockIndexes.empty()) {
            OnSessionSucceeded();
            return;
        }
        std::sort(blockIndexes.begin(), blockIndexes.end());

        std::vector<TBlockWithCookie> cachedBlocks;
        std::vector<TBlockWithCookie> uncachedBlocks;

        const auto& blockCache = reader->BlockCache_;
        for (int blockIndex : blockIndexes) {
            TBlockId blockId(ChunkId_, blockIndex);
            const auto& readerConfig = reader->Config_;
            if (readerConfig->UseBlockCache) {
                if (reader->Config_->UseAsyncBlockCache) {
                    auto cookie = blockCache->GetBlockCookie(blockId, EBlockType::CompressedData);
                    if (cookie->IsActive()) {
                        uncachedBlocks.push_back(TBlockWithCookie(blockIndex, std::move(cookie)));
                    } else {
                        cachedBlocks.push_back(TBlockWithCookie(blockIndex, std::move(cookie)));
                    }
                } else {
                    if (auto block = blockCache->FindBlock(blockId, EBlockType::CompressedData)) {
                        cachedBlocks.push_back(TBlockWithCookie(blockIndex, CreatePresetCachedBlockCookie(block)));
                    } else {
                        uncachedBlocks.push_back(TBlockWithCookie(blockIndex, /*cookie*/ nullptr));
                    }
                }
            } else {
                uncachedBlocks.push_back(TBlockWithCookie(blockIndex, /*cookie*/ nullptr));
            }
        }

        bool requestMoreBlocks = true;
        if (!uncachedBlocks.empty()) {
            auto candidates = FindCandidates(reader, uncachedBlocks);
            if (candidates.empty()) {
                OnPassCompleted();
                for (const auto& block : uncachedBlocks) {
                    if (const auto& cookie = block.Cookie) {
                        cookie->SetBlock(TError("No peer candidates were found"));
                    }
                }
                return;
            }
            requestMoreBlocks = FetchBlocksFromNodes(reader, uncachedBlocks, candidates);
        }

        FetchBlocksFromCache(cachedBlocks);

        if (requestMoreBlocks) {
            RequestBlocks();
        }
    }

    TPeerList FindCandidates(
        const TReplicationReaderPtr& reader,
        const std::vector<TBlockWithCookie>& blocksToFetch)
    {
        TPeerList candidates;

        int desiredPeerCount = GetDesiredPeerCount();
        while (std::ssize(candidates) < desiredPeerCount) {
            auto hasUnfetchedBlocks = [&] (const std::string& address) {
                const auto& peerBlockIndexes = GetOrCrash(PeerBlocksMap_, address);

                for (const auto& block : blocksToFetch) {
                    int blockIndex = block.BlockIndex;
                    if (peerBlockIndexes.contains(blockIndex)) {
                        return true;
                    }
                }

                return false;
            };

            auto moreCandidates = PickPeerCandidates(
                reader,
                ReaderConfig_->ProbePeerCount,
                /*enableEarlyExit*/ !SessionOptions_.HedgingManager && !ReaderConfig_->BlockRpcHedgingDelay,
                hasUnfetchedBlocks);

            if (moreCandidates.empty()) {
                break;
            }
            candidates.insert(candidates.end(), moreCandidates.begin(), moreCandidates.end());
        }

        return candidates;
    }

    //! Fetches blocks from nodes and adds them to block cache via cookies.
    //! Blocks must be sorted by block index.
    //! Returns |True| if more blocks can be requested and |False| otherwise.
    bool FetchBlocksFromNodes(
        const TReplicationReaderPtr& reader,
        const std::vector<TBlockWithCookie>& blocks,
        const TPeerList& candidates)
    {
        auto cancelAll = [&] (const TError& error) {
            for (const auto& block : blocks) {
                // NB: Setting cookie twice is OK. Only the first value
                // will be used.
                if (const auto& cookie = block.Cookie) {
                    cookie->SetBlock(error);
                }
            }
        };

        std::vector<int> blockIndexes;
        blockIndexes.reserve(blocks.size());
        for (const auto& block : blocks) {
            blockIndexes.push_back(block.BlockIndex);
        }

        NProfiling::TWallTimer pickPeerTimer;

        // One extra request for actually getting blocks.
        // Hedging requests are disregarded.
        if (!SyncThrottle(RpsThrottler_, 1 + candidates.size())) {
            cancelAll(TError(
                NChunkClient::EErrorCode::ReaderThrottlingFailed,
                "Failed to apply throttling in reader"));
            return false;
        }

        auto peers = ProbeAndSelectBestPeers(
            candidates,
            GetDesiredPeerCount(),
            blockIndexes);

        SessionOptions_.ChunkReaderStatistics->PickPeerWaitTime.fetch_add(
            pickPeerTimer.GetElapsedValue(),
            std::memory_order::relaxed);

        IHedgingManagerPtr hedgingManager;
        if (SessionOptions_.HedgingManager) {
            hedgingManager = SessionOptions_.HedgingManager;
        } else if (ReaderConfig_->BlockRpcHedgingDelay) {
            hedgingManager = CreateSimpleHedgingManager(*ReaderConfig_->BlockRpcHedgingDelay);
        }

        std::optional<THedgingChannelOptions> hedgingOptions;
        if (hedgingManager) {
            hedgingOptions = THedgingChannelOptions{
                .HedgingManager = std::move(hedgingManager),
                .CancelPrimaryOnHedging = ReaderConfig_->CancelPrimaryBlockRpcRequestOnHedging,
            };
        }

        auto channel = MakePeersChannel(
            peers,
            hedgingOptions);
        if (!channel) {
            cancelAll(TError("No peers were selected"));
            return true;
        }

        const auto& primaryAddress = peers[0].Address;
        if (ShouldThrottle(primaryAddress, BytesThrottled_ == 0 && EstimatedSize_)) {
            // NB(psushin): This is preliminary throttling. The subsequent request may fail or return partial result.
            // In order not to throttle twice, we check BytesThrottled_ is zero.
            // Still it protects us from bursty incoming traffic on the host.
            // If estimated size was not given, we fallback to post-throttling on actual received size.
            YT_VERIFY(blockIndexes.size() <= BlockIndexes_.size());
            // NB: Some blocks are read from cache hence we need to further estimate throttling amount.
            auto requestedBlocksEstimatedSize = *EstimatedSize_ * std::ssize(blockIndexes) / std::ssize(BlockIndexes_);
            BytesThrottled_ = requestedBlocksEstimatedSize;
            if (!SyncThrottle(CombinedDataByteThrottler_, requestedBlocksEstimatedSize)) {
                cancelAll(TError(
                    NChunkClient::EErrorCode::ReaderThrottlingFailed,
                    "Failed to apply throttling in reader"));
                return false;
            }
        }

        auto getBlockSetResponseFuture = RequestBatcher_->GetBlockSet(
            IRequestBatcher::TRequest{
                .Address = primaryAddress,
                .Channel = channel,
                .BlockIndexes = blockIndexes,
                .Session = MakeStrong(this),
                .Barriers = FillP2PBarriers(peers, blockIndexes),
                .Peers = peers,
            },
            // If hedging is enabled, then get block batching is not allowed.
            hedgingOptions.has_value());

        SetSessionFuture(getBlockSetResponseFuture.As<void>());
        auto result = WaitFor(getBlockSetResponseFuture);

        auto rspOrError = result.IsOK()
            ? result.Value().Response
            : TErrorOr<TDataNodeServiceProxy::TRspGetBlockSetPtr>(result.Wrap());

        bool backup = IsBackup(rspOrError);
        const auto& respondedPeer = backup ? peers[1] : peers[0];

        if (!rspOrError.IsOK()) {
            auto wrappingError = TError(
                "Error fetching blocks from node %v",
                respondedPeer.Address);
            ProcessError(
                rspOrError,
                respondedPeer,
                wrappingError);
            cancelAll(wrappingError << rspOrError);
            return true;
        }

        const auto& rsp = rspOrError.Value();

        if (backup) {
            BanPeer(peers[0].Address, false);
        }

        auto netThrottling = rsp->net_throttling();
        auto diskThrottling = rsp->disk_throttling();

        UpdatePeerBlockMap(
            respondedPeer,
            rsp->peer_descriptors(),
            FromProto<TAllyReplicasInfo>(rsp->ally_replicas()));

        if (netThrottling || diskThrottling) {
            YT_LOG_DEBUG("Peer is throttling (Address: %v, NetThrottling: %v, DiskThrottling: %v)",
                respondedPeer.Address,
                netThrottling,
                diskThrottling);
        }

        i64 bytesReceived = 0;
        int invalidBlockCount = 0;
        std::vector<int> receivedBlockIndexes;

        auto responseBlocks = GetRpcAttachedBlocks(rsp, /*validateChecksums*/ false);

        auto& requestedBlockIndexes = result.Value().RequestedBlocks;
        auto requestedBlockIndexIt = requestedBlockIndexes.begin();
        auto responseBlockIt = responseBlocks.begin();

        // Two pointer algorithm requires block indexes to be sorted.
        YT_VERIFY(std::is_sorted(blockIndexes.begin(), blockIndexes.end()));
        YT_VERIFY(std::is_sorted(requestedBlockIndexes.begin(), requestedBlockIndexes.end()));

        for (int index = 0; index < std::ssize(blockIndexes); ++index) {
            int blockIndex = blockIndexes[index];
            auto blockId = TBlockId(ChunkId_, blockIndex);

            while(requestedBlockIndexIt != requestedBlockIndexes.end() &&
                *requestedBlockIndexIt != blockIndex &&
                responseBlockIt != responseBlocks.end())
            {
                ++requestedBlockIndexIt;
                ++responseBlockIt;
            }

            if (responseBlockIt == responseBlocks.end()) {
                break;
            }

            auto& block = *responseBlockIt;
            if (!block) {
                continue;
            }

            block.Data = TrackMemory(SessionOptions_.MemoryUsageTracker, std::move(block.Data));

            if (auto error = block.ValidateChecksum(); !error.IsOK()) {
                RegisterError(
                    TError(
                        NChunkClient::EErrorCode::BlockChecksumMismatch,
                        "Failed to validate received block checksum")
                        << TErrorAttribute("block_id", ToString(blockId))
                        << TErrorAttribute("peer", respondedPeer.Address)
                        << error,
                    /*raiseAlert*/ true);

                ++invalidBlockCount;
                continue;
            }

            auto sourceDescriptor = ReaderOptions_->EnableP2P
                ? std::optional<TNodeDescriptor>(GetPeerDescriptor(respondedPeer.Address))
                : std::optional<TNodeDescriptor>(std::nullopt);

            if (auto& cookie = blocks[index].Cookie) {
                cookie->SetBlock(block);
            } else if (reader->Config_->UseBlockCache) {
                reader->BlockCache_->PutBlock(blockId, EBlockType::CompressedData, block);
            }

            bytesReceived += block.Size();
            TotalBytesReceived_ += block.Size();
            receivedBlockIndexes.push_back(blockIndex);
            EmplaceOrCrash(Blocks_, blockIndex, std::move(block));
        }

        if (invalidBlockCount > 0) {
            BanPeer(respondedPeer.Address, false);
        }

        BanSeedIfIncomplete(rsp, respondedPeer.Address);

        if (bytesReceived > 0) {
            // Reinstall peer into peer queue, if some data was received.
            ReinstallPeer(respondedPeer.Address);
        }

        YT_LOG_DEBUG("Finished processing block response "
            "(Address: %v, PeerType: %v, BlocksReceived: %v, BytesReceived: %v, PeersSuggested: %v, InvalidBlockCount: %v)",
            respondedPeer.Address,
            respondedPeer.Type,
            MakeCompactIntervalView(receivedBlockIndexes),
            bytesReceived,
            rsp->peer_descriptors_size(),
            invalidBlockCount);

        if (ShouldThrottle(respondedPeer.Address, TotalBytesReceived_ > BytesThrottled_)) {
            auto delta = TotalBytesReceived_ - BytesThrottled_;
            BytesThrottled_ = TotalBytesReceived_;
            if (!SyncThrottle(CombinedDataByteThrottler_, delta)) {
                cancelAll(TError(
                    NChunkClient::EErrorCode::ReaderThrottlingFailed,
                    "Failed to apply throttling in reader"));
                return false;
            }
        }

        cancelAll(TError("Block was not sent by node"));
        return true;
    }

    std::vector<NProto::TP2PBarrier> FillP2PBarriers(
        const TPeerList& peerList,
        const std::vector<int>& blockIndexes)
    {
        // We need a way of sending two separate requests to the two different nodes.
        // And there is no way of doing this with hedging channel.
        // So we send all barriers to both nodes, and filter them in data node service.
        std::vector<NProto::TP2PBarrier> barriers;

        for (const auto& peer : peerList) {
            auto blockBarriers = P2PDeliveryBarrier_.find(peer.Replica.GetNodeId());
            if (blockBarriers == P2PDeliveryBarrier_.end()) {
                continue;
            }

            THashMap<TGuid, i64> maxBarrier;
            for (int blockIndex : blockIndexes) {
                auto it = blockBarriers->second.find(blockIndex);
                if (it == blockBarriers->second.end()) {
                    continue;
                }

                auto sessionId = FromProto<TGuid>(it->second.session_id());
                maxBarrier[sessionId] = std::max(maxBarrier[sessionId], it->second.iteration());
            }

            for (const auto& [sessionId, iteration] : maxBarrier) {
                NProto::TP2PBarrier waitBarrier;

                ToProto(waitBarrier.mutable_session_id(), sessionId);
                waitBarrier.set_iteration(iteration);
                waitBarrier.set_if_node_id(ToProto(peer.Replica.GetNodeId()));

                barriers.push_back(waitBarrier);
            }
        }

        return barriers;
    }

    void FetchBlocksFromCache(const std::vector<TBlockWithCookie>& blocks)
    {
        std::vector<TFuture<void>> cachedBlockFutures;
        cachedBlockFutures.reserve(blocks.size());
        for (const auto& block : blocks) {
            cachedBlockFutures.push_back(block.Cookie->GetBlockFuture());
        }
        WaitForFast(AllSet(cachedBlockFutures))
            .ValueOrThrow();

        std::vector<int> fetchedBlockIndexes;
        for (int index = 0; index < std::ssize(blocks); ++index) {
            if (!cachedBlockFutures[index].Get().IsOK()) {
                continue;
            }

            auto it = EmplaceOrCrash(Blocks_, blocks[index].BlockIndex, blocks[index].Cookie->GetBlock());
            fetchedBlockIndexes.push_back(it->first);
            SessionOptions_.ChunkReaderStatistics->DataBytesReadFromCache.fetch_add(
                it->second.Size(),
                std::memory_order::relaxed);
        }

        YT_LOG_DEBUG("Fetched blocks from block cache (Count: %v, BlockIndexes: %v)",
            fetchedBlockIndexes.size(),
            MakeCompactIntervalView(fetchedBlockIndexes));
    }

    void OnSessionSucceeded()
    {
        std::vector<TBlock> blocks;
        blocks.reserve(BlockIndexes_.size());
        for (int blockIndex : BlockIndexes_) {
            const auto& block = Blocks_[blockIndex];
            YT_VERIFY(block.Data);
            blocks.push_back(block);
        }

        YT_LOG_DEBUG("All requested blocks are fetched "
            "(BlockCount: %v, TotalSize: %v, ThrottledSize: %v, EstimatedSize: %v)",
            blocks.size(),
            GetByteSize(blocks),
            BytesThrottled_,
            EstimatedSize_);

        AccountExtraMediumBandwidth(BytesThrottled_);

        Promise_.TrySet(std::vector<TBlock>(blocks));
    }

    void OnSessionFailed(bool fatal) override
    {
        auto error = BuildCombinedError(TError(
            NChunkClient::EErrorCode::ChunkBlockFetchFailed,
            "Error fetching blocks for chunk %v",
            ChunkId_));
        OnSessionFailed(fatal, error);
    }

    void OnSessionFailed(bool fatal, const TError& error) override
    {
        YT_LOG_DEBUG(error, "Reader session failed (Fatal: %v)", fatal);

        if (fatal) {
            SetReaderFailed();
        }

        ReleaseThrottledBytesExcess(BandwidthThrottler_, BytesThrottled_);

        Promise_.TrySet(error);
    }

    int GetDesiredPeerCount() const
    {
        return (SessionOptions_.HedgingManager || ReaderConfig_->BlockRpcHedgingDelay) ? 2 : 1;
    }

    void OnCanceled(const TError& error) override
    {
        auto wrappedError = TError(NYT::EErrorCode::Canceled, "ReadBlockSet session canceled")
            << error;
        TSessionBase::OnCanceled(wrappedError);
        Promise_.TrySet(wrappedError);
    }
};

TFuture<std::vector<TBlock>> TReplicationReader::ReadBlocks(
    const TReadBlocksOptions& options,
    const std::vector<int>& blockIndexes)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    if (blockIndexes.empty()) {
        return MakeFuture<std::vector<TBlock>>({});
    }

    auto session = New<TReadBlockSetSession>(
        this,
        options,
        blockIndexes,
        BandwidthThrottler_,
        RpsThrottler_,
        MediumThrottler_);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TReadBlockRangeSession
    : public TSessionBase
{
public:
    TReadBlockRangeSession(
        TReplicationReader* reader,
        const IChunkReader::TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        IThroughputThrottlerPtr mediumThrottler)
        : TSessionBase(
            reader,
            options.ClientOptions,
            options.DisableBandwidthThrottler
                ? GetUnlimitedThrottler()
                : std::move(bandwidthThrottler),
            std::move(rpsThrottler),
            std::move(mediumThrottler))
        , FirstBlockIndex_(firstBlockIndex)
        , BlockCount_(blockCount)
        , EstimatedSize_(options.EstimatedSize)
    {
        YT_LOG_DEBUG("Will read block range (Blocks: %v)", FormatBlocks());
    }

    TFuture<std::vector<TBlock>> Run()
    {
        if (BlockCount_ == 0) {
            return MakeFuture(std::vector<TBlock>());
        }
        Promise_.OnCanceled(BIND(&TReadBlockRangeSession::OnCanceled, MakeWeak(this)));
        StartTime_ = TInstant::Now();
        NextRetry();

        return Promise_.ToFuture()
            .Apply(BIND([
                startTime = StartTime_,
                readerStatistics = SessionOptions_.ChunkReaderStatistics
            ] (const TErrorOr<std::vector<TBlock>>& result) {
                readerStatistics->RecordDataWaitTime(TInstant::Now() - startTime);
                return result.ValueOrThrow();
            }));
    }

private:
    const int FirstBlockIndex_;
    const int BlockCount_;

    const std::optional<i64> EstimatedSize_;

    //! Promise representing the session.
    const TPromise<std::vector<TBlock>> Promise_ = NewPromise<std::vector<TBlock>>();

    //! Blocks that are fetched so far.
    std::vector<TBlock> FetchedBlocks_;

    i64 BytesThrottled_ = 0;


    void NextPass() override
    {
        if (!PrepareNextPass()) {
            return;
        }

        RequestBlocks();
    }

    void RequestBlocks()
    {
        SessionInvoker_->Invoke(
            BIND(&TReadBlockRangeSession::DoRequestBlocks, MakeStrong(this)));
    }

    void DoRequestBlocks()
    {
        auto reader = Reader_.Lock();
        if (!reader || IsCanceled()) {
            return;
        }

        YT_VERIFY(FetchedBlocks_.empty());

        auto candidates = PickPeerCandidates(
            reader,
            /*count*/ 1,
            /*enableEarlyExit*/ true);
        if (candidates.empty()) {
            OnPassCompleted();
            return;
        }

        const auto& peer = candidates.front();
        const auto& peerAddress = peer.Address;
        auto channel = GetChannel(peerAddress);
        if (!channel) {
            RequestBlocks();
            return;
        }

        YT_LOG_DEBUG("Requesting blocks from peer (Address: %v, Blocks: %v, EstimatedSize: %v, BytesThrottled: %v)",
            peerAddress,
            FormatBlocks(),
            EstimatedSize_,
            BytesThrottled_);

        if (ShouldThrottle(peerAddress, BytesThrottled_ == 0 && EstimatedSize_)) {
            // NB(psushin): This is preliminary throttling. The subsequent request may fail or return partial result.
            // In order not to throttle twice, we check BytesThrottled_ is zero.
            // Still it protects us from bursty incoming traffic on the host.
            // If estimated size was not given, we fallback to post-throttling on actual received size.
            BytesThrottled_ = *EstimatedSize_;
            if (!SyncThrottle(CombinedDataByteThrottler_, *EstimatedSize_)) {
                return;
            }
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(ReaderConfig_->BlockRpcTimeout);

        auto req = proxy.GetBlockRange();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(SessionOptions_.MultiplexingBand);
        req->SetMultiplexingParallelism(SessionOptions_.MultiplexingParallelism);
        SetRequestWorkloadDescriptor(req, WorkloadDescriptor_);
        ToProto(req->mutable_chunk_id(), ChunkId_);
        req->set_first_block_index(FirstBlockIndex_);
        req->set_block_count(BlockCount_);

        auto rspFuture = req->Invoke();
        SetSessionFuture(rspFuture.As<void>());
        auto rspOrError = WaitFor(rspFuture);

        if (!rspOrError.IsOK()) {
            ProcessError(
                rspOrError,
                peer,
                TError(
                    "Error fetching blocks from node %v",
                    peerAddress));
            RequestBlocks();
            return;
        }

        const auto& rsp = rspOrError.Value();

        SessionOptions_.ChunkReaderStatistics->DataBytesTransmitted.fetch_add(
            rsp->GetTotalSize(),
            std::memory_order::relaxed);
        if (rsp->has_chunk_reader_statistics()) {
            HandleChunkReaderStatistics(rsp->chunk_reader_statistics());
        }

        auto blocks = GetRpcAttachedBlocks(rsp, /*validateChecksums*/ false);

        for (auto& block : blocks) {
            block.Data = TrackMemory(SessionOptions_.MemoryUsageTracker, std::move(block.Data));
        }

        int blocksReceived = 0;
        i64 bytesReceived = 0;

        for (const auto& block : blocks) {
            if (!block) {
                break;
            }

            if (auto error = block.ValidateChecksum(); !error.IsOK()) {
                RegisterError(
                    TError(
                        NChunkClient::EErrorCode::BlockChecksumMismatch,
                        "Failed to validate received block checksum")
                        << TErrorAttribute("block_id", ToString(TBlockId(ChunkId_, FirstBlockIndex_ + blocksReceived)))
                        << TErrorAttribute("peer", peerAddress)
                        << error,
                    /*raiseAlert*/ true);

                BanPeer(peerAddress, false);
                FetchedBlocks_.clear();
                RequestBlocks();
                return;
            }

            blocksReceived += 1;
            bytesReceived += block.Size();
            TotalBytesReceived_ += block.Size();

            FetchedBlocks_.push_back(std::move(block));
        }

        BanSeedIfIncomplete(rsp, peerAddress);

        if (rsp->net_throttling() || rsp->disk_throttling()) {
            YT_LOG_DEBUG("Peer is throttling (Address: %v)", peerAddress);
        } else {
            if (blocksReceived == 0) {
                YT_LOG_DEBUG("Peer has no relevant blocks (Address: %v)", peerAddress);
                BanPeer(peerAddress, false);
            } else {
                ReinstallPeer(peerAddress);
            }
        }

        YT_LOG_DEBUG("Finished processing block response (Address: %v, BlocksReceived: %v-%v, BytesReceived: %v)",
            peerAddress,
            FirstBlockIndex_,
            FirstBlockIndex_ + blocksReceived - 1,
            bytesReceived);

        if (ShouldThrottle(peerAddress, TotalBytesReceived_ > BytesThrottled_)) {
            auto delta = TotalBytesReceived_ - BytesThrottled_;
            BytesThrottled_ = TotalBytesReceived_;
            if (!SyncThrottle(CombinedDataByteThrottler_, delta)) {
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
        YT_LOG_DEBUG("Some blocks are fetched (Blocks: %v)", FormatBlocks());

        AccountExtraMediumBandwidth(BytesThrottled_);

        Promise_.TrySet(std::vector<TBlock>(FetchedBlocks_));
    }

    void OnSessionFailed(bool fatal) override
    {
        auto error = BuildCombinedError(TError(
            NChunkClient::EErrorCode::ChunkBlockFetchFailed,
            "Error fetching blocks for chunk %v",
            ChunkId_));
        OnSessionFailed(fatal, error);
    }

    void OnSessionFailed(bool fatal, const TError& error) override
    {
        YT_LOG_DEBUG(error, "Reader session failed (Fatal: %v)",
            fatal);

        if (fatal) {
            SetReaderFailed();
        }

        ReleaseThrottledBytesExcess(BandwidthThrottler_, BytesThrottled_);

        Promise_.TrySet(error);
    }

    void OnCanceled(const TError& error) override
    {
        auto wrappedError = TError(NYT::EErrorCode::Canceled, "ReadBlockRange session canceled")
            << error;
        TSessionBase::OnCanceled(wrappedError);
        Promise_.TrySet(wrappedError);
    }

    TString FormatBlocks() const
    {
        TStringBuilder builder;

        if (BlockCount_ == 1) {
            builder.AppendFormat("[%v]", FirstBlockIndex_);
        } else {
            builder.AppendFormat("[%v-%v]",
                FirstBlockIndex_,
                FirstBlockIndex_ + BlockCount_ - 1);
        }

        return builder.Flush();
    }
};

TFuture<std::vector<TBlock>> TReplicationReader::ReadBlocks(
    const TReadBlocksOptions& options,
    int firstBlockIndex,
    int blockCount)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    YT_VERIFY(blockCount >= 0);

    if (blockCount == 0) {
        return MakeFuture<std::vector<TBlock>>({});
    }

    auto session = New<TReadBlockRangeSession>(
        this,
        options,
        firstBlockIndex,
        blockCount,
        BandwidthThrottler_,
        RpsThrottler_,
        MediumThrottler_);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TGetMetaSession
    : public TSessionBase
{
public:
    TGetMetaSession(
        TReplicationReader* reader,
        const IChunkReader::TGetMetaOptions& options,
        const std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        IThroughputThrottlerPtr mediumThrottler)
        : TSessionBase(
            reader,
            options.ClientOptions,
            std::move(bandwidthThrottler),
            std::move(rpsThrottler),
            std::move(mediumThrottler),
            GetCurrentInvoker())
        , MetaSize_(options.MetaSize)
        , PartitionTag_(partitionTag)
        , ExtensionTags_(extensionTags)
    { }

    ~TGetMetaSession()
    {
        Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Reader destroyed"));
    }

    TFuture<TRefCountedChunkMetaPtr> Run()
    {
        StartTime_ = TInstant::Now();
        Promise_.OnCanceled(BIND(&TGetMetaSession::OnCanceled, MakeWeak(this)));
        NextRetry();

        return Promise_.ToFuture()
            .Apply(BIND([
                startTime = StartTime_,
                readerStatistics = SessionOptions_.ChunkReaderStatistics
            ] (const TErrorOr<TRefCountedChunkMetaPtr>& result) {
                readerStatistics->RecordMetaWaitTime(TInstant::Now() - startTime);
                return result.ValueOrThrow();
            }));
    }

private:
    const std::optional<i64> MetaSize_;
    const std::optional<int> PartitionTag_;
    const std::optional<std::vector<int>> ExtensionTags_;

    //! Promise representing the session.
    const TPromise<TRefCountedChunkMetaPtr> Promise_ = NewPromise<TRefCountedChunkMetaPtr>();

    i64 BytesThrottled_ = 0;


    void NextPass() override
    {
        if (!PrepareNextPass()) {
            return;
        }

        RequestMeta();
    }

    void RequestMeta()
    {
        // NB: Strong ref here is the only reference that keeps session alive.
        SessionInvoker_->Invoke(
            BIND(&TGetMetaSession::DoRequestMeta, MakeStrong(this)));
    }

    void DoRequestMeta()
    {
        auto reader = Reader_.Lock();
        if (!reader || IsCanceled()) {
            return;
        }

        // NB: This is solely for testing purposes.
        if (ReaderConfig_->ChunkMetaCacheFailureProbability &&
            RandomNumber<double>() < *ReaderConfig_->ChunkMetaCacheFailureProbability)
        {
            TError simulatedError("Simulated chunk meta fetch failure");
            YT_LOG_ERROR(simulatedError);
            Promise_.TrySet(simulatedError);
            return;
        }

        auto peers = PickPeerCandidates(
            reader,
            ReaderConfig_->MetaRpcHedgingDelay ? 2 : 1,
            /*enableEarlyExit*/ false);
        if (peers.empty()) {
            OnPassCompleted();
            return;
        }

        std::optional<THedgingChannelOptions> hedgingOptions;
        if (ReaderConfig_->MetaRpcHedgingDelay) {
            hedgingOptions = THedgingChannelOptions{
                .HedgingManager = CreateSimpleHedgingManager(*ReaderConfig_->MetaRpcHedgingDelay),
                .CancelPrimaryOnHedging = false,
            };
        }
        auto channel = MakePeersChannel(peers, hedgingOptions);
        if (!channel) {
            RequestMeta();
            return;
        }

        YT_LOG_DEBUG("Requesting chunk meta (Addresses: %v)", peers);

        if (ShouldThrottle(peers[0].Address, BytesThrottled_ == 0 && MetaSize_ > 0)) {
            BytesThrottled_ = *MetaSize_;
            if (!CombinedDataByteThrottler_->IsOverdraft()) {
                CombinedDataByteThrottler_->Acquire(BytesThrottled_);
            } else if (!SyncThrottle(CombinedDataByteThrottler_, BytesThrottled_)) {
                return;
            }
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(ReaderConfig_->MetaRpcTimeout);

        auto req = proxy.GetChunkMeta();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(SessionOptions_.MultiplexingBand);
        req->SetMultiplexingParallelism(SessionOptions_.MultiplexingParallelism);
        SetRequestWorkloadDescriptor(req, WorkloadDescriptor_);
        req->set_enable_throttling(true);
        ToProto(req->mutable_chunk_id(), ChunkId_);
        req->set_all_extension_tags(!ExtensionTags_);
        if (PartitionTag_) {
            req->set_partition_tag(*PartitionTag_);
        }
        if (ExtensionTags_) {
            ToProto(req->mutable_extension_tags(), *ExtensionTags_);
        }
        req->set_supported_chunk_features(ToUnderlying(GetSupportedChunkFeatures()));

        auto rspFuture = req->Invoke();
        SetSessionFuture(rspFuture.As<void>());
        auto rspOrError = WaitFor(rspFuture);

        bool backup = IsBackup(rspOrError);
        const auto& respondedPeer = backup ? peers[1] : peers[0];

        if (!rspOrError.IsOK()) {
            ProcessError(
                rspOrError,
                respondedPeer,
                TError(
                    "Error fetching meta from node %v",
                    respondedPeer.Address));
            RequestMeta();
            return;
        }

        const auto& rsp = rspOrError.Value();

        if (backup) {
            BanPeer(peers[0].Address, false);
        }

        SessionOptions_.ChunkReaderStatistics->MetaBytesTransmitted.fetch_add(
            rsp->GetTotalSize(),
            std::memory_order::relaxed);

        if (rsp->net_throttling()) {
            YT_LOG_DEBUG("Peer is throttling (Address: %v)", respondedPeer.Address);
            RequestMeta();
            return;
        }

        if (rsp->has_chunk_reader_statistics()) {
            HandleChunkReaderStatistics(rsp->chunk_reader_statistics());
        }

        TotalBytesReceived_ += rsp->ByteSize();
        OnSessionSucceeded(std::move(*rsp->mutable_chunk_meta()));
    }

    void OnSessionSucceeded(NProto::TChunkMeta&& chunkMeta)
    {
        YT_LOG_DEBUG("Chunk meta obtained (ThrottledBytes: %v, BytesReceived: %v)",
            BytesThrottled_,
            TotalBytesReceived_);

        Promise_.TrySet(New<TRefCountedChunkMeta>(std::move(chunkMeta)));
    }

    void OnSessionFailed(bool fatal) override
    {
        auto error = BuildCombinedError(TError(
            NChunkClient::EErrorCode::ChunkMetaFetchFailed,
            "Error fetching meta for chunk %v",
            ChunkId_));
        OnSessionFailed(fatal, error);
    }

    void OnSessionFailed(bool fatal, const TError& error) override
    {
        YT_LOG_DEBUG(error, "Get meta session failed (Fatal: %v)", fatal);

        if (fatal) {
            SetReaderFailed();
        }

        ReleaseThrottledBytesExcess(BandwidthThrottler_, BytesThrottled_);

        Promise_.TrySet(error);
    }

    void OnCanceled(const TError& error) override
    {
        auto wrappedError = TError(NYT::EErrorCode::Canceled, "GetMeta session canceled")
            << error;
        TSessionBase::OnCanceled(wrappedError);
        Promise_.TrySet(wrappedError);
    }
};

TFuture<TRefCountedChunkMetaPtr> TReplicationReader::FindMetaInCache(
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags,
    const ICachedChunkMeta::TMetaFetchCallback& callback)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto decodedChunkId = DecodeChunkId(ChunkId_).Id;
    if (!partitionTag && !IsJournalChunkId(decodedChunkId) && ChunkMetaCache_ && Config_->EnableChunkMetaCache) {
        return ChunkMetaCache_->Fetch(
            decodedChunkId,
            extensionTags,
            callback);
    }

    return {};
}

TFuture<TRefCountedChunkMetaPtr> TReplicationReader::GetMeta(
    const TGetMetaOptions& options,
    std::optional<int> partitionTag,
    const std::optional<std::vector<int>>& extensionTags)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto callback = BIND([
        this,
        this_ = MakeStrong(this),
        options,
        partitionTag
    ] (const std::optional<std::vector<int>>& extensionTags) {
        return New<TGetMetaSession>(
            this,
            options,
            partitionTag,
            extensionTags,
            BandwidthThrottler_,
            RpsThrottler_,
            MediumThrottler_)
            ->Run();
    });

    auto cacheFuture = FindMetaInCache(partitionTag, extensionTags, callback);
    return cacheFuture
        ? cacheFuture
        : callback(extensionTags);
}

////////////////////////////////////////////////////////////////////////////////

class TLookupRowsSession
    : public TSessionBase
{
public:
    TLookupRowsSession(
        TReplicationReader* reader,
        TOffloadingReaderOptionsPtr options,
        TSharedRange<TLegacyKey> lookupKeys,
        const std::optional<i64> estimatedSize,
        NCompression::ECodec codecId,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        IThroughputThrottlerPtr mediumThrottler,
        IInvokerPtr sessionInvoker = {})
        : TSessionBase(
            reader,
            options->ChunkReadOptions,
            std::move(bandwidthThrottler),
            std::move(rpsThrottler),
            std::move(mediumThrottler),
            std::move(sessionInvoker))
        , Options_(std::move(options))
        , LookupKeys_(std::move(lookupKeys))
        , CodecId_(codecId)
    {
        Logger.AddTag("TableId: %v, Revision: %x",
            Options_->TableId,
            Options_->MountRevision);
        if (estimatedSize) {
            BytesToThrottle_ += std::max(0L, *estimatedSize);
        }

        auto writer = CreateWireProtocolWriter();
        writer->WriteUnversionedRowset(TRange(LookupKeys_));
        Keyset_ = writer->Finish();
    }

    ~TLookupRowsSession()
    {
        Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Reader destroyed"));
    }

    TFuture<TSharedRef> Run()
    {
        YT_VERIFY(!LookupKeys_.Empty());

        StartTime_ = NProfiling::GetInstant();
        Promise_.OnCanceled(BIND(&TLookupRowsSession::OnCanceled, MakeWeak(this)));
        NextRetry();

        return Promise_.ToFuture()
            .Apply(BIND([
                startTime = StartTime_,
                readerStatistics = SessionOptions_.ChunkReaderStatistics
            ] (const TErrorOr<TSharedRef>& result) {
                readerStatistics->RecordDataWaitTime(TInstant::Now() - startTime);
                return result.ValueOrThrow();
            }));
    }

private:
    using TLookupResponse = TIntrusivePtr<NRpc::TTypedClientResponse<NChunkClient::NProto::TRspLookupRows>>;

    const TOffloadingReaderOptionsPtr Options_;
    const TSharedRange<TLegacyKey> LookupKeys_;
    const TReadSessionId ReadSessionId_;
    const NCompression::ECodec CodecId_;

    //! Promise representing the session.
    const TPromise<TSharedRef> Promise_ = NewPromise<TSharedRef>();

    std::vector<TSharedRef> Keyset_;

    i64 BytesToThrottle_ = 0;
    i64 BytesThrottled_ = 0;


    void NextPass() override
    {
        // Specific bounds for lookup.
        if (PassIndex_ >= ReaderConfig_->LookupRequestPassCount) {
            if (RetryIndex_ >= ReaderConfig_->LookupRequestRetryCount) {
                OnSessionFailed(true);
            } else {
                OnRetryFailed();
            }
            return;
        }

        if (!PrepareNextPass()) {
            return;
        }

        RequestRows();
    }

    void OnSessionFailed(bool fatal) override
    {
        auto error = BuildCombinedError(TError(
            NChunkClient::EErrorCode::RowsLookupFailed,
            "Error looking up rows in chunk %v",
            ChunkId_));
        OnSessionFailed(fatal, error);
    }

    void OnSessionFailed(bool fatal, const TError& error) override
    {
        YT_LOG_DEBUG(error, "Lookup rows session failed (Fatal: %v)", fatal);

        if (fatal) {
            SetReaderFailed();
        }

        ReleaseThrottledBytesExcess(BandwidthThrottler_, BytesThrottled_);

        Promise_.TrySet(error);
    }

    void RequestRows()
    {
        SessionInvoker_->Invoke(BIND(
            &TLookupRowsSession::DoRequestRows,
            MakeStrong(this)));
    }

    void DoRequestRows()
    {
        auto reader = Reader_.Lock();
        if (!reader || IsCanceled()) {
            return;
        }

        NProfiling::TWallTimer pickPeerTimer;

        auto candidates = PickPeerCandidates(
            reader,
            ReaderConfig_->ProbePeerCount,
            /*enableEarlyExit*/ !SessionOptions_.HedgingManager && !ReaderConfig_->LookupRpcHedgingDelay);
        if (candidates.empty()) {
            OnPassCompleted();
            return;
        }

        AsyncProbeAndSelectBestPeers(candidates, GetDesiredPeerCount(), /*blockIndexes*/ {})
            .Subscribe(BIND(
                &TLookupRowsSession::OnBestPeersSelected,
                MakeStrong(this),
                Passed(std::move(reader)),
                pickPeerTimer)
            .Via(SessionInvoker_));
        return;
    }

    void OnBestPeersSelected(
        TReplicationReaderPtr reader,
        const NProfiling::TWallTimer& pickPeerTimer,
        const TErrorOr<TPeerList>& peersOrError)
    {
        YT_ASSERT_INVOKER_AFFINITY(SessionInvoker_);

        if (IsCanceled()) {
            return;
        }

        SessionOptions_.ChunkReaderStatistics->PickPeerWaitTime.fetch_add(
            pickPeerTimer.GetElapsedValue(),
            std::memory_order::relaxed);

        auto peers = peersOrError.ValueOrThrow();

        IHedgingManagerPtr hedgingManager;
        if (SessionOptions_.HedgingManager) {
            hedgingManager = SessionOptions_.HedgingManager;
        } else if (ReaderConfig_->LookupRpcHedgingDelay) {
            hedgingManager = CreateSimpleHedgingManager(*ReaderConfig_->LookupRpcHedgingDelay);
        }

        std::optional<THedgingChannelOptions> hedgingOptions;
        if (hedgingManager) {
            hedgingOptions = THedgingChannelOptions{
                .HedgingManager = std::move(hedgingManager),
                .CancelPrimaryOnHedging = ReaderConfig_->CancelPrimaryLookupRpcRequestOnHedging,
            };
        }

        auto channel = MakePeersChannel(peers, hedgingOptions);
        if (!channel) {
            RequestRows();
            return;
        }

        if (ShouldThrottle(peers[0].Address, BytesThrottled_ == 0 && BytesToThrottle_ > 0)) {
            // NB(psushin): This is preliminary throttling. The subsequent request may fail or return partial result.
            // In order not to throttle twice, we check BytesThrottled_ is zero.
            // Still it protects us from bursty incoming traffic on the host.
            // If estimated size was not given, we fallback to post-throttling on actual received size.
            std::swap(BytesThrottled_, BytesToThrottle_);
            if (!CombinedDataByteThrottler_->IsOverdraft()) {
                CombinedDataByteThrottler_->Acquire(BytesThrottled_);
            } else {
                AsyncThrottle(
                    CombinedDataByteThrottler_,
                    BytesThrottled_,
                    BIND(
                        &TLookupRowsSession::SendLookupRowsRequest,
                        MakeStrong(this),
                        std::move(channel),
                        Passed(std::move(peers)),
                        Passed(std::move(reader)),
                        /*schemaRequested*/ false));
                return;
            }
        }

        SendLookupRowsRequest(
            channel,
            std::move(peers),
            std::move(reader),
            /*schemaRequested*/ false);

        return;
    }

    void SendLookupRowsRequest(
        const IChannelPtr& channel,
        TPeerList peers,
        TReplicationReaderPtr reader,
        bool schemaRequested)
    {
        if (IsCanceled()) {
            return;
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(ReaderConfig_->LookupRpcTimeout);

        auto req = proxy.LookupRows();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(SessionOptions_.MultiplexingBand);
        req->SetMultiplexingParallelism(SessionOptions_.MultiplexingParallelism);
        SetRequestWorkloadDescriptor(req, WorkloadDescriptor_);
        ToProto(req->mutable_chunk_id(), ChunkId_);
        ToProto(req->mutable_read_session_id(), SessionOptions_.ReadSessionId);
        req->set_timestamp(Options_->Timestamp);
        req->set_compression_codec(ToProto(CodecId_));
        ToProto(req->mutable_column_filter(), Options_->ColumnFilter);
        req->set_produce_all_versions(Options_->ProduceAllVersions);
        req->set_override_timestamp(Options_->OverrideTimestamp);
        req->set_populate_cache(true);
        req->set_enable_hash_chunk_index(Options_->EnableHashChunkIndex);
        req->set_use_direct_io(ReaderConfig_->UseDirectIO);

        auto schemaData = req->mutable_schema_data();
        ToProto(schemaData->mutable_table_id(), Options_->TableId);
        schemaData->set_revision(ToProto(Options_->MountRevision));
        schemaData->set_schema_size(Options_->TableSchema->GetMemoryUsage());
        if (schemaRequested) {
            req->SetRequestHeavy(true);
            ToProto(schemaData->mutable_schema(), *Options_->TableSchema);
        }

        req->Attachments() = Keyset_;

        BytesToThrottle_ += GetByteSize(req->Attachments());

        auto future = req->Invoke();
        SetSessionFuture(future.As<void>());
        future.Subscribe(BIND(
            &TLookupRowsSession::OnLookupRowsResponse,
            MakeStrong(this),
            std::move(channel),
            std::move(peers),
            std::move(reader))
            .Via(SessionInvoker_));
    }

    void OnLookupRowsResponse(
        const IChannelPtr& channel,
        const TPeerList& peers,
        const TReplicationReaderPtr& reader,
        const TDataNodeServiceProxy::TErrorOrRspLookupRowsPtr& rspOrError)
    {
        bool backup = IsBackup(rspOrError);
        const auto& respondedPeer = backup ? peers[1] : peers[0];

        if (!rspOrError.IsOK()) {
            ProcessError(
                rspOrError,
                respondedPeer,
                TError("Error fetching rows from node %v",
                    respondedPeer.Address));

            RequestRows();
            return;
        }

        const auto& response = rspOrError.Value();

        SessionOptions_.ChunkReaderStatistics->DataBytesTransmitted.fetch_add(
            response->GetTotalSize(),
            std::memory_order::relaxed);

        reader->AccountTraffic(response->GetTotalSize(), *respondedPeer.NodeDescriptor);

        // COMPAT(akozhikhov): Get rid of fetched_rows field.
        if (response->request_schema() || !response->fetched_rows()) {
            YT_LOG_DEBUG("Sending schema upon data node request "
                "(Backup: %v, SchemaRequested: %v, RowsFetched: %v)",
                backup,
                response->request_schema(),
                response->fetched_rows());

            if (backup) {
                SendLookupRowsRequest(
                    GetChannel(respondedPeer.Address),
                    {respondedPeer},
                    reader,
                    /*schemaRequested*/ true);
            } else {
                SendLookupRowsRequest(
                    channel,
                    peers,
                    reader,
                    /*schemaRequested*/ true);
            }

            return;
        }

        if (response->net_throttling() || response->disk_throttling()) {
            YT_LOG_DEBUG("Peer is throttling on lookup "
                "(Address: %v, Backup: %v, "
                "NetThrottling: %v, NetQueueSize: %v, DiskThrottling: %v, DiskQueueSize: %v)",
                respondedPeer.Address,
                backup,
                response->net_throttling(),
                response->net_queue_size(),
                response->disk_throttling(),
                response->disk_queue_size());
        }

        if (response->has_chunk_reader_statistics()) {
            HandleChunkReaderStatistics(response->chunk_reader_statistics());
        }

        auto result = response->Attachments()[0];
        TotalBytesReceived_ += result.Size();
        BytesToThrottle_ += result.Size();

        if (ShouldThrottle(respondedPeer.Address, BytesToThrottle_ > BytesThrottled_)) {
            auto delta = BytesToThrottle_ - BytesThrottled_;
            BytesThrottled_ += delta;
            CombinedDataByteThrottler_->Acquire(delta);
            BytesToThrottle_ = 0;
        }

        OnSessionSucceeded(backup, std::move(result));
    }

    void OnSessionSucceeded(bool backup, TSharedRef result)
    {
        YT_LOG_DEBUG("Finished processing rows response "
            "(BytesThrottled: %v, Backup: %v)",
            BytesThrottled_,
            backup);

        AccountExtraMediumBandwidth(BytesThrottled_);

        Promise_.TrySet(TrackMemory(SessionOptions_.MemoryUsageTracker, std::move(result)));
    }

    bool UpdatePeerBlockMap(
        const TPeer& /*suggestorPeer*/,
        const ::google::protobuf::RepeatedPtrField<NChunkClient::NProto::TPeerDescriptor>& /*peerDescriptors*/,
        const TAllyReplicasInfo& allyReplicas) override
    {
        if (allyReplicas) {
            MaybeUpdateSeeds(allyReplicas);
        }

        return false;
    }

    int GetDesiredPeerCount() const
    {
        return (SessionOptions_.HedgingManager || ReaderConfig_->LookupRpcHedgingDelay) ? 2 : 1;
    }

    void OnCanceled(const TError& error) override
    {
        auto wrappedError = TError(NYT::EErrorCode::Canceled, "LookupRows session canceled")
            << error;
        TSessionBase::OnCanceled(wrappedError);
        Promise_.TrySet(wrappedError);
    }
};

TFuture<TSharedRef> TReplicationReader::LookupRows(
    TOffloadingReaderOptionsPtr options,
    TSharedRange<TLegacyKey> lookupKeys,
    std::optional<i64> estimatedSize,
    NCompression::ECodec codecId,
    IInvokerPtr sessionInvoker)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    auto session = New<TLookupRowsSession>(
        this,
        std::move(options),
        std::move(lookupKeys),
        estimatedSize,
        codecId,
        BandwidthThrottler_,
        RpsThrottler_,
        MediumThrottler_,
        std::move(sessionInvoker));
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TReplicationReaderWithOverriddenThrottlers)

class TReplicationReaderWithOverriddenThrottlers
    : public IChunkReaderAllowingRepair
    , public IOffloadingReader
{
public:
    TReplicationReaderWithOverriddenThrottlers(
        TReplicationReaderPtr underlyingReader,
        IThroughputThrottlerPtr bandwidthThrottler,
        IThroughputThrottlerPtr rpsThrottler,
        IThroughputThrottlerPtr mediumThrottler)
        : UnderlyingReader_(std::move(underlyingReader))
        , BandwidthThrottler_(std::move(bandwidthThrottler))
        , RpsThrottler_(std::move(rpsThrottler))
        , MediumThrottler_(std::move(mediumThrottler))
    { }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        const std::vector<int>& blockIndexes) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        if (blockIndexes.empty()) {
            return MakeFuture<std::vector<TBlock>>({});
        }

        auto session = New<TReadBlockSetSession>(
            UnderlyingReader_.Get(),
            options,
            blockIndexes,
            BandwidthThrottler_,
            RpsThrottler_,
            MediumThrottler_);
        return session->Run();
    }

    TFuture<std::vector<TBlock>> ReadBlocks(
        const TReadBlocksOptions& options,
        int firstBlockIndex,
        int blockCount) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(blockCount >= 0);

        if (blockCount == 0) {
            return MakeFuture<std::vector<TBlock>>({});
        }

        auto session = New<TReadBlockRangeSession>(
            UnderlyingReader_.Get(),
            options,
            firstBlockIndex,
            blockCount,
            BandwidthThrottler_,
            RpsThrottler_,
            MediumThrottler_);
        return session->Run();
    }

    TFuture<TRefCountedChunkMetaPtr> GetMeta(
        const TGetMetaOptions& options,
        std::optional<int> partitionTag,
        const std::optional<std::vector<int>>& extensionTags) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto callback = BIND([
            this,
            this_ = MakeStrong(this),
            options,
            partitionTag
        ] (const std::optional<std::vector<int>>& extensionTags) {
            return New<TGetMetaSession>(
                UnderlyingReader_.Get(),
                options,
                partitionTag,
                extensionTags,
                BandwidthThrottler_,
                RpsThrottler_,
                MediumThrottler_)
                ->Run();
        });

        auto cacheFuture = UnderlyingReader_->FindMetaInCache(partitionTag, extensionTags, callback);
        return cacheFuture
            ? cacheFuture
            : callback(extensionTags);
    }

    TFuture<TSharedRef> LookupRows(
        TOffloadingReaderOptionsPtr options,
        TSharedRange<TLegacyKey> lookupKeys,
        std::optional<i64> estimatedSize,
        NCompression::ECodec codecId,
        IInvokerPtr sessionInvoker = {}) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto session = New<TLookupRowsSession>(
            UnderlyingReader_.Get(),
            std::move(options),
            std::move(lookupKeys),
            estimatedSize,
            codecId,
            BandwidthThrottler_,
            RpsThrottler_,
            MediumThrottler_,
            std::move(sessionInvoker));
        return session->Run();
    }

    TChunkId GetChunkId() const override
    {
        return UnderlyingReader_->GetChunkId();
    }

    TInstant GetLastFailureTime() const override
    {
        return UnderlyingReader_->GetLastFailureTime();
    }

    void SetSlownessChecker(TCallback<TError(i64, TDuration)> slownessChecker) override
    {
        UnderlyingReader_->SetSlownessChecker(std::move(slownessChecker));
    }

private:
    const TReplicationReaderPtr UnderlyingReader_;

    const IThroughputThrottlerPtr BandwidthThrottler_;
    const IThroughputThrottlerPtr RpsThrottler_;
    const IThroughputThrottlerPtr MediumThrottler_;
};

DEFINE_REFCOUNTED_TYPE(TReplicationReaderWithOverriddenThrottlers)

////////////////////////////////////////////////////////////////////////////////

class TRequestBatcher
    : public IRequestBatcher
{
public:
    explicit TRequestBatcher(TIntrusivePtr<TReplicationReader> reader)
        : Reader_(MakeWeak(reader))
        , ReaderConfig_(reader->Config_)
        , ReaderOptions_(reader->Options_)
        , ChunkId_(reader->ChunkId_)
        , Logger(reader->Logger)
    { }

    TFuture<TPeerResponsePtr> ProbeBlockSet(const TRequest& request) override
    {
        if (!ReaderConfig_->UseChunkProber) {
            auto batch = BuildRequestBatch<TPeerResponsePtr>(request);
            return ExecuteBatchRequest<TPeerResponsePtr>(batch);
        } else {
            return ExecuteRequest<TPeerResponsePtr>(request);
        }
    }

    TFuture<TGetBlocksResult> GetBlockSet(const TRequest& request, bool hedgingEnabled) override
    {
        if (!hedgingEnabled && ReaderConfig_->UseReadBlocksBatcher) {
            return ExecuteRequest<TGetBlocksResult>(request);
        } else {
            auto batch = BuildRequestBatch<TGetBlocksResult>(request);
            return ExecuteBatchRequest<TGetBlocksResult>(batch);
        }
    }

private:
    template <class TResponse>
    struct TRequestBatch
    {
        IChannelPtr Channel;
        THashSet<int> BlockIds;
        TIntrusivePtr<TSessionBase> Session;

        std::vector<NProto::TP2PBarrier> Barriers;
        TPeerList Peers;

        bool RequestIsInitialized = false;
        TPromise<TResponse> RequestResult;

        int RequestCount;

        TFuture<TResponse> GetRequestResultFuture()
        {
            return RequestResult.ToFuture();
        }
    };

    template <class TResponse>
    struct TNodeState
    {
        TRequestBatch<TResponse> Current;
        TRequestBatch<TResponse> Next;
    };

    const TWeakPtr<TReplicationReader> Reader_;
    const TReplicationReaderConfigPtr ReaderConfig_;
    const TRemoteReaderOptionsPtr ReaderOptions_;
    const TChunkId ChunkId_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    THashMap<std::string, TNodeState<TPeerResponsePtr>> ProbeBlocksStates_;
    THashMap<std::string, TNodeState<TGetBlocksResult>> GetBlocksStates_;

    template <class TResponse>
    TRequestBatch<TResponse> BuildRequestBatch(const TRequest& request)
    {
        TRequestBatch<TResponse> batch;
        batch.RequestIsInitialized = true;
        batch.Channel = request.Channel;
        batch.RequestResult = NewPromise<TResponse>();
        batch.Session = request.Session;
        batch.Barriers = request.Barriers;
        batch.Peers = request.Peers;
        batch.RequestCount = 1;

        for (auto index : request.BlockIndexes) {
            batch.BlockIds.insert(index);
        }

        return batch;
    }

    template <class TResponse>
    THashMap<std::string, TNodeState<TResponse>>& GetNodeStates()
    {
        if constexpr (std::is_same_v<TResponse, TPeerResponsePtr>) {
            return ProbeBlocksStates_;
        } else if constexpr (std::is_same_v<TResponse, TGetBlocksResult>) {
            return GetBlocksStates_;
        } else {
            static_assert(TDependentFalse<TResponse>);
        }
    }

    template <class TResponse>
    std::string GetRequestLogName()
    {
        if constexpr (std::is_same_v<TResponse, TPeerResponsePtr>) {
            return "ProbeBlockSet";
        } else if constexpr (std::is_same_v<TResponse, TGetBlocksResult>) {
            return "GetBlockSet";
        } else {
            static_assert(TDependentFalse<TResponse>);
        }
    }

    TGetBlocksResult HandleGetBlockSetResponse(
        TIntrusivePtr<TSessionBase> session,
        TPeerList peers,
        std::vector<int> blockIndexes,
        TErrorOr<TGetBlocksResponsePtr>&& rspOrError)
    {
        auto chunkReaderStatistics = session->SessionOptions_.ChunkReaderStatistics;

        auto&& rsp = rspOrError.ValueOrThrow();

        bool backup = IsBackup(rspOrError);
        const auto& respondedPeer = backup ? peers[1] : peers[0];

        chunkReaderStatistics->DataBytesTransmitted.fetch_add(
            rsp->GetTotalSize(),
            std::memory_order::relaxed);

        if (auto reader = Reader_.Lock()) {
            reader->AccountTraffic(rsp->GetTotalSize(), *respondedPeer.NodeDescriptor);
        }

        if (rsp->has_chunk_reader_statistics()) {
            session->HandleChunkReaderStatistics(rsp->chunk_reader_statistics());
        }

        return TGetBlocksResult{
            .RequestedBlocks = std::move(blockIndexes),
            .Response = std::move(rsp),
        };
    }

    template<class TResponse>
    TFuture<TResponse> ExecuteBatchRequest(const TRequestBatch<TResponse>& queuedBatch);

    template<>
    TFuture<TPeerResponsePtr> ExecuteBatchRequest(const TRequestBatch<TPeerResponsePtr>& queuedBatch)
    {
        TDataNodeServiceProxy proxy(queuedBatch.Channel);
        proxy.SetDefaultTimeout(ReaderConfig_->ProbeRpcTimeout);

        auto req = proxy.ProbeBlockSet();
        SetRequestWorkloadDescriptor(req, queuedBatch.Session->SessionOptions_.WorkloadDescriptor);
        req->SetResponseHeavy(true);
        ToProto(req->mutable_chunk_id(), ChunkId_);
        ToProto(req->mutable_block_indexes(), std::vector<int>(queuedBatch.BlockIds.begin(), queuedBatch.BlockIds.end()));
        req->SetAcknowledgementTimeout(std::nullopt);
        req->set_ally_replicas_revision(ToProto(queuedBatch.Session->SeedReplicas_.Revision));

        return req->Invoke();
    }

    template<>
    TFuture<TGetBlocksResult> ExecuteBatchRequest(const TRequestBatch<TGetBlocksResult>& queuedBatch)
    {
        TDataNodeServiceProxy proxy(queuedBatch.Channel);
        proxy.SetDefaultTimeout(ReaderConfig_->BlockRpcTimeout);

        auto req = proxy.GetBlockSet();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(queuedBatch.Session->SessionOptions_.MultiplexingBand);
        req->SetMultiplexingParallelism(queuedBatch.Session->SessionOptions_.MultiplexingParallelism);
        SetRequestWorkloadDescriptor(req, queuedBatch.Session->SessionOptions_.WorkloadDescriptor);
        ToProto(req->mutable_chunk_id(), ChunkId_);

        auto blockIndexes = std::vector(queuedBatch.BlockIds.begin(), queuedBatch.BlockIds.end());
        std::sort(blockIndexes.begin(), blockIndexes.end());

        ToProto(req->mutable_block_indexes(), blockIndexes);
        req->set_populate_cache(ReaderConfig_->PopulateCache);
        ToProto(req->mutable_read_session_id(), queuedBatch.Session->SessionOptions_.ReadSessionId);

        for (const auto& barrier : queuedBatch.Barriers) {
            auto* waitBarrier = req->add_wait_barriers();
            waitBarrier->CopyFrom(barrier);
        }

        return req->Invoke()
            .ApplyUnique(BIND(
                &TRequestBatcher::HandleGetBlockSetResponse,
                MakeStrong(this),
                queuedBatch.Session,
                queuedBatch.Peers,
                Passed(std::move(blockIndexes))));
    }

    template <class TResponse>
    TFuture<TResponse> ExecuteRequest(const TRequest& request)
    {
        auto guard = Guard(Lock_);
        auto& state = GetNodeStates<TResponse>()[request.Address];

        YT_VERIFY(state.Current.RequestIsInitialized || !state.Next.RequestIsInitialized);

        if (state.Current.RequestIsInitialized) {
            if (CurrentBatchContainsBlocks(state, request.BlockIndexes)) {
                return state.Current.GetRequestResultFuture();
            } else {
                AddBlocksToBatchRequest(request, state.Next);
                return state.Next.GetRequestResultFuture();
            }
        } else {
            state.Current = ExecuteNewRequest<TResponse>(request);
            auto result = state.Current.GetRequestResultFuture();
            return result;
        }
    }

    template <class TResponse>
    TRequestBatch<TResponse> ExecuteNewRequest(const TRequest& request)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        YT_LOG_DEBUG(
            "Start new batch request (Address: %v, RequestType: %v, BlockIds: %v, BlockCount: %v)",
            request.Address,
            GetRequestLogName<TResponse>(),
            MakeShrunkFormattableView(request.BlockIndexes, TDefaultFormatter(), 3),
            request.BlockIndexes.size());

        auto batch = BuildRequestBatch<TResponse>(request);

        auto requestFuture = ExecuteBatchRequest(batch);
        batch.RequestResult.SetFrom(requestFuture);
        requestFuture.Subscribe(BIND(
            &TRequestBatcher::OnBatchRequestFinished<TResponse>,
            MakeStrong(this),
            request.Address)
            .Via(batch.Session->SessionInvoker_));

        return batch;
    }

    template <class TResponse>
    void AddBlocksToBatchRequest(
        const TRequest& request,
        TRequestBatch<TResponse>& nextBatch)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        YT_LOG_DEBUG(
            "Add blocks to next batch request (Address: %v, RequestType: %v, BlockIds: %v, BlockCount: %v)",
            request.Address,
            GetRequestLogName<TResponse>(),
            MakeShrunkFormattableView(request.BlockIndexes, TDefaultFormatter(), 3),
            request.BlockIndexes.size());

        nextBatch.Channel = request.Channel;
        nextBatch.Session = request.Session;
        nextBatch.Peers = request.Peers;
        nextBatch.Barriers = request.Barriers;
        nextBatch.RequestCount++;

        if (!nextBatch.RequestIsInitialized) {
            nextBatch.RequestResult = NewPromise<TResponse>();
            nextBatch.RequestIsInitialized = true;
        }

        for (auto index : request.BlockIndexes) {
            nextBatch.BlockIds.insert(index);
        }
    }

    template <class TState>
    bool CurrentBatchContainsBlocks(const TState& probeNodeState, const std::vector<int>& blockIndexes)
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Lock_);

        for (auto index : blockIndexes) {
            if (!probeNodeState.Current.BlockIds.contains(index)) {
                return false;
            }
        }

        return true;
    }

    template <class TResponse>
    void OnBatchRequestFinished(
        const std::string& address,
        const TError& /*error*/)
    {
        auto guard = Guard(Lock_);
        auto& state = GetNodeStates<TResponse>()[address];

        if (state.Next.RequestIsInitialized) {
            state.Current = std::move(state.Next);
            auto& nextBatch = state.Current;

            YT_LOG_DEBUG(
                "Start next batch request (Address: %v, RequestType: %v, BlockIds: %v, BlockCount: %v, RequestCount: %v)",
                address,
                GetRequestLogName<TResponse>(),
                MakeShrunkFormattableView(nextBatch.BlockIds, TDefaultFormatter(), 3),
                nextBatch.BlockIds.size(),
                nextBatch.RequestCount);

            auto requestFuture = ExecuteBatchRequest(nextBatch);
            nextBatch.RequestResult.SetFrom(requestFuture);
            requestFuture.Subscribe(BIND(
                &TRequestBatcher::OnBatchRequestFinished<TResponse>,
                MakeStrong(this),
                address)
                .Via(nextBatch.Session->SessionInvoker_));
        } else {
            state.Current = {};
        }

        state.Next = {};
    }
};

DEFINE_REFCOUNTED_TYPE(TRequestBatcher)

////////////////////////////////////////////////////////////////////////////////

IRequestBatcherPtr CreateRequestBatcher(TWeakPtr<TReplicationReader> reader)
{
    return New<TRequestBatcher>(reader.Lock());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaWithMediumList seedReplicas)
{
    return New<TReplicationReader>(
        std::move(config),
        std::move(options),
        std::move(chunkReaderHost),
        chunkId,
        std::move(seedReplicas));
}

////////////////////////////////////////////////////////////////////////////////

IChunkReaderAllowingRepairPtr CreateReplicationReaderThrottlingAdapter(
    const IChunkReaderPtr& underlyingReader,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler,
    IThroughputThrottlerPtr mediumThrottler)
{
    auto* underlyingReplicationReader = dynamic_cast<TReplicationReader*>(underlyingReader.Get());
    YT_VERIFY(underlyingReplicationReader);

    return New<TReplicationReaderWithOverriddenThrottlers>(
        underlyingReplicationReader,
        std::move(bandwidthThrottler),
        std::move(rpsThrottler),
        std::move(mediumThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
