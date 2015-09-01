#include "stdafx.h"
#include "config.h"
#include "replication_reader.h"
#include "chunk_reader.h"
#include "block_cache.h"
#include "private.h"
#include "block_id.h"
#include "chunk_ypath_proxy.h"
#include "data_node_service_proxy.h"
#include "dispatcher.h"

#include <ytlib/api/config.h>
#include <ytlib/api/client.h>
#include <ytlib/api/connection.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>
#include <ytlib/chunk_client/replication_reader.h>

#include <core/misc/string.h>
#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

#include <core/logging/log.h>

#include <util/random/shuffle.h>
#include <util/generic/ymath.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NRpc;
using namespace NApi;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient::NProto;

using NYT::ToProto;
using NYT::FromProto;
using ::ToString;

const double MaxBackoffMultiplier = 1000.0;

///////////////////////////////////////////////////////////////////////////////

class TReplicationReader
    : public IChunkReader
{
public:
    TReplicationReader(
        TReplicationReaderConfigPtr config,
        TRemoteReaderOptionsPtr options,
        IClientPtr client,
        TNodeDirectoryPtr nodeDirectory,
        const TNullable<TNodeDescriptor>& localDescriptor,
        const TChunkId& chunkId,
        const TChunkReplicaList& seedReplicas,
        IBlockCachePtr blockCache,
        IThroughputThrottlerPtr throttler)
        : Config_(config)
        , Options_(options)
        , NodeDirectory_(nodeDirectory)
        , LocalDescriptor_(localDescriptor)
        , ChunkId_(chunkId)
        , BlockCache_(blockCache)
        , Throttler_(throttler)
        , NetworkName_(client->GetConnection()->GetConfig()->NetworkName)
        , ObjectServiceProxy_(client->GetMasterChannel(EMasterChannelKind::LeaderOrFollower))
        , ChunkServiceProxy_(client->GetMasterChannel(EMasterChannelKind::LeaderOrFollower))
        , InitialSeedReplicas_(seedReplicas)
        , SeedsTimestamp_(TInstant::Zero())
    {
        Logger.AddTag("ChunkId: %v", ChunkId_);
    }

    void Initialize()
    {
        if (!Config_->AllowFetchingSeedsFromMaster && InitialSeedReplicas_.empty()) {
            THROW_ERROR_EXCEPTION(
                "Cannot read chunk %v: master seeds retries are disabled and no initial seeds are given",
                ChunkId_);
        }

        if (!InitialSeedReplicas_.empty()) {
            GetSeedsPromise_ = MakePromise(InitialSeedReplicas_);
        }

        auto maybeLocalAddress = LocalDescriptor_ ? MakeNullable(LocalDescriptor_->GetAddressOrThrow(NetworkName_)) : Null;
        LOG_DEBUG("Reader initialized (InitialSeedReplicas: [%v], FetchPromPeers: %v, LocalAddress: %v, PopulateCache: %v, "
            "AllowFetchingSeedsFromMaster: %v, Network: %v)",
            JoinToString(InitialSeedReplicas_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            Config_->FetchFromPeers,
            maybeLocalAddress,
            Config_->PopulateCache,
            Config_->AllowFetchingSeedsFromMaster,
            NetworkName_);
    }

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(int firstBlockIndex, int blockCount) override;

    virtual TFuture<TChunkMeta> GetMeta(
        const TNullable<int>& partitionTag,
        const TNullable<std::vector<int>>& extensionTags) override;

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

private:
    class TSessionBase;
    class TReadBlockSetSession;
    class TReadBlockRangeSession;
    class TGetMetaSession;

    const TReplicationReaderConfigPtr Config_;
    const TRemoteReaderOptionsPtr Options_;
    const TNodeDirectoryPtr NodeDirectory_;
    const TNullable<TNodeDescriptor> LocalDescriptor_;
    const TChunkId ChunkId_;
    const IBlockCachePtr BlockCache_;
    const IThroughputThrottlerPtr Throttler_;

    NLogging::TLogger Logger = ChunkClientLogger;

    Stroka NetworkName_;
    TObjectServiceProxy ObjectServiceProxy_;
    TChunkServiceProxy ChunkServiceProxy_;

    TSpinLock SpinLock_;
    TChunkReplicaList InitialSeedReplicas_;
    TInstant SeedsTimestamp_;
    TPromise<TChunkReplicaList> GetSeedsPromise_;


    TFuture<TChunkReplicaList> AsyncGetSeeds()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);
        if (!GetSeedsPromise_) {
            LOG_DEBUG("Need fresh chunk seeds");
            GetSeedsPromise_ = NewPromise<TChunkReplicaList>();
            // Don't ask master for fresh seeds too often.
            TDelayedExecutor::Submit(
                BIND(&TReplicationReader::LocateChunk, MakeStrong(this))
                    .Via(TDispatcher::Get()->GetReaderInvoker()),
                SeedsTimestamp_ + Config_->RetryBackoffTime);
        }

        return GetSeedsPromise_;
    }

    void DiscardSeeds(TFuture<TChunkReplicaList> result)
    {
        YCHECK(result);
        YCHECK(result.IsSet());

        TGuard<TSpinLock> guard(SpinLock_);

        if (!Config_->AllowFetchingSeedsFromMaster) {
            // We're not allowed to ask master for seeds.
            // Better keep the initial ones.
            return;
        }

        if (GetSeedsPromise_.ToFuture() != result) {
            return;
        }

        YCHECK(GetSeedsPromise_.IsSet());
        GetSeedsPromise_.Reset();
    }

    void LocateChunk()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_DEBUG("Requesting chunk seeds from master");

        auto req = ChunkServiceProxy_.LocateChunks();
        ToProto(req->add_chunk_ids(), ChunkId_);
        req->Invoke().Subscribe(
            BIND(&TReplicationReader::OnLocateChunkResponse, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnLocateChunkResponse(const TChunkServiceProxy::TErrorOrRspLocateChunksPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(GetSeedsPromise_);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            SeedsTimestamp_ = TInstant::Now();
        }

        if (!rspOrError.IsOK()) {
            YCHECK(!GetSeedsPromise_.IsSet());
            GetSeedsPromise_.Set(TError(rspOrError));
            return;
        }

        const auto& rsp = rspOrError.Value();
        YCHECK(rsp->chunks_size() <= 1);
        if (rsp->chunks_size() == 0) {
            YCHECK(!GetSeedsPromise_.IsSet());
            GetSeedsPromise_.Set(TError(
                NChunkClient::EErrorCode::NoSuchChunk,
                "No such chunk %v",
                ChunkId_));
            return;
        }
        const auto& chunkInfo = rsp->chunks(0);

        NodeDirectory_->MergeFrom(rsp->node_directory());
        auto seedReplicas = FromProto<TChunkReplica, TChunkReplicaList>(chunkInfo.replicas());

        LOG_DEBUG("Chunk seeds received (SeedReplicas: [%v])",
            JoinToString(seedReplicas, TChunkReplicaAddressFormatter(NodeDirectory_)));

        YCHECK(!GetSeedsPromise_.IsSet());
        GetSeedsPromise_.Set(seedReplicas);
    }

};

///////////////////////////////////////////////////////////////////////////////

//! Please keep the items in this particular order: the further the better.
DEFINE_ENUM(EPeerType,
    (Peer)
    (Seed)
);

///////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TSessionBase
    : public TRefCounted
{
protected:
    //! Reference to the owning reader.
    TWeakPtr<TReplicationReader> Reader_;

    //! Translates node ids to node descriptors.
    TNodeDirectoryPtr NodeDirectory_;

    //! Name of the network to use from descriptor.
    Stroka NetworkName_;

    //! Zero based retry index (less than |Reader->Config->RetryCount|).
    int RetryIndex_ = 0;

    //! Zero based pass index (less than |Reader->Config->PassCount|).
    int PassIndex_ = 0;

    //! Seed replicas for the current retry.
    TChunkReplicaList SeedReplicas_;

    //! Set of peer addresses corresponding to SeedReplicas_.
    yhash_set<Stroka> SeedAddresses_;

    //! Set of peer addresses banned for the current retry.
    yhash_set<Stroka> BannedPeers_;

    //! List of candidates addresses to try, prioritized by type (seeds before peers), locality and random number.
    typedef std::priority_queue<std::tuple<EPeerType, EAddressLocality, ui32, Stroka>> TPeerQueue;
    TPeerQueue PeerQueue_;

    //! Set of addresses corresponding to PeerQueue_.
    yhash_set<Stroka> PeerSet_;

    //! Maps addresses of peers (see PeerQueue_) to descriptors.
    yhash_map<Stroka, TNodeDescriptor> AddressToDescriptor_;

    //! The instant this session has started.
    TInstant StartTime_;

    NLogging::TLogger Logger = ChunkClientLogger;


    explicit TSessionBase(TReplicationReader* reader)
        : Reader_(reader)
        , NodeDirectory_(reader->NodeDirectory_)
        , NetworkName_(reader->NetworkName_)
        , StartTime_(TInstant::Now())
    {
        Logger.AddTag("Session: %p, ChunkId: %v",
            this,
            reader->ChunkId_);
    }

    void AddPeer(const Stroka& address, const TNodeDescriptor& descriptor, EPeerType type)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        if (PeerSet_.insert(address).second) {
            EAddressLocality locality = EAddressLocality::None;
            if (reader->LocalDescriptor_) {
                const auto& localDescriptor = *reader->LocalDescriptor_;
                locality = ComputeAddressLocality(descriptor, localDescriptor);
            }
            PeerQueue_.emplace(type, locality, RandomNumber<ui32>(), address);
            YCHECK(AddressToDescriptor_.insert(std::make_pair(address, descriptor)).second);
        }
    }

    const TNodeDescriptor& GetPeerDescriptor(const Stroka& address)
    {
        auto it = AddressToDescriptor_.find(address);
        YCHECK(it != AddressToDescriptor_.end());
        return it->second;
    }

    void ClearPeers()
    {
        PeerQueue_ = TPeerQueue{};
        PeerSet_.clear();
        AddressToDescriptor_.clear();
    }

    void BanPeer(const Stroka& address)
    {
        if (BannedPeers_.insert(address).second) {
            LOG_DEBUG("Node is banned for the current retry (Address: %v)",
                address);
        }
    }

    bool IsPeerBanned(const Stroka& address)
    {
        return BannedPeers_.find(address) != BannedPeers_.end();
    }

    bool IsSeed(const Stroka& address)
    {
        return SeedAddresses_.find(address) != SeedAddresses_.end();
    }

    bool HasMorePeers()
    {
        return !PeerQueue_.empty();
    }

    Stroka PickNextPeer()
    {
        YCHECK(!PeerQueue_.empty());
        auto address = std::get<3>(PeerQueue_.top());
        PeerQueue_.pop();
        return address;
    }

    virtual void NextRetry()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        YCHECK(!GetSeedsResult);

        LOG_DEBUG("Retry started: %v of %v",
            RetryIndex_ + 1,
            reader->Config_->RetryCount);

        GetSeedsResult = reader->AsyncGetSeeds();
        GetSeedsResult.Subscribe(
            BIND(&TSessionBase::OnGotSeeds, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));

        PassIndex_ = 0;
        BannedPeers_.clear();
    }

    virtual void NextPass() = 0;

    void OnRetryFailed()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        int retryCount = reader->Config_->RetryCount;
        LOG_DEBUG("Retry failed: %v of %v",
            RetryIndex_ + 1,
            retryCount);

        YCHECK(GetSeedsResult);
        reader->DiscardSeeds(GetSeedsResult);
        GetSeedsResult.Reset();

        ++RetryIndex_;
        if (RetryIndex_ >= retryCount) {
            OnSessionFailed();
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TSessionBase::NextRetry, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()),
            reader->Config_->RetryBackoffTime);
    }


    bool PrepareNextPass()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return false;

        LOG_DEBUG("Pass started: %v of %v",
            PassIndex_ + 1,
            reader->Config_->PassCount);

        ClearPeers();

        for (auto replica : SeedReplicas_) {
            const auto& descriptor = NodeDirectory_->GetDescriptor(replica);
            auto address = descriptor.FindAddress(NetworkName_);
            if (address && !IsPeerBanned(*address)) {
                AddPeer(*address, descriptor, EPeerType::Seed);
            }
        }

        if (PeerQueue_.empty()) {
            LOG_DEBUG("No feasible seeds to start a pass");
            OnRetryFailed();
            return false;
        }

        return true;
    }

    void OnPassCompleted()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        int passCount = reader->Config_->PassCount;
        LOG_DEBUG("Pass completed: %v of %v",
            PassIndex_ + 1,
            passCount);

        ++PassIndex_;
        if (PassIndex_ >= passCount) {
            OnRetryFailed();
            return;
        }

        auto backoffMultiplier = std::min(
            std::pow(reader->Config_->PassBackoffTimeMultiplier, PassIndex_ - 1), 
            MaxBackoffMultiplier);

        auto backoffTime = reader->Config_->MinPassBackoffTime * backoffMultiplier;
        backoffTime = std::min(backoffTime, reader->Config_->MaxPassBackoffTime);

        TDelayedExecutor::Submit(
            BIND(&TSessionBase::NextPass, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()),
            backoffTime);
    }


    void RegisterError(const TError& error)
    {
        LOG_ERROR(error);
        InnerErrors.push_back(error);
    }

    TError BuildCombinedError(const TError& error)
    {
        return error << InnerErrors;
    }

    virtual void OnSessionFailed() = 0;

private:
    //! Errors collected by the session.
    std::vector<TError> InnerErrors;

    TFuture<TChunkReplicaList> GetSeedsResult;


    void OnGotSeeds(const TErrorOr<TChunkReplicaList>& result)
    {
        if (!result.IsOK()) {
            RegisterError(TError(
                NChunkClient::EErrorCode::MasterCommunicationFailed,
                "Error requesting seeds from master")
                << result);
            OnSessionFailed();
            return;
        }

        SeedReplicas_ = result.Value();
        if (SeedReplicas_.empty()) {
            RegisterError(TError("Chunk is lost"));
            OnRetryFailed();
            return;
        }

        SeedAddresses_.clear();
        for (auto replica : SeedReplicas_) {
            auto descriptor = NodeDirectory_->GetDescriptor(replica.GetNodeId());
            auto address = descriptor.FindAddress(NetworkName_);
            if (address) {
                SeedAddresses_.insert(*address);
            } else {
                RegisterError(TError(
                    NNodeTrackerClient::EErrorCode::NoSuchNetwork,
                    "Cannot find %Qv address for seed %v",
                    NetworkName_,
                    descriptor.GetDefaultAddress()));
                OnSessionFailed();
            }
        }

        NextPass();
    }

};

///////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TReadBlockSetSession
    : public TSessionBase
{
public:
    TReadBlockSetSession(TReplicationReader* reader, const std::vector<int>& blockIndexes)
        : TSessionBase(reader)
        , Promise_(NewPromise<std::vector<TSharedRef>>())
        , BlockIndexes_(blockIndexes)
    {
        Logger.AddTag("Blocks: [%v]", JoinToString(blockIndexes));
    }

    ~TReadBlockSetSession()
    {
        Promise_.TrySet(TError("Reader terminated"));
    }

    TFuture<std::vector<TSharedRef>> Run()
    {
        FetchBlocksFromCache();

        if (GetUnfetchedBlockIndexes().empty()) {
            LOG_DEBUG("All requested blocks are fetched from cache");
            OnSessionSucceeded();
        } else {
            NextRetry();
        }

        return Promise_;
    }

private:
    //! Promise representing the session.
    TPromise<std::vector<TSharedRef>> Promise_;

    //! Block indexes to read during the session.
    std::vector<int> BlockIndexes_;

    //! Blocks that are fetched so far.
    yhash_map<int, TSharedRef> Blocks_;

    //! Maps peer addresses to block indexes.
    yhash_map<Stroka, yhash_set<int>> PeerBlocksMap_;


    virtual void NextPass() override
    {
        if (!PrepareNextPass())
            return;

        PeerBlocksMap_.clear();
        auto blockIndexes = GetUnfetchedBlockIndexes();
        for (const auto& address : PeerSet_) {
            PeerBlocksMap_[address] = yhash_set<int>(blockIndexes.begin(), blockIndexes.end());
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

    std::vector<int> GetRequestBlockIndexes(const Stroka& address, const std::vector<int>& indexesToFetch)
    {
        std::vector<int> result;
        result.reserve(indexesToFetch.size());

        auto it = PeerBlocksMap_.find(address);
        YCHECK(it != PeerBlocksMap_.end());
        const auto& peerBlockIndexes = it->second;

        for (int blockIndex : indexesToFetch) {
            if (peerBlockIndexes.find(blockIndex) != peerBlockIndexes.end()) {
                result.push_back(blockIndex);
            }
        }

        return result;
    }


    void FetchBlocksFromCache()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

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


    void RequestBlocks()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        while (true) {
            FetchBlocksFromCache();

            auto unfetchedBlockIndexes = GetUnfetchedBlockIndexes();
            if (unfetchedBlockIndexes.empty()) {
                OnSessionSucceeded();
                break;
            }

            if (!HasMorePeers()) {
                OnPassCompleted();
                break;
            }

            auto currentAddress = PickNextPeer();
            auto blockIndexes = GetRequestBlockIndexes(currentAddress, unfetchedBlockIndexes);

            if (!IsPeerBanned(currentAddress) && !blockIndexes.empty()) {
                LOG_DEBUG("Requesting blocks from peer (Address: %v, Blocks: [%v])",
                    currentAddress,
                    JoinToString(unfetchedBlockIndexes));

                IChannelPtr channel;
                try {
                    channel = HeavyNodeChannelFactory->CreateChannel(currentAddress);
                } catch (const std::exception& ex) {
                    RegisterError(ex);
                    continue;
                }

                TDataNodeServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(reader->Config_->BlockRpcTimeout);

                auto req = proxy.GetBlockSet();
                req->SetStartTime(StartTime_);
                ToProto(req->mutable_chunk_id(), reader->ChunkId_);
                ToProto(req->mutable_block_indexes(), unfetchedBlockIndexes);
                req->set_populate_cache(reader->Config_->PopulateCache);
                req->set_session_type(static_cast<int>(reader->Options_->SessionType));
                if (reader->LocalDescriptor_) {
                    auto expirationTime = TInstant::Now() + reader->Config_->PeerExpirationTimeout;
                    ToProto(req->mutable_peer_descriptor(), reader->LocalDescriptor_.Get());
                    req->set_peer_expiration_time(expirationTime.GetValue());
                }

                req->Invoke().Subscribe(
                    BIND(
                        &TReadBlockSetSession::OnGotBlocks,
                        MakeStrong(this),
                        currentAddress,
                        req)
                    .Via(TDispatcher::Get()->GetReaderInvoker()));
                break;
            }

            LOG_DEBUG("Skipping peer (Address: %v)",
                currentAddress);
        }
    }

    void OnGotBlocks(
        const Stroka& address,
        TDataNodeServiceProxy::TReqGetBlockSetPtr req,
        const TDataNodeServiceProxy::TErrorOrRspGetBlockSetPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            RegisterError(TError("Error fetching blocks from node %v",
                address)
                << rspOrError);
            if (rspOrError.GetCode() != NRpc::EErrorCode::Unavailable) {
                // Do not ban node if it says "Unavailable".
                BanPeer(address);
            }
            RequestBlocks();
            return;
        }

        const auto& rsp = rspOrError.Value();
        ProcessResponse(address, req, rsp)
            .Subscribe(BIND(&TReadBlockSetSession::OnResponseProcessed, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    TFuture<void> ProcessResponse(
        const Stroka& adddress,
        TDataNodeServiceProxy::TReqGetBlockSetPtr req,
        TDataNodeServiceProxy::TRspGetBlockSetPtr rsp)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return VoidFuture;
        }

        if (rsp->throttling()) {
            LOG_DEBUG("Peer is throttling (Address: %v)",
                adddress);
        }

        i64 bytesReceived = 0;
        std::vector<int> receivedBlockIndexes;
        for (int index = 0; index < rsp->Attachments().size(); ++index) {
            const auto& block = rsp->Attachments()[index];
            if (!block)
                continue;

            int blockIndex = req->block_indexes(index);
            auto blockId = TBlockId(reader->ChunkId_, blockIndex);

            // Only keep source address if P2P is on.
            auto sourceDescriptor = reader->LocalDescriptor_
                ? TNullable<TNodeDescriptor>(GetPeerDescriptor(adddress))
                : TNullable<TNodeDescriptor>(Null);
            reader->BlockCache_->Put(blockId, EBlockType::CompressedData, block, sourceDescriptor);

            YCHECK(Blocks_.insert(std::make_pair(blockIndex, block)).second);
            bytesReceived += block.Size();
            receivedBlockIndexes.push_back(blockIndex);
        }

        if (reader->Config_->FetchFromPeers) {
            for (const auto& peerDescriptor : rsp->peer_descriptors()) {
                int blockIndex = peerDescriptor.block_index();
                TBlockId blockId(reader->ChunkId_, blockIndex);
                for (const auto& protoPeerDescriptor : peerDescriptor.node_descriptors()) {
                    auto suggestedDescriptor = FromProto<TNodeDescriptor>(protoPeerDescriptor);
                    auto suggestedAddress = suggestedDescriptor.FindAddress(NetworkName_);
                    if (suggestedAddress) {
                        AddPeer(*suggestedAddress, suggestedDescriptor, EPeerType::Peer);
                        PeerBlocksMap_[*suggestedAddress].insert(blockIndex);
                        LOG_DEBUG("Peer descriptor received (Block: %v, SuggestorAddress: %v, SuggestedAddress: %v)",
                            blockIndex,
                            adddress,
                            *suggestedAddress);
                    } else {
                        LOG_WARNING("Peer suggestion ignored, required network is missing (Block: %v, SuggestorAddress: %v, SuggestedAddress: %v)",
                            blockIndex,
                            adddress,
                            suggestedDescriptor.GetDefaultAddress());
                    }
                }
            }
        } else {
            if (rsp->peer_descriptors_size() > 0) {
                LOG_DEBUG("Peer suggestions received but ignored");
            }
        }

        if (IsSeed(adddress) && !rsp->has_complete_chunk()) {
            LOG_DEBUG("Seed does not contain the chunk (Address: %v)",
                adddress);
            BanPeer(adddress);
        }

        LOG_DEBUG("Finished processing block response (Address: %v, BlocksReceived: [%v], BytesReceived: %v, PeersSuggested: %v)",
            adddress,
            JoinToString(receivedBlockIndexes),
            bytesReceived,
            rsp->peer_descriptors_size());

        return reader->Throttler_->Throttle(bytesReceived);
    }

    void OnResponseProcessed(const TError& error)
    {
        if (!error.IsOK()) {
            RegisterError(error);
            OnPassCompleted();
            return;
        }
        RequestBlocks();
    }


    void OnSessionSucceeded()
    {
        LOG_DEBUG("All requested blocks are fetched");

        std::vector<TSharedRef> blocks;
        blocks.reserve(BlockIndexes_.size());
        for (int blockIndex : BlockIndexes_) {
            const auto& block = Blocks_[blockIndex];
            YCHECK(block);
            blocks.push_back(block);
        }
        Promise_.TrySet(std::vector<TSharedRef>(blocks));
    }

    virtual void OnSessionFailed() override
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        auto error = BuildCombinedError(TError(
            "Error fetching blocks for chunk %v",
            reader->ChunkId_));
        Promise_.TrySet(error);
    }
};

TFuture<std::vector<TSharedRef>> TReplicationReader::ReadBlocks(const std::vector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadBlockSetSession>(this, blockIndexes);
    return BIND(&TReadBlockSetSession::Run, session)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

///////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TReadBlockRangeSession
    : public TSessionBase
{
public:
    TReadBlockRangeSession(
        TReplicationReader* reader,
        int firstBlockIndex,
        int blockCount)
        : TSessionBase(reader)
        , Promise_(NewPromise<std::vector<TSharedRef>>())
        , FirstBlockIndex_(firstBlockIndex)
        , BlockCount_(blockCount)
    {
        Logger.AddTag("Blocks: %v-%v",
            FirstBlockIndex_,
            FirstBlockIndex_ + BlockCount_ - 1);
    }

    TFuture<std::vector<TSharedRef>> Run()
    {
        if (BlockCount_ == 0) {
            return MakeFuture(std::vector<TSharedRef>());
        }

        NextRetry();
        return Promise_;
    }

private:
    //! Promise representing the session.
    TPromise<std::vector<TSharedRef>> Promise_;

    //! First block index to fetch.
    int FirstBlockIndex_;

    //! Number of blocks to fetch.
    int BlockCount_;

    //! Blocks that are fetched so far.
    std::vector<TSharedRef> FetchedBlocks_;


    virtual void NextPass() override
    {
        if (!PrepareNextPass())
            return;

        RequestBlocks();
    }

    void RequestBlocks()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        while (true) {
            if (!FetchedBlocks_.empty()) {
                OnSessionSucceeded();
                return;
            }

            if (!HasMorePeers()) {
                OnPassCompleted();
                break;
            }

            auto currentAddress = PickNextPeer();
            if (!IsPeerBanned(currentAddress)) {
                LOG_DEBUG("Requesting blocks from peer (Address: %v, Blocks: %v-%v)",
                    currentAddress,
                    FirstBlockIndex_,
                    FirstBlockIndex_ + BlockCount_ - 1);

                IChannelPtr channel;
                try {
                    channel = HeavyNodeChannelFactory->CreateChannel(currentAddress);
                } catch (const std::exception& ex) {
                    RegisterError(ex);
                    continue;
                }

                TDataNodeServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(reader->Config_->BlockRpcTimeout);

                auto req = proxy.GetBlockRange();
                req->SetStartTime(StartTime_);
                ToProto(req->mutable_chunk_id(), reader->ChunkId_);
                req->set_first_block_index(FirstBlockIndex_);
                req->set_block_count(BlockCount_);
                req->set_session_type(static_cast<int>(reader->Options_->SessionType));

                req->Invoke().Subscribe(
                    BIND(
                        &TReadBlockRangeSession::OnGotBlocks,
                        MakeStrong(this),
                        currentAddress,
                        req)
                    .Via(TDispatcher::Get()->GetReaderInvoker()));
                break;
            }

            LOG_DEBUG("Skipping peer (Address: %v)",
                currentAddress);
        }
    }

    void OnGotBlocks(
        const Stroka& address,
        TDataNodeServiceProxy::TReqGetBlockRangePtr req,
        const TDataNodeServiceProxy::TErrorOrRspGetBlockRangePtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            RegisterError(TError("Error fetching blocks from node %v",
                address)
                << rspOrError);
            if (rspOrError.GetCode() != NRpc::EErrorCode::Unavailable) {
                // Do not ban node if it says "Unavailable".
                BanPeer(address);
            }
            RequestBlocks();
            return;
        }

        const auto& rsp = rspOrError.Value();
        ProcessResponse(address, req, rsp)
            .Subscribe(BIND(&TReadBlockRangeSession::OnResponseProcessed, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    TFuture<void> ProcessResponse(
        const Stroka& address,
        TDataNodeServiceProxy::TReqGetBlockRangePtr req,
        TDataNodeServiceProxy::TRspGetBlockRangePtr rsp)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return VoidFuture;
        }

        if (rsp->throttling()) {
            LOG_DEBUG("Peer is throttling (Address: %v)",
                address);
        }

        const auto& blocks = rsp->Attachments();
        int blocksReceived = 0;
        i64 bytesReceived = 0;
        for (const auto& block : blocks) {
            if (!block)
                break;
            blocksReceived += 1;
            bytesReceived += block.Size();
            FetchedBlocks_.push_back(block);
        }

        if (IsSeed(address) && !rsp->has_complete_chunk()) {
            LOG_DEBUG("Seed does not contain the chunk (Address: %v)",
                address);
            BanPeer(address);
        }

        if (!rsp->throttling() && blocksReceived == 0) {
            LOG_DEBUG("Peer has no relevant blocks (Address: %v)",
                address);
            BanPeer(address);
        }

        LOG_DEBUG("Finished processing block response (Address: %v, BlocksReceived: %v-%v, BytesReceived: %v)",
            address,
            FirstBlockIndex_,
            FirstBlockIndex_ + blocksReceived - 1,
            bytesReceived);

        return reader->Throttler_->Throttle(bytesReceived);
    }

    void OnResponseProcessed(const TError& error)
    {
        if (!error.IsOK()) {
            RegisterError(error);
            OnPassCompleted();
            return;
        }
        RequestBlocks();
    }


    void OnSessionSucceeded()
    {
        LOG_DEBUG("Some blocks are fetched (Blocks: %v-%v)",
            FirstBlockIndex_,
            FirstBlockIndex_ + FetchedBlocks_.size() - 1);

        Promise_.TrySet(std::vector<TSharedRef>(FetchedBlocks_));
    }

    virtual void OnSessionFailed() override
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        auto error = BuildCombinedError(TError(
            "Error fetching blocks for chunk %v",
            reader->ChunkId_));
        Promise_.TrySet(error);
    }

};

TFuture<std::vector<TSharedRef>> TReplicationReader::ReadBlocks(
    int firstBlockIndex,
    int blockCount)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadBlockRangeSession>(this, firstBlockIndex, blockCount);
    return BIND(&TReadBlockRangeSession::Run, session)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

///////////////////////////////////////////////////////////////////////////////

class TReplicationReader::TGetMetaSession
    : public TSessionBase
{
public:
    TGetMetaSession(
        TReplicationReader* reader,
        const TNullable<int> partitionTag,
        const TNullable<std::vector<int>>& extensionTags)
        : TSessionBase(reader)
        , Promise_(NewPromise<TChunkMeta>())
        , PartitionTag_(partitionTag)
        , ExtensionTags_(extensionTags)
    { }

    ~TGetMetaSession()
    {
        Promise_.TrySet(TError("Reader terminated"));
    }

    TFuture<TChunkMeta> Run()
    {
        NextRetry();
        return Promise_;
    }

private:
    //! Promise representing the session.
    TPromise<TChunkMeta> Promise_;

    const TNullable<int> PartitionTag_;
    const TNullable<std::vector<int>> ExtensionTags_;


    virtual void NextPass()
    {
        if (!PrepareNextPass())
            return;

        RequestMeta();
    }

    void RequestMeta()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        if (!HasMorePeers()) {
            OnPassCompleted();
            return;
        }

        auto address = PickNextPeer();
        LOG_DEBUG("Requesting chunk meta (Address: %v)", address);

        IChannelPtr channel;
        try {
            channel = LightNodeChannelFactory->CreateChannel(address);
        } catch (const std::exception& ex) {
            OnGetChunkMetaFailed(address, ex);
            return;
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(reader->Config_->MetaRpcTimeout);

        auto req = proxy.GetChunkMeta();
        req->SetStartTime(StartTime_);
        ToProto(req->mutable_chunk_id(), reader->ChunkId_);
        req->set_all_extension_tags(!ExtensionTags_);
        if (PartitionTag_) {
            req->set_partition_tag(PartitionTag_.Get());
        }
        if (ExtensionTags_) {
            ToProto(req->mutable_extension_tags(), *ExtensionTags_);
        }

        req->Invoke().Subscribe(
            BIND(&TGetMetaSession::OnGetChunkMeta, MakeStrong(this), address)
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnGetChunkMeta(
        const Stroka& address,
        const TDataNodeServiceProxy::TErrorOrRspGetChunkMetaPtr& rspOrError)
    {
        if (!rspOrError.IsOK()) {
            OnGetChunkMetaFailed(address, rspOrError);
            return;
        }

        const auto& rsp = rspOrError.Value();
        OnSessionSucceeded(rsp->chunk_meta());
    }

    void OnGetChunkMetaFailed(
        const Stroka& address,
        const TError& error)
    {
        LOG_WARNING(error, "Error requesting chunk meta (Address: %v)",
            address);

        RegisterError(error);

        if (error.GetCode() !=  NRpc::EErrorCode::Unavailable) {
            BanPeer(address);
        }

        RequestMeta();
    }


    void OnSessionSucceeded(const NProto::TChunkMeta& chunkMeta)
    {
        LOG_DEBUG("Chunk meta obtained");
        Promise_.TrySet(chunkMeta);
    }

    virtual void OnSessionFailed() override
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        auto error = BuildCombinedError(TError(
            "Error fetching meta for chunk %v",
            reader->ChunkId_));
        Promise_.TrySet(error);
    }

};

TFuture<TChunkMeta> TReplicationReader::GetMeta(
    const TNullable<int>& partitionTag,
    const TNullable<std::vector<int>>& extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TGetMetaSession>(this, partitionTag, extensionTags);
    return BIND(&TGetMetaSession::Run, session)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

///////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    IClientPtr client,
    TNodeDirectoryPtr nodeDirectory,
    const TNullable<TNodeDescriptor>& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    IBlockCachePtr blockCache,
    IThroughputThrottlerPtr throttler)
{
    YCHECK(config);
    YCHECK(blockCache);
    YCHECK(client);
    YCHECK(nodeDirectory);

    auto reader = New<TReplicationReader>(
        config,
        options,
        client,
        nodeDirectory,
        localDescriptor,
        chunkId,
        seedReplicas,
        blockCache,
        throttler);
    reader->Initialize();
    return reader;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
