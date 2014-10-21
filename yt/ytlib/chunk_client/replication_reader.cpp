#include "stdafx.h"
#include "config.h"
#include "replication_reader.h"
#include "reader.h"
#include "block_cache.h"
#include "private.h"
#include "block_id.h"
#include "chunk_ypath_proxy.h"
#include "data_node_service_proxy.h"
#include "dispatcher.h"

#include <core/misc/string.h>
#include <core/misc/protobuf_helpers.h>

#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/delayed_executor.h>

#include <core/logging/log.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>
#include <ytlib/chunk_client/replication_reader.h>

#include <util/random/shuffle.h>
#include <util/generic/ymath.h>

#include <cmath>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NChunkClient;
using namespace NConcurrency;

using NYT::ToProto;
using NYT::FromProto;
using ::ToString;

///////////////////////////////////////////////////////////////////////////////

class TReplicationReader
    : public IReader
{
public:
    typedef TErrorOr<TChunkReplicaList> TGetSeedsResult;
    typedef TFuture<TGetSeedsResult> TAsyncGetSeedsResult;
    typedef TPromise<TGetSeedsResult> TAsyncGetSeedsPromise;

    TReplicationReader(
        TReplicationReaderConfigPtr config,
        IBlockCachePtr compressedBlockCache,
        IChannelPtr masterChannel,
        TNodeDirectoryPtr nodeDirectory,
        const TNullable<TNodeDescriptor>& localDescriptor,
        const TChunkId& chunkId,
        const TChunkReplicaList& seedReplicas,
        const Stroka& networkName,
        EReadSessionType sessionType,
        IThroughputThrottlerPtr throttler)
        : Config_(config)
        , CompressedBlockCache_(compressedBlockCache)
        , NodeDirectory_(nodeDirectory)
        , LocalDescriptor_(localDescriptor)
        , ChunkId_(chunkId)
        , NetworkName_(networkName)
        , SessionType_(sessionType)
        , Throttler_(throttler)
        , Logger(ChunkClientLogger)
        , ObjectServiceProxy_(masterChannel)
        , ChunkServiceProxy_(masterChannel)
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
            GetSeedsPromise_ = MakePromise(TGetSeedsResult(InitialSeedReplicas_));
        }

        LOG_INFO("Reader initialized (InitialSeedReplicas: [%v], FetchPromPeers: %v, LocalDescriptor: %v, EnableCaching: %v, Network: %v)",
            JoinToString(InitialSeedReplicas_, TChunkReplicaAddressFormatter(NodeDirectory_)),
            FormatBool(Config_->FetchFromPeers),
            LocalDescriptor_ ? ToString(LocalDescriptor_->GetAddressOrThrow(NetworkName_)) : "<Null>",
            FormatBool(Config_->EnableCaching),
            NetworkName_);
    }

    virtual TAsyncReadBlocksResult ReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncReadBlocksResult ReadBlocks(int firstBlockIndex, int blockCount) override;

    virtual TAsyncGetMetaResult GetMeta(
        const TNullable<int>& partitionTag,
        const std::vector<i32>* extensionTags = nullptr) override;

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

private:
    class TSessionBase;
    class TReadBlockSetSession;
    class TReadBlockRangeSession;
    class TGetMetaSession;

    TReplicationReaderConfigPtr Config_;
    IBlockCachePtr CompressedBlockCache_;
    TNodeDirectoryPtr NodeDirectory_;
    TNullable<TNodeDescriptor> LocalDescriptor_;
    TChunkId ChunkId_;
    Stroka NetworkName_;
    EReadSessionType SessionType_;
    IThroughputThrottlerPtr Throttler_;
    NLog::TLogger Logger;

    TObjectServiceProxy ObjectServiceProxy_;
    TChunkServiceProxy ChunkServiceProxy_;

    TSpinLock SpinLock_;
    TChunkReplicaList InitialSeedReplicas_;
    TInstant SeedsTimestamp_;
    TAsyncGetSeedsPromise GetSeedsPromise_;


    TAsyncGetSeedsResult AsyncGetSeeds()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock_);
        if (!GetSeedsPromise_) {
            LOG_INFO("Need fresh chunk seeds");
            GetSeedsPromise_ = NewPromise<TGetSeedsResult>();
            // Don't ask master for fresh seeds too often.
            TDelayedExecutor::Submit(
                BIND(&TReplicationReader::LocateChunk, MakeStrong(this))
                    .Via(TDispatcher::Get()->GetReaderInvoker()),
                SeedsTimestamp_ + Config_->RetryBackoffTime);
        }

        return GetSeedsPromise_;
    }

    void DiscardSeeds(TAsyncGetSeedsResult result)
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

        LOG_INFO("Requesting chunk seeds from master");

        auto req = ChunkServiceProxy_.LocateChunks();
        ToProto(req->add_chunk_ids(), ChunkId_);
        req->Invoke().Subscribe(
            BIND(&TReplicationReader::OnLocateChunkResponse, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnLocateChunkResponse(TChunkServiceProxy::TRspLocateChunksPtr rsp)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(GetSeedsPromise_);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            SeedsTimestamp_ = TInstant::Now();
        }

        if (!rsp->IsOK()) {
            YCHECK(!GetSeedsPromise_.IsSet());
            GetSeedsPromise_.Set(rsp->GetError());
            return;
        }

        YCHECK(rsp->chunks_size() <= 1);
        if (rsp->chunks_size() == 0) {
            YCHECK(!GetSeedsPromise_.IsSet());
            GetSeedsPromise_.Set(TError("No such chunk %v", ChunkId_));
            return;
        }
        const auto& chunkInfo = rsp->chunks(0);

        NodeDirectory_->MergeFrom(rsp->node_directory());
        auto seedReplicas = FromProto<TChunkReplica, TChunkReplicaList>(chunkInfo.replicas());

        // TODO(babenko): use std::random_shuffle here but make sure it uses true randomness.
        Shuffle(seedReplicas.begin(), seedReplicas.end());

        LOG_INFO("Chunk seeds received (SeedReplicas: [%v])",
            JoinToString(seedReplicas, TChunkReplicaAddressFormatter(NodeDirectory_)));

        YCHECK(!GetSeedsPromise_.IsSet());
        GetSeedsPromise_.Set(seedReplicas);
    }

};

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
    int RetryIndex_;

    //! Zero based pass index (less than |Reader->Config->PassCount|).
    int PassIndex_;

    //! Seed replicas for the current retry.
    TChunkReplicaList SeedReplicas_;

    //! Set of peer addresses corresponding to SeedReplicas_.
    yhash_set<Stroka> SeedAddresses_;

    //! Set of peer addresses banned for the current retry.
    yhash_set<Stroka> BannedPeers_;

    //! List of candidates addresses to try.
    std::vector<Stroka> PeerList_;

    //! Set of addresses corresponding to PeerList_.
    yhash_set<Stroka> PeerSet_;

    //! Maps addresses of peers (see PeerList_) to descriptors.
    yhash_map<Stroka, TNodeDescriptor> AddressToDescriptor_;

    //! Current index in #PeerList.
    int PeerIndex_;

    //! The instant this session has started.
    TInstant StartTime_;

    NLog::TLogger Logger;


    explicit TSessionBase(TReplicationReader* reader)
        : Reader_(reader)
        , NodeDirectory_(reader->NodeDirectory_)
        , NetworkName_(reader->NetworkName_)
        , RetryIndex_(0)
        , PassIndex_(0)
        , PeerIndex_(0)
        , StartTime_(TInstant::Now())
        , Logger(ChunkClientLogger)
    {
        Logger.AddTag("ChunkId: %v", reader->ChunkId_);
    }

    void AddPeer(const Stroka& address, const TNodeDescriptor& descriptor)
    {
        if (PeerSet_.insert(address).second) {
            PeerList_.push_back(address);
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
        PeerList_.clear();
        PeerSet_.clear();
        PeerIndex_ = 0;
        AddressToDescriptor_.clear();
    }

    void BanPeer(const Stroka& address)
    {
        if (BannedPeers_.insert(address).second) {
            LOG_INFO("Node is banned for the current retry (Address: %v)",
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
        return PeerIndex_ < PeerList_.size();
    }

    Stroka PickNextPeer()
    {
        // When the time comes to fetch from a non-seeding node, pick a random one.
        if (PeerIndex_ >= SeedReplicas_.size()) {
            size_t count = PeerList_.size() - PeerIndex_;
            size_t randomIndex = PeerIndex_ + RandomNumber(count);
            std::swap(PeerList_[PeerIndex_], PeerList_[randomIndex]);
        }
        return PeerList_[PeerIndex_++];
    }

    virtual void NextRetry()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        YCHECK(!GetSeedsResult);

        LOG_INFO("Retry started: %v of %v",
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
        LOG_INFO("Retry failed: %v of %v",
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

        LOG_INFO("Pass started: %v of %v",
            PassIndex_ + 1,
            reader->Config_->PassCount);

        ClearPeers();

        for (auto replica : SeedReplicas_) {
            const auto& descriptor = NodeDirectory_->GetDescriptor(replica);
            auto address = descriptor.FindAddress(NetworkName_);
            if (address && !IsPeerBanned(*address)) {
                AddPeer(*address, descriptor);
            }
        }

        if (PeerList_.empty()) {
            LOG_INFO("No feasible seeds to start a pass");
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
        LOG_INFO("Pass completed: %d of %d",
            PassIndex_ + 1,
            passCount);

        ++PassIndex_;
        if (PassIndex_ >= passCount) {
            OnRetryFailed();
            return;
        }

        auto backoffTime = reader->Config_->MinPassBackoffTime *
            std::pow(reader->Config_->PassBackoffTimeMultiplier, PassIndex_ - 1);

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

    TError BuildCombinedError(TError error)
    {
        return error << InnerErrors;
    }

    virtual void OnSessionFailed() = 0;

private:
    //! Errors collected by the session.
    std::vector<TError> InnerErrors;

    TReplicationReader::TAsyncGetSeedsResult GetSeedsResult;


    void OnGotSeeds(TReplicationReader::TGetSeedsResult result)
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

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

        // Prefer local node if in seeds.
        for (auto it = SeedReplicas_.begin(); it != SeedReplicas_.end(); ++it) {
            const auto& descriptor = reader->NodeDirectory_->GetDescriptor(*it);
            if (descriptor.IsLocal()) {
                auto localSeed = *it;
                SeedReplicas_.erase(it);
                SeedReplicas_.insert(SeedReplicas_.begin(), localSeed);
                break;
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
        , Promise_(NewPromise<TReadBlocksResult>())
        , BlockIndexes_(blockIndexes)
    {
        Logger.AddTag("Session: %v", this);
    }

    ~TReadBlockSetSession()
    {
        Promise_.TrySet(TError("Reader terminated"));
    }

    TAsyncReadBlocksResult Run()
    {
        FetchBlocksFromCache();

        if (GetUnfetchedBlockIndexes().empty()) {
            LOG_INFO("All requested blocks are fetched from cache");
            OnSessionSucceeded();
        } else {
            NextRetry();
        }

        return Promise_;
    }

private:
    //! Promise representing the session.
    TPromise<TReadBlocksResult> Promise_;

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
        for (const auto& address : PeerList_) {
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
                auto block = reader->CompressedBlockCache_->Find(blockId);
                if (block) {
                    LOG_INFO("Block is fetched from cache (Block: %v)", blockIndex);
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
                LOG_INFO("Requesting blocks from peer (Address: %v, Blocks: [%v])",
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
                req->set_enable_caching(reader->Config_->EnableCaching);
                req->set_session_type(reader->SessionType_);
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

            LOG_INFO("Skipping peer (Address: %v)",
                currentAddress);
        }
    }

    void OnGotBlocks(
        const Stroka& address,
        TDataNodeServiceProxy::TReqGetBlockSetPtr req,
        TDataNodeServiceProxy::TRspGetBlockSetPtr rsp)
    {
        if (!rsp->IsOK()) {
            RegisterError(TError("Error fetching blocks from node %v",
                address)
                << *rsp);
            if (rsp->GetError().GetCode() != NRpc::EErrorCode::Unavailable) {
                // Do not ban node if it says "Unavailable".
                BanPeer(address);
            }
            RequestBlocks();
            return;
        }

        ProcessResponse(address, req, rsp)
            .Subscribe(BIND(&TReadBlockSetSession::RequestBlocks, MakeStrong(this))
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
            LOG_INFO("Peer is throttling (Address: %v)",
                adddress);
            return VoidFuture;
        }

        int blocksReceived = 0;
        i64 bytesReceived = 0;

        for (int index = 0; index < rsp->Attachments().size(); ++index) {
            const auto& block = rsp->Attachments()[index];
            if (!block)
                continue;

            int blockIndex = req->block_indexes(index);
            TBlockId blockId(reader->ChunkId_, blockIndex);

            LOG_INFO("Block received (Block: %d)",
                blockIndex);

            // Only keep source address if P2P is on.
            auto sourceDescriptor = reader->LocalDescriptor_
                ? TNullable<TNodeDescriptor>(GetPeerDescriptor(adddress))
                : TNullable<TNodeDescriptor>(Null);
            reader->CompressedBlockCache_->Put(blockId, block, sourceDescriptor);

            YCHECK(Blocks_.insert(std::make_pair(blockIndex, block)).second);
            blocksReceived += 1;
            bytesReceived += block.Size();
        }

        if (reader->Config_->FetchFromPeers) {
            for (const auto& peerDescriptor : rsp->peer_descriptors()) {
                int blockIndex = peerDescriptor.block_index();
                TBlockId blockId(reader->ChunkId_, blockIndex);
                for (const auto& protoPeerDescriptor : peerDescriptor.node_descriptors()) {
                    auto peerDescriptor = FromProto<TNodeDescriptor>(protoPeerDescriptor);
                    auto peerAddress = peerDescriptor.FindAddress(NetworkName_);
                    if (peerAddress) {
                        AddPeer(*peerAddress, peerDescriptor);
                        PeerBlocksMap_[*peerAddress].insert(blockIndex);
                        LOG_INFO("Peer descriptor received (Block: %v, Address: %v)",
                            blockIndex,
                            *peerAddress);
                    } else {
                        LOG_WARNING("Peer descriptor ignored, required network is missing (Block: %v, Address: %v)",
                            blockIndex,
                            peerDescriptor.GetDefaultAddress());
                    }
                }
            }
        }


        if (IsSeed(adddress) && !rsp->has_complete_chunk()) {
            LOG_INFO("Seed does not contain the chunk (Address: %v)",
                adddress);
            BanPeer(adddress);
        }

        LOG_INFO("Finished processing block response (BlocksReceived: %d, BytesReceived: %" PRId64 ")",
            blocksReceived,
            bytesReceived);

        return reader->Throttler_->Throttle(bytesReceived);
    }


    void OnSessionSucceeded()
    {
        LOG_INFO("All requested blocks are fetched");

        std::vector<TSharedRef> blocks;
        blocks.reserve(BlockIndexes_.size());
        for (int blockIndex : BlockIndexes_) {
            auto block = Blocks_[blockIndex];
            YCHECK(block);
            blocks.push_back(block);
        }
        Promise_.TrySet(TReadBlocksResult(blocks));
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

IReader::TAsyncReadBlocksResult TReplicationReader::ReadBlocks(const std::vector<int>& blockIndexes)
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
        , Promise_(NewPromise<TReadBlocksResult>())
        , FirstBlockIndex_(firstBlockIndex)
        , BlockCount_(blockCount)
    {
        Logger.AddTag("Session: %v", this);
    }

    ~TReadBlockRangeSession()
    {
        Promise_.TrySet(TError("Reader terminated"));
    }

    TAsyncReadBlocksResult Run()
    {
        if (BlockCount_ == 0) {
            return MakeFuture(TReadBlocksResult());
        }

        NextRetry();
        return Promise_;
    }

private:
    //! Promise representing the session.
    TPromise<TReadBlocksResult> Promise_;

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
                LOG_INFO("Requesting blocks from peer (Address: %v, Blocks: %v-%v)",
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
                req->set_session_type(reader->SessionType_);

                req->Invoke().Subscribe(
                    BIND(
                        &TReadBlockRangeSession::OnGotBlocks,
                        MakeStrong(this),
                        currentAddress,
                        req)
                    .Via(TDispatcher::Get()->GetReaderInvoker()));
                break;
            }

            LOG_INFO("Skipping peer (Address: %v)",
                currentAddress);
        }
    }

    void OnGotBlocks(
        const Stroka& address,
        TDataNodeServiceProxy::TReqGetBlockRangePtr req,
        TDataNodeServiceProxy::TRspGetBlockRangePtr rsp)
    {
        if (!rsp->IsOK()) {
            RegisterError(TError("Error fetching blocks from node %v",
                address)
                << *rsp);
            if (rsp->GetError().GetCode() != NRpc::EErrorCode::Unavailable) {
                // Do not ban node if it says "Unavailable".
                BanPeer(address);
            }
            RequestBlocks();
            return;
        }

        ProcessResponse(address, req, rsp)
            .Subscribe(BIND(&TReadBlockRangeSession::RequestBlocks, MakeStrong(this))
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
            LOG_INFO("Peer is throttling (Address: %v)",
                address);
            return VoidFuture;
        }

        int blocksReceived = static_cast<int>(rsp->Attachments().size());
        i64 bytesReceived = 0;

        if (blocksReceived > 0) {
            LOG_INFO("Block range received (Blocks: %v-%v)",
                FirstBlockIndex_,
                FirstBlockIndex_ + blocksReceived - 1);
            for (const auto& block : rsp->Attachments()) {
                if (!block)
                    break;
                FetchedBlocks_.push_back(block);
                bytesReceived += block.Size();
            }
        }

        if (IsSeed(address) && !rsp->has_complete_chunk()) {
            LOG_INFO("Seed does not contain the chunk (Address: %v)",
                address);
            BanPeer(address);
        }

        if (blocksReceived == 0) {
            LOG_INFO("Peer has no relevant blocks (Address: %v)",
                address);
            BanPeer(address);
        }

        LOG_INFO("Finished processing block response (BlocksReceived: %d, BytesReceived: %" PRId64 ")",
            blocksReceived,
            bytesReceived);

        return reader->Throttler_->Throttle(bytesReceived);
    }


    void OnSessionSucceeded()
    {
        LOG_INFO("Some blocks are fetched (Blocks: %d-%d)",
            FirstBlockIndex_,
            FirstBlockIndex_ + static_cast<int>(FetchedBlocks_.size()) - 1);

        Promise_.TrySet(TReadBlocksResult(FetchedBlocks_));
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

IReader::TAsyncReadBlocksResult TReplicationReader::ReadBlocks(
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
        const std::vector<int>* extensionTags)
        : TSessionBase(reader)
        , Promise_(NewPromise<TGetMetaResult>())
        , PartitionTag_(partitionTag)
    {
        if (extensionTags) {
            ExtensionTags_ = *extensionTags;
            AllExtensionTags_ = false;
        } else {
            AllExtensionTags_ = true;
        }

        Logger.AddTag("Session: %v", this);
    }

    ~TGetMetaSession()
    {
        Promise_.TrySet(TError("Reader terminated"));
    }

    TAsyncGetMetaResult Run()
    {
        NextRetry();
        return Promise_;
    }

private:
    //! Promise representing the session.
    TPromise<TGetMetaResult> Promise_;

    std::vector<int> ExtensionTags_;
    TNullable<int> PartitionTag_;
    bool AllExtensionTags_;


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
        LOG_INFO("Requesting chunk meta (Address: %v)", address);

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
        req->set_all_extension_tags(AllExtensionTags_);
        if (PartitionTag_) {
            req->set_partition_tag(PartitionTag_.Get());
        }
        ToProto(req->mutable_extension_tags(), ExtensionTags_);

        req->Invoke().Subscribe(
            BIND(&TGetMetaSession::OnGetChunkMeta, MakeStrong(this), address)
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnGetChunkMeta(
        const Stroka& address,
        TDataNodeServiceProxy::TRspGetChunkMetaPtr rsp)
    {
        if (!rsp->IsOK()) {
            OnGetChunkMetaFailed(address, *rsp);
            return;
        }

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
        LOG_INFO("Chunk meta obtained");
        Promise_.TrySet(TGetMetaResult(chunkMeta));
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

TReplicationReader::TAsyncGetMetaResult TReplicationReader::GetMeta(
    const TNullable<int>& partitionTag,
    const std::vector<i32>* extensionTags)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TGetMetaSession>(this, partitionTag, extensionTags);
    return BIND(&TGetMetaSession::Run, session)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

///////////////////////////////////////////////////////////////////////////////

IReaderPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr compressedBlockCache,
    NRpc::IChannelPtr masterChannel,
    TNodeDirectoryPtr nodeDirectory,
    const TNullable<TNodeDescriptor>& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
    const Stroka& networkName,
    EReadSessionType sessionType,
    IThroughputThrottlerPtr throttler)
{
    YCHECK(config);
    YCHECK(compressedBlockCache);
    YCHECK(masterChannel);
    YCHECK(nodeDirectory);

    auto reader = New<TReplicationReader>(
        config,
        compressedBlockCache,
        masterChannel,
        nodeDirectory,
        localDescriptor,
        chunkId,
        seedReplicas,
        networkName,
        sessionType,
        throttler);
    reader->Initialize();
    return reader;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
