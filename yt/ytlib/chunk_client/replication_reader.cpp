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

#include <core/logging/tagged_logger.h>

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

class TSessionBase;
class TReadSession;
class TGetMetaSession;

class TReplicationReader
    : public IReader
{
public:
    typedef TErrorOr<TChunkReplicaList> TGetSeedsResult;
    typedef TFuture<TGetSeedsResult> TAsyncGetSeedsResult;
    typedef TPromise<TGetSeedsResult> TAsyncGetSeedsPromise;

    TReplicationReader(
        TReplicationReaderConfigPtr config,
        IBlockCachePtr blockCache,
        IChannelPtr masterChannel,
        TNodeDirectoryPtr nodeDirectory,
        const TNullable<TNodeDescriptor>& localDescriptor,
        const TChunkId& chunkId,
        const TChunkReplicaList& seedReplicas,
        const Stroka& networkName,
        EReadSessionType sessionType,
        IThroughputThrottlerPtr throttler)
        : Config_(config)
        , BlockCache_(blockCache)
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
        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId_)));
    }

    void Initialize()
    {
        if (!Config_->AllowFetchingSeedsFromMaster && InitialSeedReplicas_.empty()) {
            THROW_ERROR_EXCEPTION(
                "Reader is unusable: master seeds retries are disabled and no initial seeds are given (ChunkId: %s)",
                ~ToString(ChunkId_));
        }

        if (!InitialSeedReplicas_.empty()) {
            GetSeedsPromise_ = MakePromise(TGetSeedsResult(InitialSeedReplicas_));
        }

        LOG_INFO("Reader initialized (InitialSeedReplicas: [%s], FetchPromPeers: %s, LocalDescriptor: %s, EnableCaching: %s, Network: %s)",
            ~JoinToString(InitialSeedReplicas, TChunkReplicaAddressFormatter(NodeDirectory)),
            ~FormatBool(Config_->FetchFromPeers),
            LocalDescriptor ? ~ToString(LocalDescriptor->GetAddressOrThrow(NetworkName_)) : "<Null>",
            ~FormatBool(Config_->EnableNodeCaching),
            ~NetworkName_);
    }

    virtual TAsyncReadResult ReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncGetMetaResult GetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<i32>* extensionTags = nullptr) override;

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

private:
    friend class TSessionBase;
    friend class TReadSession;
    friend class TGetMetaSession;

    TReplicationReaderConfigPtr Config_;
    IBlockCachePtr BlockCache_;
    TNodeDirectoryPtr NodeDirectory_;
    TNullable<TNodeDescriptor> LocalDescriptor_;
    TChunkId ChunkId_;
    Stroka NetworkName_;
    EReadSessionType SessionType_;
    IThroughputThrottlerPtr Throttler_;
    NLog::TTaggedLogger Logger;

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
            GetSeedsPromise_.Set(TError("No such chunk %s", ~ToString(ChunkId_)));
            return;
        }
        const auto& chunkInfo = rsp->chunks(0);

        NodeDirectory_->MergeFrom(rsp->node_directory());
        auto seedReplicas = FromProto<TChunkReplica, TChunkReplicaList>(chunkInfo.replicas());

        // TODO(babenko): use std::random_shuffle here but make sure it uses true randomness.
        Shuffle(seedReplicas.begin(), seedReplicas.end());

        LOG_INFO("Chunk seeds received (SeedReplicas: [%s])",
            ~JoinToString(seedReplicas, TChunkReplicaAddressFormatter(NodeDirectory_)));

        YCHECK(!GetSeedsPromise_.IsSet());
        GetSeedsPromise_.Set(seedReplicas);
    }

};

///////////////////////////////////////////////////////////////////////////////

class TSessionBase
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

    //! Set of peer addresses corresponding to SeedReplcias.
    yhash_set<Stroka> SeedAddresses_;

    //! Set of peer addresses banned for the current retry.
    yhash_set<Stroka> BannedPeers_;

    //! List of candidates to try.
    std::vector<TNodeDescriptor> PeerList_;

    //! Current index in #PeerList.
    int PeerIndex_;

    //! The instant this session has started.
    TInstant StartTime_;

    NLog::TTaggedLogger Logger;


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
        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(reader->ChunkId_)));
    }

    void BanPeer(const Stroka& address)
    {
        if (BannedPeers_.insert(address).second) {
            LOG_INFO("Node is banned for the current retry (Address: %s)",
                ~address);
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

    virtual void NextRetry()
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return;
        }

        YCHECK(!GetSeedsResult);

        LOG_INFO("Retry started: %d of %d",
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
        LOG_INFO("Retry failed: %d of %d",
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


    template <class TSeedHandler>
    bool PrepareNextPass(const TSeedHandler& seedHandler)
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return false;

        LOG_INFO("Pass started: %d of %d",
            PassIndex_ + 1,
            reader->Config_->PassCount);

        PeerList_.clear();
        PeerIndex_ = 0;
        for (auto replica : SeedReplicas_) {
            const auto& descriptor = NodeDirectory_->GetDescriptor(replica);
            auto address = descriptor.FindAddress(NetworkName_);
            if (address && !IsPeerBanned(*address)) {
                seedHandler(descriptor);
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
        for (auto replica : SeedReplicas) {
            auto descriptor = NodeDirectory->GetDescriptor(replica.GetNodeId());
            auto address = descriptor.FindAddress(NetworkName_);
            if (address) {
                SeedAddresses_.insert(*address);
            } else {
                RegisterError(TError(
                    NChunkClient::EErrorCode::AddressNotFound,
                    "Cannot find %s address for %s",
                    ~NetworkName_.Quote(),
                    ~descriptor.GetDefaultAddress()));
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

class TReadSession
    : public TSessionBase
{
public:
    TReadSession(TReplicationReader* reader, const std::vector<int>& blockIndexes)
        : TSessionBase(reader)
        , Promise_(NewPromise<IReader::TReadResult>())
        , BlockIndexes_(blockIndexes)
    {
        Logger.AddTag(Sprintf("ReadSession: %p", this));
    }

    ~TReadSession()
    {
        if (!Promise_.IsSet()) {
            Promise_.Set(TError("Reader terminated"));
        }
    }

    IReader::TAsyncReadResult Run()
    {
        FetchBlocksFromCache();

        if (GetUnfetchedBlockIndexes().empty()) {
            LOG_INFO("All chunk blocks are fetched from cache");
            OnSessionSucceeded();
        } else {
            NextRetry();
        }

        return Promise_;
    }

private:
    //! Promise representing the session.
    IReader::TAsyncReadPromise Promise_;

    //! Block indexes to read during the session.
    std::vector<int> BlockIndexes_;

    //! Blocks that are fetched so far.
    yhash_map<int, TSharedRef> FetchedBlocks_;

    struct TPeerBlocksInfo
    {
        TNodeDescriptor NodeDescriptor;
        yhash_set<int> BlockIndexes;
    };

    //! Known peers and their blocks (address -> TPeerBlocksInfo).
    yhash_map<Stroka, TPeerBlocksInfo> PeerBlocksMap_;


    virtual void NextPass() override
    {
        PeerBlocksMap_.clear();

        auto seedHandler = [&] (const TNodeDescriptor& descriptor) {
            for (int blockIndex : BlockIndexes_) {
                AddPeer(descriptor, blockIndex);
            }
        };

        if (!PrepareNextPass(seedHandler))
            return;

        RequestBlocks();
    }

    void AddPeer(const TNodeDescriptor& nodeDescriptor, int blockIndex)
    {
        const auto& address = nodeDescriptor.GetDefaultAddress();
        auto peerBlocksMapIt = PeerBlocksMap_.find(address);
        if (peerBlocksMapIt == PeerBlocksMap_.end()) {
            peerBlocksMapIt = PeerBlocksMap_.insert(std::make_pair(address, TPeerBlocksInfo())).first;
            PeerList_.push_back(nodeDescriptor);
        }
        peerBlocksMapIt->second.BlockIndexes.insert(blockIndex);
    }

    TNodeDescriptor PickNextPeer()
    {
        // When the time comes to fetch from a non-seeding node, pick a random one.
        if (PeerIndex_ >= SeedReplicas_.size()) {
            size_t count = PeerList_.size() - PeerIndex_;
            size_t randomIndex = PeerIndex_ + RandomNumber(count);
            std::swap(PeerList_[PeerIndex_], PeerList_[randomIndex]);
        }
        return PeerList_[PeerIndex_++];
    }

    std::vector<int> GetUnfetchedBlockIndexes()
    {
        std::vector<int> result;
        result.reserve(BlockIndexes_.size());
        for (int blockIndex : BlockIndexes_) {
            if (FetchedBlocks_.find(blockIndex) == FetchedBlocks_.end()) {
                result.push_back(blockIndex);
            }
        }
        return result;
    }

    std::vector<int> GetRequestBlockIndexes(
        const TNodeDescriptor& nodeDescriptor,
        const std::vector<int>& indexesToFetch)
    {
        std::vector<int> result;
        result.reserve(indexesToFetch.size());

        auto peerBlocksMapIt = PeerBlocksMap_.find(nodeDescriptor.GetDefaultAddress());
        YCHECK(peerBlocksMapIt != PeerBlocksMap_.end());

        const auto& blocksInfo = peerBlocksMapIt->second;

        for (int blockIndex : indexesToFetch) {
            if (blocksInfo.BlockIndexes.find(blockIndex) != blocksInfo.BlockIndexes.end()) {
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
            if (FetchedBlocks_.find(blockIndex) == FetchedBlocks_.end()) {
                TBlockId blockId(reader->ChunkId_, blockIndex);
                auto block = reader->BlockCache_->Find(blockId);
                if (block) {
                    LOG_INFO("Block is fetched from cache (Block: %d)", blockIndex);
                    YCHECK(FetchedBlocks_.insert(std::make_pair(blockIndex, block)).second);
                }
            }
        }
    }


    void RequestBlocks()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        LOG_INFO("Looking for blocks to request");

        while (true) {
            FetchBlocksFromCache();

            auto unfetchedBlockIndexes = GetUnfetchedBlockIndexes();
            if (unfetchedBlockIndexes.empty()) {
                OnSessionSucceeded();
                break;
            }

            if (PeerIndex_ >= PeerList_.size()) {
                OnPassCompleted();
                break;
            }

            auto currentDescriptor = PickNextPeer();
            auto blockIndexes = GetRequestBlockIndexes(currentDescriptor, unfetchedBlockIndexes);

            if (!IsPeerBanned(currentDescriptor.GetAddress(NetworkName_)) && !blockIndexes.empty()) {
                LOG_INFO("Requesting blocks from peer (Address: %s, Blocks: [%s])",
                    ~currentDescriptor.GetAddress(NetworkName_),
                    ~JoinToString(unfetchedBlockIndexes));

                IChannelPtr channel;
                try {
                    channel = LightNodeChannelFactory->CreateChannel(currentDescriptor.GetAddress(NetworkName_));
                } catch (const std::exception& ex) {
                    RegisterError(ex);
                    continue;
                }

                TDataNodeServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(reader->Config_->BlockRpcTimeout);

                auto request = proxy.GetBlocks();
                request->SetStartTime(StartTime_);
                ToProto(request->mutable_chunk_id(), reader->ChunkId_);
                for (int index : unfetchedBlockIndexes) {
                    auto* range = request->add_block_ranges();
                    range->set_first_index(index);
                    range->set_count(1);
                }
                request->set_enable_caching(reader->Config_->EnableNodeCaching);
                request->set_session_type(reader->SessionType_);
                if (reader->LocalDescriptor_) {
                    auto expirationTime = TInstant::Now() + reader->Config_->PeerExpirationTimeout;
                    ToProto(request->mutable_peer_descriptor(), reader->LocalDescriptor_.Get());
                    request->set_peer_expiration_time(expirationTime.GetValue());
                }

                request->Invoke().Subscribe(
                    BIND(
                        &TReadSession::OnGotBlocks,
                        MakeStrong(this),
                        currentDescriptor,
                        request)
                    .Via(TDispatcher::Get()->GetReaderInvoker()));
                break;
            }

            LOG_INFO("Skipping peer (Address: %s)", ~currentDescriptor.GetAddress(NetworkName_));
        }
    }

    void OnGotBlocks(
        const TNodeDescriptor& requestedDescriptor,
        TDataNodeServiceProxy::TReqGetBlocksPtr req,
        TDataNodeServiceProxy::TRspGetBlocksPtr rsp)
    {
        const auto& requestedAddress = requestedDescriptor.GetAddress(NetworkName_);
        if (!rsp->IsOK()) {
            RegisterError(TError("Error fetching blocks from node %s",
                ~requestedAddress)
                << *rsp);
            if (rsp->GetError().GetCode() != NRpc::EErrorCode::Unavailable) {
                // Do not ban node if it says "Unavailable".
                BanPeer(requestedAddress);
            }
            RequestBlocks();
            return;
        }

        ProcessResponse(requestedDescriptor, req, rsp)
            .Subscribe(BIND(&TReadSession::RequestBlocks, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    TFuture<void> ProcessResponse(
        const TNodeDescriptor& requestedDescriptor,
        TDataNodeServiceProxy::TReqGetBlocksPtr req,
        TDataNodeServiceProxy::TRspGetBlocksPtr rsp)
    {
        auto reader = Reader_.Lock();
        if (!reader) {
            return VoidFuture;
        }

        const auto& requestedAddress = requestedDescriptor.GetAddress(NetworkName_);
        LOG_INFO("Started processing block response (Address: %s)",
            ~requestedAddress);

        i64 totalSize = 0;
        int attachmentIndex = 0;
        for (const auto& range : rsp->block_ranges()) {
            for (int blockIndex = range.first_index();
                 blockIndex < range.first_index() + range.count();
                 ++blockIndex)
            {
                TBlockId blockId(reader->ChunkId_, blockIndex);
                auto block = rsp->Attachments()[attachmentIndex++];
                if (block) {
                    LOG_INFO("Block data received (BlockId: %d)",
                        ~ToString(blockId));

                    // Only keep source address if P2P is on.
                    auto source = reader->LocalDescriptor_
                        ? TNullable<TNodeDescriptor>(requestedDescriptor)
                        : TNullable<TNodeDescriptor>(Null);
                    reader->BlockCache_->Put(blockId, block, source);

                    YCHECK(FetchedBlocks_.insert(std::make_pair(blockIndex, block)).second);
                    totalSize += block.Size();
            }
        }

		if (reader->Config->FetchFromPeers) {
            for (const auto& protoPeerDescriptor : blockInfo.p2p_descriptors()) {
                auto peerDescriptor = FromProto<TNodeDescriptor>(protoP2PDescriptor);
                if (peerDescriptor.FindAddress(NetworkName_)) {
                    AddPeer(peerDescriptor, blockIndex);
                    LOG_INFO("Peer descriptor received (Block: %d, Address: %s)",
                        blockIndex,
                        ~peerDescriptor.GetAddress(NetworkName_));
                 } else {
                    RegisterError(TError(
                        NChunkClient::EErrorCode::AddressNotFound,
                        "Cannot find %s address for %s"
                        ~NetworkName_.Quote(),
                        ~peerDescriptor.GetDefaultAddress()));
                    OnSessionFailed();
        		}
            }
        }


        if (IsSeed(requestedAddress) && !rsp->has_complete_chunk()) {
            LOG_INFO("Seed does not contain the chunk (Address: %s)",
                ~requestedAddress);
            BanPeer(requestedAddress);
        }

        LOG_INFO("Finished processing block response (TotalSize: %" PRId64 ")",
            totalSize);

        return reader->Throttler_->Throttle(totalSize);
    }


    void OnSessionSucceeded()
    {
        LOG_INFO("All chunk blocks are fetched");

        std::vector<TSharedRef> blocks;
        blocks.reserve(BlockIndexes_.size());
        for (int blockIndex : BlockIndexes_) {
            auto block = FetchedBlocks_[blockIndex];
            YCHECK(block);
            blocks.push_back(block);
        }
        Promise_.Set(IReader::TReadResult(blocks));
    }

    virtual void OnSessionFailed() override
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        auto error = BuildCombinedError(TError(
            "Error fetching blocks for chunk %s",
            ~ToString(reader->ChunkId_)));
        Promise_.Set(error);
    }
};

TReplicationReader::TAsyncReadResult TReplicationReader::ReadBlocks(const std::vector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto session = New<TReadSession>(this, blockIndexes);
    return BIND(&TReadSession::Run, session)
        .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
        .Run();
}

///////////////////////////////////////////////////////////////////////////////

class TGetMetaSession
    : public TSessionBase
{
public:
    TGetMetaSession(
        TReplicationReader* reader,
        const TNullable<int> partitionTag,
        const std::vector<int>* extensionTags)
        : TSessionBase(reader)
        , Promise_(NewPromise<IReader::TGetMetaResult>())
        , PartitionTag_(partitionTag)
    {
        if (extensionTags) {
            ExtensionTags_ = *extensionTags;
            AllExtensionTags_ = false;
        } else {
            AllExtensionTags_ = true;
        }

        Logger.AddTag(Sprintf("GetMetaSession: %p", this));
    }

    ~TGetMetaSession()
    {
        if (!Promise_.IsSet()) {
            Promise_.Set(TError("Reader terminated"));
        }
    }

    IReader::TAsyncGetMetaResult Run()
    {
        NextRetry();
        return Promise_;
    }

private:
    //! Promise representing the session.
    IReader::TAsyncGetMetaPromise Promise_;

    std::vector<int> ExtensionTags_;
    TNullable<int> PartitionTag_;
    bool AllExtensionTags_;


    virtual void NextPass()
    {
        auto seedHandler = [&] (const TNodeDescriptor& descriptor) {
            PeerList_.push_back(descriptor);
        };

        if (!PrepareNextPass(seedHandler))
            return;

        RequestInfo();
    }

    void RequestInfo()
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        if (PeerIndex_ >= PeerList_.size()) {
            OnPassCompleted();
            return;
        }

        const auto& descriptor = PeerList_[PeerIndex_];
        const auto& address = descriptor.GetAddress(NetworkName_);

        LOG_INFO("Requesting chunk meta (Address: %s)", ~address);

        IChannelPtr channel;
        try {
            channel = LightNodeChannelFactory->CreateChannel(address);
        } catch (const std::exception& ex) {
            OnGetChunkMetaResponseFailed(descriptor, ex);
            return;
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(reader->Config_->MetaRpcTimeout);

        auto request = proxy.GetChunkMeta();
        request->SetStartTime(StartTime_);
        ToProto(request->mutable_chunk_id(), reader->ChunkId_);
        request->set_all_extension_tags(AllExtensionTags_);

        if (PartitionTag_) {
            request->set_partition_tag(PartitionTag_.Get());
        }

        ToProto(request->mutable_extension_tags(), ExtensionTags_);
        request->Invoke().Subscribe(
            BIND(&TGetMetaSession::OnGetChunkMetaResponse, MakeStrong(this), descriptor)
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnGetChunkMetaResponse(
        const TNodeDescriptor& descriptor,
        TDataNodeServiceProxy::TRspGetChunkMetaPtr rsp)
    {
        if (!rsp->IsOK()) {
            OnGetChunkMetaResponseFailed(descriptor, *rsp);
            return;
        }

        OnSessionSucceeded(rsp->chunk_meta());
    }

    void OnGetChunkMetaResponseFailed(
        const TNodeDescriptor& descriptor,
        const TError& error)
    {
        const auto& address = descriptor.GetAddress(NetworkName_);

        LOG_WARNING(error, "Error requesting chunk meta (Address: %s)",
            ~address);

        RegisterError(error);

        ++PeerIndex_;
        if (error.GetCode() !=  NRpc::EErrorCode::Unavailable) {
            BanPeer(address);
        }

        RequestInfo();
    }


    void OnSessionSucceeded(const NProto::TChunkMeta& chunkMeta)
    {
        LOG_INFO("Chunk meta obtained");
        Promise_.Set(IReader::TGetMetaResult(chunkMeta));
    }

    virtual void OnSessionFailed() override
    {
        auto reader = Reader_.Lock();
        if (!reader)
            return;

        auto error = BuildCombinedError(TError(
            "Error fetching meta for chunk %s",
            ~ToString(reader->ChunkId_)));
        Promise_.Set(error);
    }

};

TReplicationReader::TAsyncGetMetaResult TReplicationReader::GetChunkMeta(
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
    IBlockCachePtr blockCache,
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
    YCHECK(blockCache);
    YCHECK(masterChannel);
    YCHECK(nodeDirectory);

    auto reader = New<TReplicationReader>(
        config,
        blockCache,
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
