#include "stdafx.h"
#include "config.h"
#include "replication_reader.h"
#include "async_reader.h"
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

#include <util/random/shuffle.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NNodeTrackerClient;
using namespace NConcurrency;

using NYT::ToProto;
using NYT::FromProto;
using ::ToString;

///////////////////////////////////////////////////////////////////////////////

class TSessionBase;
class TReadSession;
class TGetMetaSession;

class TReplicationReader
    : public IAsyncReader
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
        EReadSessionType sessionType,
        IThroughputThrottlerPtr throttler)
        : Config(config)
        , BlockCache(blockCache)
        , NodeDirectory(nodeDirectory)
        , LocalDescriptor(localDescriptor)
        , ChunkId(chunkId)
        , SessionType(sessionType)
        , Throttler(throttler)
        , Logger(ChunkReaderLogger)
        , ObjectServiceProxy(masterChannel)
        , ChunkServiceProxy(masterChannel)
        , InitialSeedReplicas(seedReplicas)
        , SeedsTimestamp(TInstant::Zero())
    {
        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(ChunkId)));
    }

    void Initialize()
    {
        if (!Config->AllowFetchingSeedsFromMaster && InitialSeedReplicas.empty()) {
            THROW_ERROR_EXCEPTION(
                "Reader is unusable: master seeds retries are disabled and no initial seeds are given (ChunkId: %s)",
                ~ToString(ChunkId));
        }

        if (!InitialSeedReplicas.empty()) {
            GetSeedsPromise = MakePromise(TGetSeedsResult(InitialSeedReplicas));
        }

        LOG_INFO("Reader initialized (InitialSeedReplicas: [%s], FetchPromPeers: %s, LocalDescriptor: %s, EnableCaching: %s)",
            ~JoinToString(InitialSeedReplicas, TChunkReplicaAddressFormatter(NodeDirectory)),
            ~ToString(Config->FetchFromPeers),
            LocalDescriptor ? ~ToString(LocalDescriptor->Address) : "<Null>",
            ~FormatBool(Config->EnableNodeCaching));
    }

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag,
        const std::vector<i32>* tags = nullptr) override;

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId;
    }

private:
    friend class TSessionBase;
    friend class TReadSession;
    friend class TGetMetaSession;

    TReplicationReaderConfigPtr Config;
    IBlockCachePtr BlockCache;
    TNodeDirectoryPtr NodeDirectory;
    TNullable<TNodeDescriptor> LocalDescriptor;
    TChunkId ChunkId;
    EReadSessionType SessionType;
    IThroughputThrottlerPtr Throttler;
    NLog::TTaggedLogger Logger;

    TObjectServiceProxy ObjectServiceProxy;
    TChunkServiceProxy ChunkServiceProxy;

    TSpinLock SpinLock;
    TChunkReplicaList InitialSeedReplicas;
    TInstant SeedsTimestamp;
    TAsyncGetSeedsPromise GetSeedsPromise;

    TAsyncGetSeedsResult AsyncGetSeeds()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        if (!GetSeedsPromise) {
            LOG_INFO("Need fresh chunk seeds");
            GetSeedsPromise = NewPromise<TGetSeedsResult>();
            // Don't ask master for fresh seeds too often.
            TDelayedExecutor::Submit(
                BIND(&TReplicationReader::LocateChunk, MakeStrong(this))
                    .Via(TDispatcher::Get()->GetReaderInvoker()),
                SeedsTimestamp + Config->RetryBackoffTime);
        }

        return GetSeedsPromise;
    }

    void DiscardSeeds(TAsyncGetSeedsResult result)
    {
        YCHECK(result);
        YCHECK(result.IsSet());

        TGuard<TSpinLock> guard(SpinLock);

        if (!Config->AllowFetchingSeedsFromMaster) {
            // We're not allowed to ask master for seeds.
            // Better keep the initial ones.
            return;
        }

        if (GetSeedsPromise.ToFuture() != result) {
            return;
        }

        YCHECK(GetSeedsPromise.IsSet());
        GetSeedsPromise.Reset();
    }

    void LocateChunk()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Requesting chunk seeds from master");

        auto req = ChunkServiceProxy.LocateChunks();
        ToProto(req->add_chunk_ids(), ChunkId);
        req->Invoke().Subscribe(
            BIND(&TReplicationReader::OnLocateChunkResponse, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnLocateChunkResponse(TChunkServiceProxy::TRspLocateChunksPtr rsp)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YCHECK(GetSeedsPromise);

        {
            TGuard<TSpinLock> guard(SpinLock);
            SeedsTimestamp = TInstant::Now();
        }

        if (!rsp->IsOK()) {
            YCHECK(!GetSeedsPromise.IsSet());
            GetSeedsPromise.Set(rsp->GetError());
            return;
        }

        YCHECK(rsp->chunks_size() <= 1);
        if (rsp->chunks_size() == 0) {
            YCHECK(!GetSeedsPromise.IsSet());
            GetSeedsPromise.Set(TError("No such chunk %s", ~ToString(ChunkId)));
            return;
        }
        const auto& chunkInfo = rsp->chunks(0);

        NodeDirectory->MergeFrom(rsp->node_directory());
        auto seedReplicas = FromProto<TChunkReplica, TChunkReplicaList>(chunkInfo.replicas());

        // TODO(babenko): use std::random_shuffle here but make sure it uses true randomness.
        Shuffle(seedReplicas.begin(), seedReplicas.end());

        LOG_INFO("Chunk seeds received (SeedReplicas: [%s])",
            ~JoinToString(seedReplicas, TChunkReplicaAddressFormatter(NodeDirectory)));

        YCHECK(!GetSeedsPromise.IsSet());
        GetSeedsPromise.Set(seedReplicas);
    }

};

///////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public TRefCounted
{
protected:
    //! Reference to the owning reader.
    TWeakPtr<TReplicationReader> Reader;

    //! Translates node ids to node descriptors.
    TNodeDirectoryPtr NodeDirectory;

    //! Zero based retry index (less than |Reader->Config->RetryCount|).
    int RetryIndex;

    //! Zero based pass index (less than |Reader->Config->PassCount|).
    int PassIndex;

    //! Seed replicas for the current retry.
    TChunkReplicaList SeedReplicas;

    //! Set of peer addresses corresponding to SeedReplcias.
    yhash_set<Stroka> SeedAddresses;

    //! Set of peer addresses banned for the current retry.
    yhash_set<Stroka> BannedPeers;

    //! List of candidates to try.
    std::vector<TNodeDescriptor> PeerList;

    //! Current index in #PeerList.
    int PeerIndex;

    //! The instant this session has started.
    TInstant StartTime;

    NLog::TTaggedLogger Logger;


    explicit TSessionBase(TReplicationReader* reader)
        : Reader(reader)
        , NodeDirectory(reader->NodeDirectory)
        , RetryIndex(0)
        , PassIndex(0)
        , PeerIndex(0)
        , StartTime(TInstant::Now())
        , Logger(ChunkReaderLogger)
    {
        Logger.AddTag(Sprintf("ChunkId: %s", ~ToString(reader->ChunkId)));
    }

    void BanPeer(const Stroka& address)
    {
        if (BannedPeers.insert(address).second) {
            LOG_INFO("Node is banned for the current retry (Address: %s)",
                ~address);
        }
    }

    bool IsPeerBanned(const Stroka& address)
    {
        return BannedPeers.find(address) != BannedPeers.end();
    }

    bool IsSeed(const Stroka& address)
    {
        return SeedAddresses.find(address) != SeedAddresses.end();
    }

    virtual void NextRetry()
    {
        auto reader = Reader.Lock();
        if (!reader) {
            return;
        }

        YCHECK(!GetSeedsResult);

        LOG_INFO("Retry started: %d of %d",
            RetryIndex + 1,
            reader->Config->RetryCount);

        GetSeedsResult = reader->AsyncGetSeeds();
        GetSeedsResult.Subscribe(
            BIND(&TSessionBase::OnGotSeeds, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));

        PassIndex = 0;
        BannedPeers.clear();
    }

    virtual void NextPass() = 0;

    void OnRetryFailed()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        int retryCount = reader->Config->RetryCount;
        LOG_INFO("Retry failed: %d of %d",
            RetryIndex + 1,
            retryCount);

        YCHECK(GetSeedsResult);
        reader->DiscardSeeds(GetSeedsResult);
        GetSeedsResult.Reset();

        ++RetryIndex;
        if (RetryIndex >= retryCount) {
            OnSessionFailed();
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TSessionBase::NextRetry, MakeStrong(this))
            .Via(TDispatcher::Get()->GetReaderInvoker()),
            reader->Config->RetryBackoffTime);
    }


    template <class TSeedHandler>
    bool PrepareNextPass(const TSeedHandler& seedHandler)
    {
        auto reader = Reader.Lock();
        if (!reader)
            return false;

        LOG_INFO("Pass started: %d of %d",
            PassIndex + 1,
            reader->Config->PassCount);

        PeerList.clear();
        PeerIndex = 0;
        FOREACH (auto replica, SeedReplicas) {
            const auto& descriptor = NodeDirectory->GetDescriptor(replica);
            if (!IsPeerBanned(descriptor.Address)) {
                seedHandler(descriptor);
            }
        }

        if (PeerList.empty()) {
            LOG_INFO("No feasible seeds to start a pass");
            OnRetryFailed();
            return false;
        }

        return true;
    }

    void OnPassCompleted()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        int passCount = reader->Config->PassCount;
        LOG_INFO("Pass completed: %d of %d",
            PassIndex + 1,
            passCount);

        ++PassIndex;
        if (PassIndex >= passCount) {
            OnRetryFailed();
            return;
        }

        auto backoffTime = reader->Config->MinPassBackoffTime *
            pow(reader->Config->PassBackoffTimeMultiplier, PassIndex - 1);

        backoffTime = std::min(backoffTime, reader->Config->MaxPassBackoffTime);

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
        error.InnerErrors() = InnerErrors;
        return error;
    }

    virtual void OnSessionFailed() = 0;

private:
    //! Errors collected by the session.
    std::vector<TError> InnerErrors;

    TReplicationReader::TAsyncGetSeedsResult GetSeedsResult;

    void OnGotSeeds(TReplicationReader::TGetSeedsResult result)
    {
        auto reader = Reader.Lock();
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

        SeedReplicas = result.Value();
        if (SeedReplicas.empty()) {
            RegisterError(TError("Chunk is lost"));
            OnRetryFailed();
            return;
        }

        SeedAddresses.clear();
        FOREACH (auto replica, SeedReplicas) {
            const auto& descriptor = NodeDirectory->GetDescriptor(replica.GetNodeId());
            SeedAddresses.insert(descriptor.Address);
        }

        // Prefer local node if in seeds.
        for (auto it = SeedReplicas.begin(); it != SeedReplicas.end(); ++it) {
            const auto& descriptor = reader->NodeDirectory->GetDescriptor(*it);
            if (descriptor.IsLocal()) {
                auto localSeed = *it;
                SeedReplicas.erase(it);
                SeedReplicas.insert(SeedReplicas.begin(), localSeed);
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
        , Promise(NewPromise<IAsyncReader::TReadResult>())
        , BlockIndexes(blockIndexes)
    {
        Logger.AddTag(Sprintf("ReadSession: %p", this));
    }

    ~TReadSession()
    {
        if (!Promise.IsSet()) {
            Promise.Set(TError("Reader terminated"));
        }
    }

    IAsyncReader::TAsyncReadResult Run()
    {
        FetchBlocksFromCache();

        if (GetUnfetchedBlockIndexes().empty()) {
            LOG_INFO("All chunk blocks are fetched from cache");
            OnSessionSucceeded();
        } else {
            NextRetry();
        }

        return Promise;
    }

private:
    //! Promise representing the session.
    IAsyncReader::TAsyncReadPromise Promise;

    //! Block indexes to read during the session.
    std::vector<int> BlockIndexes;

    //! Blocks that are fetched so far.
    yhash_map<int, TSharedRef> FetchedBlocks;

    struct TPeerBlocksInfo
    {
        TNodeDescriptor NodeDescriptor;
        yhash_set<int> BlockIndexes;
    };

    //! Known peers and their blocks (address -> TPeerBlocksInfo).
    yhash_map<Stroka, TPeerBlocksInfo> PeerBlocksMap;

    virtual void NextPass() override
    {
        PeerBlocksMap.clear();

        auto seedHandler = [&] (const TNodeDescriptor& descriptor) {
            FOREACH (int blockIndex, BlockIndexes) {
                AddPeer(descriptor, blockIndex);
            }
        };

        if (!PrepareNextPass(seedHandler))
            return;

        RequestBlocks();
    }

    void AddPeer(const TNodeDescriptor& nodeDescriptor, int blockIndex)
    {
        const auto& address = nodeDescriptor.Address;
        auto peerBlocksMapIt = PeerBlocksMap.find(address);
        if (peerBlocksMapIt == PeerBlocksMap.end()) {
            peerBlocksMapIt = PeerBlocksMap.insert(std::make_pair(address, TPeerBlocksInfo())).first;
            PeerList.push_back(nodeDescriptor);
        }
        peerBlocksMapIt->second.BlockIndexes.insert(blockIndex);
    }

    TNodeDescriptor PickNextPeer()
    {
        // When the time comes to fetch from a non-seeding node, pick a random one.
        if (PeerIndex >= SeedReplicas.size()) {
            size_t count = PeerList.size() - PeerIndex;
            size_t randomIndex = PeerIndex + RandomNumber(count);
            std::swap(PeerList[PeerIndex], PeerList[randomIndex]);
        }
        return PeerList[PeerIndex++];
    }

    std::vector<int> GetUnfetchedBlockIndexes()
    {
        std::vector<int> result;
        result.reserve(BlockIndexes.size());
        FOREACH (int blockIndex, BlockIndexes) {
            if (FetchedBlocks.find(blockIndex) == FetchedBlocks.end()) {
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

        auto peerBlocksMapIt = PeerBlocksMap.find(nodeDescriptor.Address);
        YCHECK(peerBlocksMapIt != PeerBlocksMap.end());

        const auto& blocksInfo = peerBlocksMapIt->second;

        FOREACH (int blockIndex, indexesToFetch) {
            if (blocksInfo.BlockIndexes.find(blockIndex) != blocksInfo.BlockIndexes.end()) {
                result.push_back(blockIndex);
            }
        }

        return result;
    }


    void FetchBlocksFromCache()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        FOREACH (int blockIndex, BlockIndexes) {
            if (FetchedBlocks.find(blockIndex) == FetchedBlocks.end()) {
                TBlockId blockId(reader->ChunkId, blockIndex);
                auto block = reader->BlockCache->Find(blockId);
                if (block) {
                    LOG_INFO("Block is fetched from cache (Block: %d)", blockIndex);
                    YCHECK(FetchedBlocks.insert(std::make_pair(blockIndex, block)).second);
                }
            }
        }
    }


    void RequestBlocks()
    {
        auto reader = Reader.Lock();
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

            if (PeerIndex >= PeerList.size()) {
                OnPassCompleted();
                break;
            }

            auto currentDescriptor = PickNextPeer();
            auto blockIndexes = GetRequestBlockIndexes(currentDescriptor, unfetchedBlockIndexes);

            if (!IsPeerBanned(currentDescriptor.Address) && !blockIndexes.empty()) {
                LOG_INFO("Requesting blocks from peer (Address: %s, Blocks: [%s])",
                    ~currentDescriptor.Address,
                    ~JoinToString(unfetchedBlockIndexes));

                IChannelPtr channel;
                try {
                    channel = LightNodeChannelCache->GetChannel(currentDescriptor.Address);
                } catch (const std::exception& ex) {
                    RegisterError(ex);
                    continue;
                }

                TDataNodeServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(reader->Config->BlockRpcTimeout);

                auto request = proxy.GetBlocks();
                request->SetStartTime(StartTime);
                ToProto(request->mutable_chunk_id(), reader->ChunkId);
                ToProto(request->mutable_block_indexes(), unfetchedBlockIndexes);
                request->set_enable_caching(reader->Config->EnableNodeCaching);
                request->set_session_type(reader->SessionType);
                if (reader->LocalDescriptor) {
                    auto expirationTime = TInstant::Now() + reader->Config->PeerExpirationTimeout;
                    ToProto(request->mutable_peer_descriptor(), reader->LocalDescriptor.Get());
                    request->set_peer_expiration_time(expirationTime.GetValue());
                }

                request->Invoke().Subscribe(
                    BIND(
                        &TReadSession::OnGetBlocksResponse,
                        MakeStrong(this),
                        currentDescriptor,
                        request)
                    .Via(TDispatcher::Get()->GetReaderInvoker()));
                break;
            }

            LOG_INFO("Skipping peer (Address: %s)", ~currentDescriptor.Address);
        }
    }

    void OnGetBlocksResponse(
        const TNodeDescriptor& requestedDescriptor,
        TDataNodeServiceProxy::TReqGetBlocksPtr req,
        TDataNodeServiceProxy::TRspGetBlocksPtr rsp)
    {
        const auto& requestedAddress = requestedDescriptor.Address;
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
        auto reader = Reader.Lock();
        if (!reader) {
            return MakeFuture();
        }

        const auto& requestedAddress = requestedDescriptor.Address;
        LOG_INFO("Started processing block response (Address: %s)", ~requestedAddress);

        size_t blockCount = req->block_indexes_size();
        YCHECK(rsp->blocks_size() == blockCount);
        YCHECK(rsp->Attachments().size() == blockCount);

        i64 totalSize = 0;

        for (int index = 0; index < static_cast<int>(blockCount); ++index) {
            int blockIndex = req->block_indexes(index);
            TBlockId blockId(reader->ChunkId, blockIndex);
            const auto& blockInfo = rsp->blocks(index);
            if (blockInfo.data_attached()) {
                LOG_INFO("Block data received (Block: %d)",
                    blockIndex);
                auto block = rsp->Attachments()[index];
                YCHECK(block);

                // Only keep source address if P2P is on.
                auto source = reader->LocalDescriptor
                    ? TNullable<TNodeDescriptor>(requestedDescriptor)
                    : TNullable<TNodeDescriptor>(Null);
                reader->BlockCache->Put(blockId, block, source);

                YCHECK(FetchedBlocks.insert(std::make_pair(blockIndex, block)).second);
                totalSize += block.Size();
            } else if (reader->Config->FetchFromPeers) {
                FOREACH (const auto& protoP2PDescriptor, blockInfo.p2p_descriptors()) {
                    auto p2pDescriptor= FromProto<TNodeDescriptor>(protoP2PDescriptor);
                    AddPeer(p2pDescriptor, blockIndex);
                    LOG_INFO("P2P descriptor received (Block: %d, Address: %s)",
                        blockIndex,
                        ~p2pDescriptor.Address);
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

        return reader->Throttler->Throttle(totalSize);
    }


    void OnSessionSucceeded()
    {
        LOG_INFO("All chunk blocks are fetched");

        std::vector<TSharedRef> blocks;
        blocks.reserve(BlockIndexes.size());
        FOREACH (int blockIndex, BlockIndexes) {
            auto block = FetchedBlocks[blockIndex];
            YCHECK(block);
            blocks.push_back(block);
        }
        Promise.Set(IAsyncReader::TReadResult(blocks));
    }

    virtual void OnSessionFailed() override
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        auto error = BuildCombinedError(TError(
            "Error fetching blocks for chunk %s",
            ~ToString(reader->ChunkId)));
        Promise.Set(error);
    }
};

TReplicationReader::TAsyncReadResult TReplicationReader::AsyncReadBlocks(const std::vector<int>& blockIndexes)
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
        , Promise(NewPromise<IAsyncReader::TGetMetaResult>())
        , PartitionTag(partitionTag)
    {
        if (extensionTags) {
            ExtensionTags = *extensionTags;
            AllExtensionTags = false;
        } else {
            AllExtensionTags = true;
        }

        Logger.AddTag(Sprintf("GetMetaSession: %p", this));
    }

    ~TGetMetaSession()
    {
        if (!Promise.IsSet()) {
            Promise.Set(TError("Reader terminated"));
        }
    }

    IAsyncReader::TAsyncGetMetaResult Run()
    {
        NextRetry();
        return Promise;
    }

private:
    //! Promise representing the session.
    IAsyncReader::TAsyncGetMetaPromise Promise;

    std::vector<int> ExtensionTags;
    TNullable<int> PartitionTag;
    bool AllExtensionTags;


    virtual void NextPass()
    {
        auto seedHandler = [&] (const TNodeDescriptor& descriptor) {
            PeerList.push_back(descriptor);
        };

        if (!PrepareNextPass(seedHandler))
            return;

        RequestInfo();
    }

    void RequestInfo()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        if (PeerIndex >= PeerList.size()) {
            OnPassCompleted();
            return;
        }

        const auto& descriptor = PeerList[PeerIndex];
        const auto& address = descriptor.Address;

        LOG_INFO("Requesting chunk meta (Address: %s)", ~address);

        IChannelPtr channel;
        try {
            channel = LightNodeChannelCache->GetChannel(address);
        } catch (const std::exception& ex) {
            OnGetChunkMetaResponseFailed(descriptor, ex);
            return;
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(reader->Config->MetaRpcTimeout);

        auto request = proxy.GetChunkMeta();
        request->SetStartTime(StartTime);
        ToProto(request->mutable_chunk_id(), reader->ChunkId);
        request->set_all_extension_tags(AllExtensionTags);

        if (PartitionTag) {
            request->set_partition_tag(PartitionTag.Get());
        }

        ToProto(request->mutable_extension_tags(), ExtensionTags);
        request->Invoke().Subscribe(
            BIND(&TGetMetaSession::OnGetChunkMetaResponse, MakeStrong(this), descriptor)
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnGetChunkMetaResponse(
        const TNodeDescriptor& nodeDescriptor,
        TDataNodeServiceProxy::TRspGetChunkMetaPtr rsp)
    {
        if (!rsp->IsOK()) {
            OnGetChunkMetaResponseFailed(nodeDescriptor, *rsp);
            return;
        }

        OnSessionSucceeded(rsp->chunk_meta());
    }

    void OnGetChunkMetaResponseFailed(
        const TNodeDescriptor& descriptor,
        const TError& error)
    {
        const auto& address = descriptor.Address;

        LOG_WARNING(error, "Error requesting chunk meta (Address: %s)",
            ~address);

        RegisterError(error);

        ++PeerIndex;
        if (error.GetCode() !=  NRpc::EErrorCode::Unavailable) {
            BanPeer(address);
        }

        RequestInfo();
    }


    void OnSessionSucceeded(const NProto::TChunkMeta& chunkMeta)
    {
        LOG_INFO("Chunk meta obtained");
        Promise.Set(IAsyncReader::TGetMetaResult(chunkMeta));
    }

    virtual void OnSessionFailed() override
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        auto error = BuildCombinedError(TError(
            "Error fetching meta for chunk %s",
            ~ToString(reader->ChunkId)));
        Promise.Set(error);
    }

};

TReplicationReader::TAsyncGetMetaResult TReplicationReader::AsyncGetChunkMeta(
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

IAsyncReaderPtr CreateReplicationReader(
    TReplicationReaderConfigPtr config,
    IBlockCachePtr blockCache,
    NRpc::IChannelPtr masterChannel,
    TNodeDirectoryPtr nodeDirectory,
    const TNullable<TNodeDescriptor>& localDescriptor,
    const TChunkId& chunkId,
    const TChunkReplicaList& seedReplicas,
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
        sessionType,
        throttler);
    reader->Initialize();
    return reader;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
