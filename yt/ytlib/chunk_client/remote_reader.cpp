#include "stdafx.h"
#include "config.h"
#include "remote_reader.h"
#include "async_reader.h"
#include "block_cache.h"
#include "private.h"
#include "block_id.h"
#include "chunk_ypath_proxy.h"
#include "data_node_service_proxy.h"
#include "dispatcher.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/address.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/cypress_client/cypress_ypath_proxy.h>

#include <util/random/shuffle.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NObjectClient;
using namespace NCypressClient;
using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

class TSessionBase;
class TReadSession;
class TGetMetaSession;

class TRemoteReader
    : public IAsyncReader
{
public:
    typedef TValueOrError< std::vector<Stroka> > TGetSeedsResult;
    typedef TFuture<TGetSeedsResult> TAsyncGetSeedsResult;
    typedef TPromise<TGetSeedsResult> TAsyncGetSeedsPromise;

    TRemoteReader(
        TRemoteReaderConfigPtr config,
        IBlockCachePtr blockCache,
        IChannelPtr masterChannel,
        const TChunkId& chunkId,
        const std::vector<Stroka>& seedAddresses)
        : Config(config)
        , BlockCache(blockCache)
        , ChunkId(chunkId)
        , Logger(ChunkReaderLogger)
        , ObjectProxy(masterChannel)
        , InitialSeedAddresses(seedAddresses)
        , SeedsTimestamp(TInstant::Zero())
    {
        Logger.AddTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));
    }

    void Initialize()
    {
        if (!Config->AllowFetchingSeedsFromMaster && InitialSeedAddresses.empty()) {
            THROW_ERROR_EXCEPTION(
                "Reader is unusable: master seeds retries are disabled and no initial seeds are given (ChunkId: %s)",
                ~ChunkId.ToString());
        }

        if (!InitialSeedAddresses.empty()) {
            GetSeedsPromise = MakePromise(TGetSeedsResult(InitialSeedAddresses));
        }

        LOG_INFO("Reader initialized (InitialSeedAddresses: [%s], FetchPromPeers: %s, PublishPeer: %s, EnableCaching: %s)",
            ~JoinToString(InitialSeedAddresses),
            ~ToString(Config->FetchFromPeers),
            ~ToString(Config->PublishPeer),
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

    TRemoteReaderConfigPtr Config;
    IBlockCachePtr BlockCache;
    TChunkId ChunkId;
    NLog::TTaggedLogger Logger;

    TObjectServiceProxy ObjectProxy;

    TSpinLock SpinLock;
    std::vector<Stroka> InitialSeedAddresses;
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
            TDelayedInvoker::Submit(
                BIND(&TRemoteReader::LocateChunk, MakeStrong(this))
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

        auto req = TChunkYPathProxy::Locate(FromObjectId(ChunkId));
        ObjectProxy.Execute(req).Subscribe(
            BIND(&TRemoteReader::OnLocateChunkResponse, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnLocateChunkResponse(TChunkYPathProxy::TRspLocatePtr rsp)
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

        auto seedAddresses = FromProto<Stroka>(rsp->node_addresses());

        // TODO(babenko): use std::random_shuffle here but make sure it uses true randomness.
        Shuffle(seedAddresses.begin(), seedAddresses.end());

        LOG_INFO("Chunk seeds received (SeedAddresses: [%s])", ~JoinToString(seedAddresses));

        YCHECK(!GetSeedsPromise.IsSet());
        GetSeedsPromise.Set(seedAddresses);
    }

};

///////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public TRefCounted
{
protected:
    //! Reference to the owning reader.
    TWeakPtr<TRemoteReader> Reader;

    //! Zero based retry index (less than |Reader->Config->RetryCount|).
    int RetryIndex;

    //! Zero based pass index (less than |Reader->Config->PassCount|).
    int PassIndex;

    NLog::TTaggedLogger Logger;

    //! Seed addresses for the current retry.
    std::vector<Stroka> SeedAddresses;


    explicit TSessionBase(TRemoteReader* reader)
        : Reader(reader)
        , RetryIndex(0)
        , PassIndex(0)
        , Logger(ChunkReaderLogger)
    {
        Logger.AddTag(Sprintf("ChunkId: %s", ~reader->ChunkId.ToString()));
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
    }

    virtual void NextPass()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        LOG_INFO("Pass started: %d of %d",
            PassIndex + 1,
            reader->Config->PassCount);
    }


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
        
        TDelayedInvoker::Submit(
            BIND(&TSessionBase::NextRetry, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()),
            reader->Config->RetryBackoffTime);
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

        TDelayedInvoker::Submit(
            BIND(&TSessionBase::NextPass, MakeStrong(this))
                .Via(TDispatcher::Get()->GetReaderInvoker()),
            reader->Config->PassBackoffTime);
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

    TRemoteReader::TAsyncGetSeedsResult GetSeedsResult;


    void OnGotSeeds(TRemoteReader::TGetSeedsResult result)
    {
        if (!result.IsOK()) {
            RegisterError(TError("Error requesting seeds from master")
                << result);
            OnSessionFailed();
            return;
        }

        SeedAddresses = result.Value();
        if (SeedAddresses.empty()) {
            RegisterError(TError("Chunk is lost"));
            OnRetryFailed();
            return;
        }

        // Prefer local node if in seeds.
        for (auto it = SeedAddresses.begin(); it != SeedAddresses.end(); ++it) {
            if (GetServiceHostName(*it) == TAddressResolver::Get()->GetLocalHostName()) {
                auto localSeed = *it;
                SeedAddresses.erase(it);
                SeedAddresses.insert(SeedAddresses.begin(), localSeed);
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
    TReadSession(TRemoteReader* reader, const std::vector<int>& blockIndexes)
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
        yhash_set<int> BlockIndexes;
    };

    //! Known peers and their blocks (peer address -> TPeerBlocksInfo).
    yhash_map<Stroka, TPeerBlocksInfo> PeerBlocksMap;

    //! List of candidates to try.
    std::vector<Stroka> PeerAddressList;

    //! Current index in #PeerAddressList.
    int PeerIndex;

    //! Set of peers banned for the current retry.
    yhash_set<Stroka> BannedPeerAddresses;


    virtual void NextRetry() override
    {
        TSessionBase::NextRetry();

        BannedPeerAddresses.clear();
    }

    virtual void NextPass() override
    {
        TSessionBase::NextPass();

        PeerAddressList.clear();
        PeerBlocksMap.clear();
        PeerIndex = 0;

        // Mark the seeds as having all blocks.
        FOREACH (const auto& address, SeedAddresses) {
            if (!IsPeerBanned(address)) {
                FOREACH (int blockIndex, BlockIndexes) {
                    AddPeer(address, blockIndex);
                }
            }
        }

        if (PeerAddressList.empty()) {
            LOG_INFO("No feasible seeds to start a pass");
            OnRetryFailed();
            return;
        }

        RequestBlocks();
    }


    void AddPeer(const Stroka& address, int blockIndex)
    {
        auto peerBlocksMapIt = PeerBlocksMap.find(address);
        if (peerBlocksMapIt == PeerBlocksMap.end()) {
            peerBlocksMapIt = PeerBlocksMap.insert(std::make_pair(address, TPeerBlocksInfo())).first;
            PeerAddressList.push_back(address);
        }
        peerBlocksMapIt->second.BlockIndexes.insert(blockIndex);
    }

    Stroka PickNextPeer()
    {
        // When the time comes to fetch from a non-seeding node, pick a random one.
        if (PeerIndex >= SeedAddresses.size()) {
            size_t count = PeerAddressList.size() - PeerIndex;
            size_t randomIndex = PeerIndex + RandomNumber(count);
            std::swap(PeerAddressList[PeerIndex], PeerAddressList[randomIndex]);
        }
        return PeerAddressList[PeerIndex++];
    }


    void BanPeer(const Stroka& address)
    {
        if (BannedPeerAddresses.insert(address).second) {
            LOG_INFO("Peer %s is banned for the current retry", ~address);
        }
    }

    bool IsPeerBanned(const Stroka& address)
    {
        return BannedPeerAddresses.find(address) != BannedPeerAddresses.end();
    }

    bool IsSeed(const Stroka& address)
    {
        return std::find(SeedAddresses.begin(), SeedAddresses.end(), address) != SeedAddresses.end();
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
        const Stroka& address,
        const std::vector<int>& indexesToFetch)
    {
        std::vector<int> result;
        result.reserve(indexesToFetch.size());

        auto peerBlocksMapIt = PeerBlocksMap.find(address);
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
                    LOG_INFO("Block %d is fetched from cache", blockIndex);
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

        while (true) {
            FetchBlocksFromCache();

            auto unfetchedBlockIndexes = GetUnfetchedBlockIndexes();
            if (unfetchedBlockIndexes.empty()) {
                OnSessionSucceeded();
                break;
            }

            if (PeerIndex >= PeerAddressList.size()) {
                OnPassCompleted();
                break;
            }

            auto address = PickNextPeer();
            auto blockIndexes = GetRequestBlockIndexes(address, unfetchedBlockIndexes);

            if (!IsPeerBanned(address) && !blockIndexes.empty()) {
                LOG_INFO("Requesting blocks from peer %s (BlockIndexes: [%s])",
                    ~address,
                    ~JoinToString(unfetchedBlockIndexes));

                IChannelPtr channel;
                try {
                    channel = NodeChannelCache->GetChannel(address);
                } catch (const std::exception& ex) {
                    RegisterError(ex);
                    continue;
                }

                TDataNodeServiceProxy proxy(channel);
                proxy.SetDefaultTimeout(reader->Config->NodeRpcTimeout);

                auto request = proxy.GetBlocks();
                *request->mutable_chunk_id() = reader->ChunkId.ToProto();
                ToProto(request->mutable_block_indexes(), unfetchedBlockIndexes);
                request->set_enable_caching(reader->Config->EnableNodeCaching);
                if (reader->Config->PublishPeer) {
                    request->set_peer_address(reader->Config->PeerAddress);
                    request->set_peer_expiration_time((TInstant::Now() + reader->Config->PeerExpirationTimeout).GetValue());
                }

                request->Invoke().Subscribe(
                    BIND(
                        &TReadSession::OnGetBlocksResponse,
                        MakeStrong(this),
                        address,
                        request)
                    .Via(TDispatcher::Get()->GetReaderInvoker()));
                break;
            }

            LOG_INFO("Skipping peer %s", ~address);
        }
    }

    void OnGetBlocksResponse(
        const Stroka& address,
        TDataNodeServiceProxy::TReqGetBlocksPtr request,
        TDataNodeServiceProxy::TRspGetBlocksPtr response)
    {
        if (response->IsOK()) {
            ProcessResponse(address, request, response);
        } else {
            RegisterError(TError("Error fetching blocks from %s",
                ~address)
                << *response);
            BanPeer(address);
        }

        RequestBlocks();
    }

    void ProcessResponse(
        const Stroka& address,
        TDataNodeServiceProxy::TReqGetBlocksPtr req,
        TDataNodeServiceProxy::TRspGetBlocksPtr rsp)
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        LOG_INFO("Response received from %s", ~address);

        size_t blockCount = req->block_indexes_size();
        YCHECK(rsp->blocks_size() == blockCount);
        YCHECK(rsp->Attachments().size() == blockCount);

        for (int index = 0; index < static_cast<int>(blockCount); ++index) {
            int blockIndex = req->block_indexes(index);
            TBlockId blockId(reader->ChunkId, blockIndex);
            const auto& blockInfo = rsp->blocks(index);
            if (blockInfo.data_attached()) {
                LOG_INFO("Received data for block %d",
                    blockIndex);
                auto block = rsp->Attachments()[index];
                YCHECK(block);

                // Only keep source address if PublishPeer is on.
                auto sourceAddress =
                    reader->Config->PublishPeer
                    ? TNullable<Stroka>(address)
                    : TNullable<Stroka>(Null);
                reader->BlockCache->Put(blockId, block, sourceAddress);

                YCHECK(FetchedBlocks.insert(std::make_pair(blockIndex, block)).second);
            } else if (reader->Config->FetchFromPeers) {
                FOREACH (const auto& peerAddress, blockInfo.peer_addresses()) {
                    LOG_INFO("Received peer info for block %d (Address: %s, PeerAddress: %s)",
                        blockIndex,
                        ~address,
                        ~peerAddress);
                    AddPeer(peerAddress, blockIndex);
                }
            }
        }

        if (IsSeed(address) && !rsp->has_complete_chunk()) {
            LOG_INFO("Seed %s does not contain the chunk",
                ~address);
            BanPeer(address);
        }
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
            ~reader->ChunkId.ToString()));
        Promise.Set(error);
    }
};

TRemoteReader::TAsyncReadResult TRemoteReader::AsyncReadBlocks(const std::vector<int>& blockIndexes)
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
        TRemoteReader* reader,
        const TNullable<int> partitionTag,
        const std::vector<int>* extensionTags)
        : TSessionBase(reader)
        , Promise(NewPromise<IAsyncReader::TGetMetaResult>())
        , SeedIndex(0)
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

    //! Current index in #SeedAddresses.
    int SeedIndex;

    std::vector<int> ExtensionTags;
    TNullable<int> PartitionTag;
    bool AllExtensionTags;


    virtual void NextPass()
    {
        TSessionBase::NextPass();
        SeedIndex = 0;
        RequestInfo();
    }


    void RequestInfo()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        const auto& address = SeedAddresses[SeedIndex];

        LOG_INFO("Requesting chunk meta from %s", ~address);

        IChannelPtr channel;
        try {
            channel = NodeChannelCache->GetChannel(address);
        } catch (const std::exception& ex) {
            OnGetChunkMetaResponseFailed(address, ex);
            return;
        }

        TDataNodeServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(reader->Config->NodeRpcTimeout);

        auto request = proxy.GetChunkMeta();
        *request->mutable_chunk_id() = reader->ChunkId.ToProto();
        request->set_all_extension_tags(AllExtensionTags);

        if (PartitionTag) {
            request->set_partition_tag(PartitionTag.Get());
        }

        ToProto(request->mutable_extension_tags(), ExtensionTags);
        request->Invoke().Subscribe(
            BIND(&TGetMetaSession::OnGetChunkMetaResponse, MakeStrong(this), address)
                .Via(TDispatcher::Get()->GetReaderInvoker()));
    }

    void OnGetChunkMetaResponse(
        const Stroka& address,
        TDataNodeServiceProxy::TRspGetChunkMetaPtr rsp)
    {
        if (!rsp->IsOK()) {
            OnGetChunkMetaResponseFailed(address, *rsp);
            return;
        }

        OnSessionSucceeded(rsp->chunk_meta());
    }

    void OnGetChunkMetaResponseFailed(
        const Stroka& address,
        const TError& error)
    {
        LOG_WARNING(error, "Error requesting chunk meta from %s",
            ~address);

        RegisterError(error);

        ++SeedIndex;
        if (SeedIndex >= SeedAddresses.size()) {
            OnRetryFailed();
            return;
        }

        RequestInfo();
    }


    void OnSessionSucceeded(const TChunkMeta& chunkMeta)
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
            "Error getting meta for chunk %s",
            ~reader->ChunkId.ToString()));
        Promise.Set(error);
    }

};

TRemoteReader::TAsyncGetMetaResult TRemoteReader::AsyncGetChunkMeta(
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

IAsyncReaderPtr CreateRemoteReader(
    TRemoteReaderConfigPtr config,
    IBlockCachePtr blockCache,
    NRpc::IChannelPtr masterChannel,
    const TChunkId& chunkId,
    const std::vector<Stroka>& seedAddresses)
{
    YCHECK(config);
    YCHECK(blockCache);
    YCHECK(masterChannel);

    auto reader = New<TRemoteReader>(
        config,
        blockCache,
        masterChannel,
        chunkId,
        seedAddresses);
    reader->Initialize();
    return reader;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
