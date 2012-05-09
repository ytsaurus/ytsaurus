#include "stdafx.h"
#include "config.h"
#include "remote_reader.h"
#include "async_reader.h"
#include "block_cache.h"
#include "holder_channel_cache.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/string.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/logging/tagged_logger.h>
#include <ytlib/chunk_server/block_id.h>
#include <ytlib/chunk_server/chunk_ypath_proxy.h>
#include <ytlib/chunk_server/chunk_service_proxy.h>
#include <ytlib/chunk_holder/chunk_holder_service_proxy.h>
#include <ytlib/object_server/object_service_proxy.h>
#include <ytlib/cypress/cypress_ypath_proxy.h>

#include <util/random/shuffle.h>
#include <util/system/hostname.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NChunkHolder;
using namespace NChunkHolder::NProto;
using namespace NChunkServer;
using namespace NChunkServer::NProto;
using namespace NObjectServer;
using namespace NCypress;

///////////////////////////////////////////////////////////////////////////////

class TSessionBase;
class TReadSession;
class TGetMetaSession;

class TRemoteReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TRemoteReader> TPtr;
    typedef TRemoteReaderConfig TConfig;

    typedef TValueOrError< std::vector<Stroka> > TGetSeedsResult;
    typedef TFuture<TGetSeedsResult> TAsyncGetSeedsResult;
    typedef TPromise<TGetSeedsResult> TAsyncGetSeedsPromise;

    TRemoteReader(
        TRemoteReaderConfigPtr config,
        IBlockCachePtr blockCache,
        IChannel* masterChannel,
        const TChunkId& chunkId,
        const std::vector<Stroka>& seedAddresses)
        : Config(config)
        , BlockCache(blockCache)
        , ChunkId(chunkId)
        , Logger(ChunkClientLogger)
        , GetSeedsPromise(Null)
    {
        Logger.AddTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));

        LOG_INFO("Reader created (SeedAddresses: [%s], FetchPromPeers: %s, PublishPeer: %s)",
            ~JoinToString(seedAddresses),
            ~ToString(Config->FetchFromPeers),
            ~ToString(Config->PublishPeer));

        if (!seedAddresses.empty()) {
            GetSeedsPromise = MakePromise(TGetSeedsResult(seedAddresses));
        }

        ChunkProxy = new TChunkServiceProxy(masterChannel);
        ObjectProxy = new TObjectServiceProxy(masterChannel);
    }

    TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes);
    TAsyncGetMetaResult AsyncGetChunkMeta(const std::vector<i32>* tags = NULL);

    TAsyncGetSeedsResult AsyncGetSeeds()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        if (GetSeedsPromise.IsNull()) {
            LOG_INFO("Need fresh chunk seeds");
            GetSeedsPromise = NewPromise<TGetSeedsResult>();
            TDelayedInvoker::Submit(
                BIND(&TRemoteReader::DoFindChunk, MakeWeak(this)),
                SeedsTimestamp + Config->RetryBackoffTime);
        }

        return GetSeedsPromise;
    }

    void DiscardSeeds(TAsyncGetSeedsResult result)
    {
        YASSERT(!result.IsNull());
        YASSERT(result.IsSet());

        TGuard<TSpinLock> guard(SpinLock);

        if (GetSeedsPromise.ToFuture() != result) {
            return;
        }

        YASSERT(GetSeedsPromise.IsSet());
        GetSeedsPromise.Reset();
    }

private:
    friend class TSessionBase;
    friend class TReadSession;
    friend class TGetMetaSession;

    TRemoteReaderConfigPtr Config;
    IBlockCachePtr BlockCache;
    TChunkId ChunkId;
    NLog::TTaggedLogger Logger;

    TAutoPtr<TChunkServiceProxy> ChunkProxy;
    TAutoPtr<TObjectServiceProxy> ObjectProxy;

    TSpinLock SpinLock;
    TAsyncGetSeedsPromise GetSeedsPromise;
    TInstant SeedsTimestamp;

    void DoFindChunk()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Requesting chunk seeds from the master");

        auto req = TChunkYPathProxy::Fetch(FromObjectId(ChunkId));
        ObjectProxy
            ->Execute(req)
            .Subscribe(BIND(&TRemoteReader::OnChunkFetched, MakeWeak(this)));
    }

    void OnChunkFetched(TChunkYPathProxy::TRspFetch::TPtr rsp)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(!GetSeedsPromise.IsNull());

        {
            TGuard<TSpinLock> guard(SpinLock);
            SeedsTimestamp = TInstant::Now();
        }

        if (rsp->IsOK()) {
            auto seedAddresses = FromProto<Stroka>(rsp->holder_addresses());

            // TODO(babenko): use std::random_shuffle here but make sure it uses true randomness.
            Shuffle(seedAddresses.begin(), seedAddresses.end());

            if (seedAddresses.empty()) {
                LOG_WARNING("Chunk is lost");
            } else {
                LOG_INFO("Chunk seeds found (SeedAddresses: [%s])", ~JoinToString(seedAddresses));
            }

            YASSERT(!GetSeedsPromise.IsSet());
            GetSeedsPromise.Set(seedAddresses);
        } else {
            auto message = Sprintf("Error requesting chunk seeds from master\n%s",
                ~rsp->GetError().ToString());
            LOG_WARNING("%s", ~message);

            YASSERT(!GetSeedsPromise.IsSet());
            GetSeedsPromise.Set(TError(message));
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public TRefCounted
{
protected:
    typedef TIntrusivePtr<TSessionBase> TPtr;

    TWeakPtr<TRemoteReader> Reader;
    int RetryIndex;
    TRemoteReader::TAsyncGetSeedsResult GetSeedsResult;
    NLog::TTaggedLogger Logger;
    std::vector<Stroka> SeedAddresses;

    TSessionBase(TRemoteReader* reader)
        : Reader(reader)
        , RetryIndex(0)
        , Logger(ChunkClientLogger)
    {
        Logger.AddTag(Sprintf("ChunkId: %s", ~reader->ChunkId.ToString()));
    }

    virtual void NewRetry()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        YASSERT(GetSeedsResult.IsNull());

        LOG_INFO("New retry started (RetryIndex: %d)", RetryIndex);

        GetSeedsResult = reader->AsyncGetSeeds();
        GetSeedsResult.Subscribe(BIND(&TSessionBase::OnGetSeedsReply, MakeStrong(this)));
    }

    void OnGetSeedsReply(TRemoteReader::TGetSeedsResult result)
    {
        if (result.IsOK()) {
            SeedAddresses = result.Value();
            if (SeedAddresses.empty()) {
                OnRetryFailed(TError("Chunk is lost"));
            } else {
                OnGotSeeds();
            }
        } else {
            OnSessionFailed(TError("Retries have been aborted due to master error (RetryIndex: %d)\n%s",
                RetryIndex,
                ~result.ToString()));
        }
    }

    void OnRetryFailed(const TError& error)
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        LOG_WARNING("Retry failed (RetryIndex: %d)\n%s",
            RetryIndex,
            ~error.ToString());

        YASSERT(!GetSeedsResult.IsNull());
        reader->DiscardSeeds(GetSeedsResult);
        GetSeedsResult.Reset();

        if (RetryIndex < reader->Config->RetryCount) {
            ++RetryIndex;
            NewRetry();
        } else {
            OnSessionFailed(TError("All retries failed (RetryCount: %d)",
                RetryIndex));
        }
    }

    virtual void OnGotSeeds()
    {
        auto hostName = Sprintf("%s:", GetHostName());

        // Prefer local holder if in seeds.
        // ToDo(psushin): consider using more effective container?

        for (auto it = SeedAddresses.begin(); it != SeedAddresses.end(); ++it)
        {
            if (it->has_prefix(hostName)) {
                auto localSeed = *it;
                SeedAddresses.erase(it);
                SeedAddresses.insert(SeedAddresses.begin(), localSeed);
                break;
            }
        }
    }

    virtual void OnSessionFailed(const TError& error) = 0;

};

///////////////////////////////////////////////////////////////////////////////

class TReadSession
    : public TSessionBase
{
public:
    typedef TIntrusivePtr<TReadSession> TPtr;

    TReadSession(TRemoteReader* reader, const std::vector<int>& blockIndexes)
        : TSessionBase(reader)
        , Promise(NewPromise<IAsyncReader::TReadResult>())
        , BlockIndexes(blockIndexes)
    {
        Logger.AddTag(Sprintf("ReadSession: %p", this));

        FetchBlocksFromCache();

        if (GetUnfetchedBlockIndexes().empty()) {
            LOG_INFO("All chunk blocks are fetched from cache");
            OnSessionSucceeded();
            return;
        }

        NewRetry();
    }

    IAsyncReader::TAsyncReadResult GetAsyncResult() const
    {
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

    //! Current pass index.
    int PassIndex;

    //! Current index in #PeerAddressList.
    int PeerIndex;


    virtual void NewRetry()
    {
        PassIndex = 0;
        TSessionBase::NewRetry();
    }

    void NewPass()
    {
        LOG_INFO("New pass started (PassIndex: %d)", PassIndex);

        PeerAddressList.clear();
        PeerBlocksMap.clear();
        PeerIndex = 0;

        // Mark the seeds as having all blocks.
        FOREACH (const auto& address, SeedAddresses) {
            FOREACH (int blockIndex, BlockIndexes) {
                AddPeer(address, blockIndex);
            }
        }

        RequestPeer();
    }

    void AddPeer(const Stroka& address, int blockIndex)
    {
        auto peerBlocksMapIt = PeerBlocksMap.find(address);
        if (peerBlocksMapIt == PeerBlocksMap.end()) {
            peerBlocksMapIt = PeerBlocksMap.insert(MakePair(address, TPeerBlocksInfo())).first;
            PeerAddressList.push_back(address);
        }
        peerBlocksMapIt->second.BlockIndexes.insert(blockIndex);
    }

    Stroka PickNextPeer()
    {
        // When the time comes to fetch from a non-seeding holder, pick a random one.
        if (PeerIndex >= SeedAddresses.size()) {
            size_t count = PeerAddressList.size() - PeerIndex;
            size_t randomIndex = PeerIndex + RandomNumber(count);
            std::swap(PeerAddressList[PeerIndex], PeerAddressList[randomIndex]);
        }
        return PeerAddressList[PeerIndex++];
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

    std::vector<int> GetRequestBlockIndexes(const Stroka& address, const std::vector<int>& indexesToFetch)
    {
        std::vector<int> result;
        result.reserve(indexesToFetch.size());

        auto peerBlocksMapIt = PeerBlocksMap.find(address);
        YASSERT(peerBlocksMapIt != PeerBlocksMap.end());

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
                    LOG_INFO("Block is fetched from cache (BlockIndex: %d)", blockIndex);
                    YVERIFY(FetchedBlocks.insert(MakePair(blockIndex, block)).second);
                }
            }
        }
    }

    virtual void OnGotSeeds()
    {
        TSessionBase::OnGotSeeds();
        NewPass();
    }

    void RequestPeer()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        while (true) {
            FetchBlocksFromCache();

            auto unfetchedBlockIndexes = GetUnfetchedBlockIndexes();
            if (unfetchedBlockIndexes.empty()) {
                OnSessionSucceeded();
                return;
            }

            if (PeerIndex >= PeerAddressList.size()) {
                LOG_INFO("Pass completed (PassIndex: %d)", PassIndex);
                ++PassIndex;
                if (PassIndex >= reader->Config->PassCount) {
                    OnRetryFailed(TError("Unable to fetch all chunk blocks"));
                } else {
                    TDelayedInvoker::Submit(
                        BIND(&TReadSession::NewPass, MakeStrong(this)),
                        reader->Config->PassBackoffTime);
                }
                return;
            }

            auto address = PickNextPeer();

            auto requestBlockIndexes = GetRequestBlockIndexes(address, unfetchedBlockIndexes);
            if (!requestBlockIndexes.empty()) {
                LOG_INFO("Requesting blocks from peer (Address: %s, BlockIndexes: [%s])",
                    ~address,
                    ~JoinToString(unfetchedBlockIndexes));

                auto channel = HolderChannelCache->GetChannel(address);

                TChunkHolderServiceProxy proxy(~channel);
                proxy.SetDefaultTimeout(reader->Config->HolderRpcTimeout);

                auto request = proxy.GetBlocks();
                *request->mutable_chunk_id() = reader->ChunkId.ToProto();
                ToProto(request->mutable_block_indexes(), unfetchedBlockIndexes);
                if (reader->Config->PublishPeer) {
                    request->set_peer_address(reader->Config->PeerAddress);
                    request->set_peer_expiration_time((TInstant::Now() + reader->Config->PeerExpirationTimeout).GetValue());
                }

                request->Invoke().Subscribe(BIND(
                    &TReadSession::OnGotBlocks,
                    MakeStrong(this),
                    address,
                    request));
                return;
            }

            LOG_INFO("Skipping peer (Address: %s)", ~address);
        }
    }

    void OnGotBlocks(
        const Stroka& address,
        TChunkHolderServiceProxy::TReqGetBlocks::TPtr request,
        TChunkHolderServiceProxy::TRspGetBlocks::TPtr response)
    {
        if (response->IsOK()) {
            ProcessReceivedBlocks(address, ~request, ~response);
        } else {
            LOG_WARNING("Error getting blocks from peer (Address: %s)\n%s",
                ~address,
                ~response->GetError().ToString());
        }

        RequestPeer();
    }

    void ProcessReceivedBlocks(
        const Stroka& address,
        TChunkHolderServiceProxy::TReqGetBlocks* request,
        TChunkHolderServiceProxy::TRspGetBlocks* response)
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        size_t blockCount = request->block_indexes_size();
        YASSERT(response->blocks_size() == blockCount);
        YASSERT(response->Attachments().size() == blockCount);

        int receivedBlockCount = 0;
        int oldPeerCount = PeerAddressList.size();

        for (int index = 0; index < static_cast<int>(blockCount); ++index) {
            int blockIndex = request->block_indexes(index);
            TBlockId blockId(reader->ChunkId, blockIndex);
            const auto& blockInfo = response->blocks(index);
            if (blockInfo.data_attached()) {
                LOG_INFO("Block received (Address: %s, BlockIndex: %d)",
                    ~address,
                    blockIndex);
                auto block = response->Attachments()[index];
                YASSERT(block);
                
                // If we don't publish peer we should forget source address
                // to avoid updating peer in TPeerBlockUpdater.
                Stroka source;
                if (reader->Config->PublishPeer) {
                    source = address;
                }
                reader->BlockCache->Put(blockId, block, source);
                
                YVERIFY(FetchedBlocks.insert(MakePair(blockIndex, block)).second);
                ++receivedBlockCount;
            } else if (reader->Config->FetchFromPeers) {
                FOREACH (const auto& peerAddress, blockInfo.peer_addresses()) {
                    LOG_INFO("Peer info received (Address: %s, PeerAddress: %s, BlockIndex: %d)",
                        ~address,
                        ~peerAddress,
                        blockIndex);
                    AddPeer(peerAddress, blockIndex);
                }
            }
        }

        LOG_INFO("Finished processing reply (BlocksReceived: %d, PeersAdded: %d)",
            receivedBlockCount,
            static_cast<int>(PeerAddressList.size()) - oldPeerCount);
    }

    virtual void OnSessionSucceeded()
    {
        LOG_INFO("All chunk blocks are fetched");

        std::vector<TSharedRef> blocks;
        blocks.reserve(BlockIndexes.size());
        FOREACH (int blockIndex, BlockIndexes) {
            auto block = FetchedBlocks[blockIndex];
            YASSERT(block);
            blocks.push_back(block);
        }
        Promise.Set(IAsyncReader::TReadResult(blocks));
    }

    virtual void OnSessionFailed(const TError& error)
    {
        TError wrappedError(Sprintf("Error fetching chunk blocks\n%s",
            ~error.ToString()));

        LOG_ERROR("%s", ~wrappedError.ToString());

        Promise.Set(wrappedError);
    }
};

TRemoteReader::TAsyncReadResult TRemoteReader::AsyncReadBlocks(const std::vector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();
    return New<TReadSession>(this, blockIndexes)->GetAsyncResult();
}

///////////////////////////////////////////////////////////////////////////////

class TGetMetaSession
    : public TSessionBase
{
public:
    typedef TIntrusivePtr<TGetMetaSession> TPtr;

    TGetMetaSession(
        TRemoteReader* reader,
        const std::vector<int>* extensionTags)
        : TSessionBase(reader)
        , Promise(NewPromise<IAsyncReader::TGetMetaResult>())
        , SeedIndex(0)
    {
        if (extensionTags) {
            ExtensionTags = *extensionTags;
            AllExtensionTags = false;
        } else {
            AllExtensionTags = true;
        }
        Logger.AddTag(Sprintf("GetInfoSession: %p", this));
        NewRetry();
    }

    IAsyncReader::TAsyncGetMetaResult GetAsyncResult() const
    {
        return Promise;
    }

private:
    //! Promise representing the session.
    IAsyncReader::TAsyncGetMetaPromise Promise;

    //! Current index in #SeedAddresses.
    int SeedIndex;

    std::vector<int> ExtensionTags;
    bool AllExtensionTags;

    void RequestInfo()
    {
        auto reader = Reader.Lock();
        if (!reader)
            return;

        auto address = SeedAddresses[SeedIndex];

        LOG_INFO("Requesting chunk info from holder (Address: %s)", ~address);

        auto channel = HolderChannelCache->GetChannel(address);

        TChunkHolderServiceProxy proxy(~channel);
        proxy.SetDefaultTimeout(reader->Config->HolderRpcTimeout);

        auto request = proxy.GetChunkMeta();
        *request->mutable_chunk_id() = reader->ChunkId.ToProto();
        request->set_all_extension_tags(AllExtensionTags);
        ToProto(request->mutable_extension_tags(), ExtensionTags);
        request->Invoke().Subscribe(BIND(&TGetMetaSession::OnGotChunkMeta, MakeStrong(this)));
    }

    void OnGotChunkMeta(TChunkHolderServiceProxy::TRspGetChunkMeta::TPtr response)
    {
        if (response->IsOK()) {
            OnSessionSucceeded(response->chunk_meta());
        } else {
            LOG_WARNING("Error getting chunk info from holder\n%s", ~response->GetError().ToString());

            ++SeedIndex;
            if (SeedIndex < SeedAddresses.size()) {
                RequestInfo();
            } else {
                OnRetryFailed(TError("Unable to get chunk info"));
            }
        }
    }

    virtual void OnGotSeeds()
    {
        TSessionBase::OnGotSeeds();
        RequestInfo();
    }

    void OnSessionSucceeded(const TChunkMeta& chunkMeta)
    {
        LOG_INFO("Chunk info is obtained");
        Promise.Set(IAsyncReader::TGetMetaResult(chunkMeta));
    }

    virtual void OnSessionFailed(const TError& error)
    {
        TError wrappedError(Sprintf("Error getting chunk info\n%s",
            ~error.ToString()));

        LOG_ERROR("%s", ~wrappedError.ToString());

        Promise.Set(wrappedError);
    }
};

TRemoteReader::TAsyncGetMetaResult TRemoteReader::AsyncGetChunkMeta(const std::vector<i32>* tags)
{
    VERIFY_THREAD_AFFINITY_ANY();
    return New<TGetMetaSession>(this, tags)->GetAsyncResult();
}

///////////////////////////////////////////////////////////////////////////////

IAsyncReaderPtr CreateRemoteReader(
    TRemoteReaderConfigPtr config,
    IBlockCachePtr blockCache,
    NRpc::IChannel* masterChannel,
    const TChunkId& chunkId,
    const std::vector<Stroka>& seedAddresses)
{
    YASSERT(config);
    YASSERT(blockCache);
    YASSERT(masterChannel);

    return New<TRemoteReader>(
        config,
        blockCache,
        masterChannel,
        chunkId,
        seedAddresses);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
