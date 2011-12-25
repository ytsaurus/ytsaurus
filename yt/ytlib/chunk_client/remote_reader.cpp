#include "stdafx.h"
#include "remote_reader.h"
#include "holder_channel_cache.h"

#include "../misc/foreach.h"
#include "../misc/string.h"
#include "../misc/thread_affinity.h"
#include "../misc/delayed_invoker.h"
#include "../logging/tagged_logger.h"
#include "../chunk_server/chunk_service_proxy.h"
#include "../chunk_holder/chunk_holder_service_proxy.h"

#include <util/random/shuffle.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NChunkHolder;
using namespace NChunkHolder::NProto;
using namespace NChunkServer;
using namespace NChunkServer::NProto;

///////////////////////////////////////////////////////////////////////////////

class TSessionBase;
class TReadSession;
class TGetInfoSession;

class TRemoteReader
    : public IAsyncReader
{
public:
    typedef TIntrusivePtr<TRemoteReader> TPtr;
    typedef TRemoteReaderConfig TConfig;

    TRemoteReader(
        TRemoteReaderConfig* config,
        IBlockCache* blockCache,
        IChannel* masterChannel,
        const TChunkId& chunkId,
        const yvector<Stroka>& seedAddresses,
        const TPeerInfo& peer)
        : Config(config)
        , BlockCache(blockCache)
        , ChunkId(chunkId)
        , Peer(peer)
        , Logger(ChunkClientLogger)
    {
        Logger.SetTag(Sprintf("ChunkId: %s", ~ChunkId.ToString()));

        if (seedAddresses.empty()) {
            LOG_INFO("Reader created, no seeds are given");
        } else {
            GetSeedsResult = ToFuture(TGetSeedsResult(seedAddresses));
            LOG_INFO("Reader created (SeedAddresses: [%s])", ~JoinToString(seedAddresses));
        }

        Proxy = new TProxy(masterChannel);
        Proxy->SetTimeout(config->MasterRpcTimeout);
    }

    TAsyncReadResult::TPtr AsyncReadBlocks(const yvector<int>& blockIndexes);
    TAsyncGetInfoResult::TPtr AsyncGetChunkInfo();

    typedef TValueOrError< yvector<Stroka> > TGetSeedsResult;
    typedef TFuture<TGetSeedsResult> TAsyncGetSeedsResult;

    TAsyncGetSeedsResult::TPtr AsyncGetSeeds()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        TGuard<TSpinLock> guard(SpinLock);
        if (!GetSeedsResult) {
            LOG_INFO("Fresh chunk seeds are needed");
            GetSeedsResult = New<TAsyncGetSeedsResult>();
            TDelayedInvoker::Submit(
                ~FromMethod(&TRemoteReader::DoFindChunk, TPtr(this)),
                LastFindChunkTime + Config->BackoffTime);
        }

        return GetSeedsResult;
    }

    void DiscardSeeds(TAsyncGetSeedsResult* result)
    {
        YASSERT(result);
        YASSERT(result->IsSet());

        TGuard<TSpinLock> guard(SpinLock);

        if (GetSeedsResult != result)
            return;

        YASSERT(GetSeedsResult->IsSet());
        GetSeedsResult.Reset();
    }

private:
    friend class TSessionBase;
    friend class TReadSession;
    friend class TGetInfoSession;

    typedef TChunkServiceProxy TProxy;

    TRemoteReaderConfig::TPtr Config;
    IBlockCache::TPtr BlockCache;
    TChunkId ChunkId;
    TPeerInfo Peer;
    NLog::TTaggedLogger Logger;

    TAutoPtr<TProxy> Proxy;

    TSpinLock SpinLock;
    TAsyncGetSeedsResult::TPtr GetSeedsResult;
    TInstant LastFindChunkTime;

    void DoFindChunk()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        LOG_INFO("Requesting chunk seeds from the master");

        auto request = Proxy->FindChunk();
        request->set_chunkid(ChunkId.ToProto());
        request->Invoke()->Subscribe(FromMethod(&TRemoteReader::OnChunkFound, TPtr(this)));
    }

    void OnChunkFound(TProxy::TRspFindChunk::TPtr response)
    {
        VERIFY_THREAD_AFFINITY_ANY();
        YASSERT(GetSeedsResult);

        {
            TGuard<TSpinLock> guard(SpinLock);
            LastFindChunkTime = TInstant::Now();
        }

        if (response->IsOK()) {
            const auto& seedAddresses = FromProto<Stroka>(response->holderaddresses());
            if (seedAddresses.empty()) {
                LOG_WARNING("Chunk is lost");
            } else {
                LOG_INFO("Chunk seeds found (SeedAddresses: [%s])", ~JoinToString(seedAddresses));
            }
            GetSeedsResult->Set(seedAddresses);
        } else {
            auto message = Sprintf("Error requesting chunk seeds from master\n%s",
                ~response->GetError().ToString());
            LOG_WARNING("%s", ~message);
            GetSeedsResult->Set(TError(message));
        }
    }
};

///////////////////////////////////////////////////////////////////////////////

class TSessionBase
    : public TRefCountedBase
{
protected:
    typedef TIntrusivePtr<TSessionBase> TPtr;

    TRemoteReader::TPtr Reader;
    int RetryIndex;
    TRemoteReader::TAsyncGetSeedsResult::TPtr GetSeedsResult;
    NLog::TTaggedLogger Logger;
    yvector<Stroka> SeedAddresses;

    TSessionBase(TRemoteReader* reader)
        : Reader(reader)
        , RetryIndex(0)
        , Logger(ChunkClientLogger)
    { }

    void NewRetry()
    {
        YASSERT(!GetSeedsResult);

        LOG_INFO("New retry started (RetryIndex: %d)", RetryIndex);

        GetSeedsResult = Reader->AsyncGetSeeds();
        GetSeedsResult->Subscribe(FromMethod(&TSessionBase::OnGetSeedsReply, TPtr(this)));
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
            OnSessionFailed(TError(Sprintf("Retries have been aborted due to master error (RetryIndex: %d)\n%s",
                RetryIndex,
                ~result.ToString())));
        }
    }

    void OnRetryFailed(const TError& error)
    {
        LOG_WARNING("Retry failed (RetryIndex: %d)\n%s",
            RetryIndex,
            ~error.ToString());

        YASSERT(GetSeedsResult);
        Reader->DiscardSeeds(~GetSeedsResult);
        GetSeedsResult.Reset();

        if (RetryIndex < Reader->Config->RetryCount) {
            ++RetryIndex;
            NewRetry();
        } else {
            OnSessionFailed(TError(Sprintf("All retries failed (RetryCount: %d)",
                RetryIndex)));
        }
    }

    virtual void OnGotSeeds() = 0;
    virtual void OnSessionFailed(const TError& error) = 0;

};

///////////////////////////////////////////////////////////////////////////////

class TReadSession
    : public TSessionBase
{
public:
    typedef TIntrusivePtr<TReadSession> TPtr;

    TReadSession(TRemoteReader* reader, const yvector<int>& blockIndexes)
        : TSessionBase(reader)
        , AsyncResult(New<IAsyncReader::TAsyncReadResult>())
        , BlockIndexes(blockIndexes)
    {
        // TODO: use stack here
        Logger.SetTag(Sprintf("ChunkId: %s, ReadSession: %p",
            ~Reader->ChunkId.ToString(),
            this));

        FetchBlocksFromCache();

        if (GetUnfetchedBlockIndexes().empty()) {
            LOG_INFO("All chunk blocks are fetched from cache");
            OnSessionSucceeded();
        } else {
            NewRetry();
        }
    }

    IAsyncReader::TAsyncReadResult::TPtr GetAsyncResult() const
    {
        return AsyncResult;
    }

private:
    //! Async result representing the session.
    IAsyncReader::TAsyncReadResult::TPtr AsyncResult;

    //! Block indexes to read during the session.
    yvector<int> BlockIndexes;

    //! Blocks that are fetched so far.
    yhash_map<int, TSharedRef> FetchedBlocks;

    //! Holder addresses tried during the current retry.
    yhash_set<Stroka> TriedAddresses;

    //! List of candidates to try.
    yvector<Stroka> PeerAddresses;

    //! Current index in #CandidateAddresses.
    int PeerIndex;

    void Cleanup()
    {
        PeerAddresses.clear();
        PeerIndex = 0;
        TriedAddresses.clear();
    }

    void AddPeers(yvector<Stroka>& addresses)
    {
        // Remove duplicates and shuffle.
        std::sort(addresses.begin(), addresses.end());
        addresses.erase(std::unique(addresses.begin(), addresses.end()), addresses.end());
        // TODO(babenko): use std::random_shuffle here but make sure is uses true randomness.
        Shuffle(addresses.begin(), addresses.end());

        FOREACH (const auto& address, addresses) {
            if (TriedAddresses.find(address) == TriedAddresses.end()) {
                PeerAddresses.push_back(address);
                LOG_INFO("Peer added (Address: %s)", ~address);
            }
        }
    }

    yvector<int> GetUnfetchedBlockIndexes()
    {
        yvector<int> result;
        result.reserve(BlockIndexes.size());
        FOREACH (int blockIndex, BlockIndexes) {
            if (FetchedBlocks.find(blockIndex) == FetchedBlocks.end()) {
                result.push_back(blockIndex);
            }
        }
        return result;
    }

    void FetchBlocksFromCache()
    {
        FOREACH (int blockIndex, BlockIndexes) {
            if (FetchedBlocks.find(blockIndex) == FetchedBlocks.end()) {
                TBlockId blockId(Reader->ChunkId, blockIndex);
                auto block = Reader->BlockCache->Find(blockId);
                if (block) {
                    LOG_INFO("Block is fetched from cache (BlockIndex: %d)", blockIndex);
                    YVERIFY(FetchedBlocks.insert(MakePair(blockIndex, block)).second);
                }
            }
        }
    }

    void ProcessReceivedBlocks(
        TChunkHolderServiceProxy::TReqGetBlocks* request,
        TChunkHolderServiceProxy::TRspGetBlocks* response)
    {
        size_t blockCount = request->block_indexes_size();
        YASSERT(response->blocks_size() == blockCount);
        YASSERT(response->Attachments().size() == blockCount);

        int receivedBlockCount = 0;
        int oldPeerCount = PeerAddresses.ysize();

        for (int index = 0; index < static_cast<int>(blockCount); ++index) {
            int blockIndex = request->block_indexes(index);
            TBlockId blockId(Reader->ChunkId, blockIndex);
            const auto& blockInfo = response->blocks(index);
            if (blockInfo.data_attached()) {
                LOG_INFO("Received block from holder (BlockIndex: %d)", blockIndex);
                auto block = response->Attachments()[index];
                YASSERT(block);
                Reader->BlockCache->Put(blockId, block);
                YVERIFY(FetchedBlocks.insert(MakePair(blockIndex, block)).second);
                ++receivedBlockCount;
            } else {
                auto addresses = FromProto<Stroka>(blockInfo.peer_addresses());
                if (!addresses.empty()) {
                    LOG_INFO("Received peer addresses (BlockIndex: %d, PeerCount: %d)",
                        blockIndex,
                        addresses.ysize());
                    AddPeers(addresses);
                }
            }
        }

        LOG_INFO("Finished processing holder reply (BlocksReceived: %d, PeersAdded: %d)",
            receivedBlockCount,
            PeerAddresses.ysize() - oldPeerCount);
    }

    virtual void OnGotSeeds()
    {
        Cleanup();
        AddPeers(SeedAddresses);
        RequestBlocks();
    }

    void RequestBlocks()
    {
        FetchBlocksFromCache();

        auto unfetchedBlockIndexes = GetUnfetchedBlockIndexes();
        if (unfetchedBlockIndexes.empty()) {
            OnSessionSucceeded();
            return;
        }

        auto address = PeerAddresses[PeerIndex];

        LOG_INFO("Requesting blocks from holder (Address: %s, BlockIndexes: [%s])",
            ~address,
            ~JoinToString(unfetchedBlockIndexes));

        YVERIFY(TriedAddresses.insert(address).second);

        auto channel = HolderChannelCache->GetChannel(address);

        TChunkHolderServiceProxy proxy(~channel);
        proxy.SetTimeout(Reader->Config->HolderRpcTimeout);

        auto request = proxy.GetBlocks();
        request->set_chunk_id(Reader->ChunkId.ToProto());
        ToProto(*request->mutable_block_indexes(), unfetchedBlockIndexes);
        if (!Reader->Peer.IsNull()) {
            request->set_peer_address(Reader->Peer.Address);
            request->set_peer_expiration_time(Reader->Peer.ExpirationTime.GetValue());
        }

        request->Invoke()->Subscribe(FromMethod(
            &TReadSession::OnGotBlocks,
            TPtr(this),
            request));
    }

    void OnGotBlocks(
        TChunkHolderServiceProxy::TRspGetBlocks::TPtr response,
        TChunkHolderServiceProxy::TReqGetBlocks::TPtr request)
    {
        if (response->IsOK()) {
            ProcessReceivedBlocks(~request, ~response);
        } else {
            LOG_WARNING("Error getting blocks from holder\n%s", ~response->GetError().ToString());
        }

        ++PeerIndex;
        if (PeerIndex < PeerAddresses.ysize()) {
            RequestBlocks();
        } else {
            OnRetryFailed(TError("Unable to fetch all chunk blocks"));
        }
    }

    virtual void OnSessionSucceeded()
    {
        LOG_INFO("All chunk blocks are fetched");

        yvector<TSharedRef> blocks;
        blocks.reserve(BlockIndexes.size());
        FOREACH (int blockIndex, BlockIndexes) {
            auto block = FetchedBlocks[blockIndex];
            YASSERT(block);
            blocks.push_back(block);
        }
        AsyncResult->Set(IAsyncReader::TReadResult(blocks));
    }

    virtual void OnSessionFailed(const TError& error)
    {
        TError wrappedError(Sprintf("Error fetching chunk blocks\n%s",
            ~error.ToString()));

        LOG_ERROR("%s", ~wrappedError.ToString());

        AsyncResult->Set(wrappedError);
    }
};

TRemoteReader::TAsyncReadResult::TPtr TRemoteReader::AsyncReadBlocks(const yvector<int>& blockIndexes)
{
    VERIFY_THREAD_AFFINITY_ANY();
    return New<TReadSession>(this, blockIndexes)->GetAsyncResult();
}

///////////////////////////////////////////////////////////////////////////////

class TGetInfoSession
    : public TSessionBase
{
public:
    typedef TIntrusivePtr<TGetInfoSession> TPtr;

    TGetInfoSession(TRemoteReader* reader)
        : TSessionBase(reader)
        , AsyncResult(New<IAsyncReader::TAsyncGetInfoResult>())
        , SeedIndex(0)
    {
        // TODO: use stack here
        Logger.SetTag(Sprintf("ChunkId: %s, GetInfoSession: %p",
            ~Reader->ChunkId.ToString(),
            this));

        NewRetry();
    }

    IAsyncReader::TAsyncGetInfoResult::TPtr GetAsyncResult() const
    {
        return AsyncResult;
    }

private:
    //! Async result representing the session.
    IAsyncReader::TAsyncGetInfoResult::TPtr AsyncResult;

    //! Current index in #SeedAddresses.
    int SeedIndex;

    void RequestInfo()
    {
        auto address = SeedAddresses[SeedIndex];

        LOG_INFO("Requesting chunk info from holder (Address: %s)", ~address);

        auto channel = HolderChannelCache->GetChannel(address);

        TChunkHolderServiceProxy proxy(~channel);
        proxy.SetTimeout(Reader->Config->HolderRpcTimeout);

        auto request = proxy.GetChunkInfo();
        request->set_chunk_id(Reader->ChunkId.ToProto());
        request->Invoke()->Subscribe(FromMethod(&TGetInfoSession::OnGotChunkInfo, TPtr(this)));
    }

    void OnGotChunkInfo(TChunkHolderServiceProxy::TRspGetChunkInfo::TPtr response)
    {
        if (response->IsOK()) {
            OnSessionSucceeded(response->chunk_info());
        } else {
            LOG_WARNING("Error getting chunk info from holder\n%s", ~response->GetError().ToString());

            ++SeedIndex;
            if (SeedIndex < SeedAddresses.ysize()) {
                RequestInfo();
            } else {
                OnRetryFailed(TError("Unable to get chunk info"));
            }
        }
    }

    virtual void OnGotSeeds()
    {
        RequestInfo();
    }

    void OnSessionSucceeded(const TChunkInfo& info)
    {
        LOG_INFO("Chunk info is obtained");
        AsyncResult->Set(IAsyncReader::TGetInfoResult(info));
    }

    virtual void OnSessionFailed(const TError& error)
    {
        TError wrappedError(Sprintf("Error getting chunk info\n%s",
            ~error.ToString()));

        LOG_ERROR("%s", ~wrappedError.ToString());

        AsyncResult->Set(wrappedError);
    }
};

TRemoteReader::TAsyncGetInfoResult::TPtr TRemoteReader::AsyncGetChunkInfo()
{
    VERIFY_THREAD_AFFINITY_ANY();
    return New<TGetInfoSession>(this)->GetAsyncResult();
}

///////////////////////////////////////////////////////////////////////////////

IAsyncReader::TPtr CreateRemoteReader(
    TRemoteReaderConfig* config,
    IBlockCache* blockCache,
    NRpc::IChannel* masterChannel,
    const TChunkId& chunkId,
    const yvector<Stroka>& seedAddresses,
    const TPeerInfo& peer)
{
    YASSERT(config);
    YASSERT(blockCache);
    YASSERT(masterChannel);

    return New<TRemoteReader>(
        config,
        blockCache,
        masterChannel,
        chunkId,
        seedAddresses,
        peer);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
