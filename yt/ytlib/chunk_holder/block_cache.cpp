#include "block_cache.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ChunkHolderLogger;

////////////////////////////////////////////////////////////////////////////////

TCachedBlock::TCachedBlock(const TBlockId& blockId, const TSharedRef& data)
    : TCacheValueBase<TBlockId, TCachedBlock, TBlockIdHash>(blockId)
    , Data(data)
{ }

TSharedRef TCachedBlock::GetData() const
{
    return Data;
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStore::TBlockCache 
    : public TCapacityLimitedCache<TBlockId, TCachedBlock, TBlockIdHash>
{
public:
    TBlockCache(const TChunkHolderConfig& config)
        : TCapacityLimitedCache<TBlockId, TCachedBlock, TBlockIdHash>(config.CacheCapacity)
    { }

    void Put(const TBlockId& blockId, const TSharedRef& data)
    {
        TInsertCookie cookie(blockId);
        if (BeginInsert(&cookie)) {
            TCachedBlock::TPtr value = new TCachedBlock(blockId, data);
            EndInsert(value, &cookie);
        }
    }

    TCachedBlock::TAsync::TPtr Get(const TBlockId& blockId, i32 blockSize)
    {
        TInsertCookie cookie(blockId);
        if (BeginInsert(&cookie)) {
            // TODO: load data
            //EndInsert(value, &cookie);
        }
        return cookie.GetAsyncResult();
    }
};

////////////////////////////////////////////////////////////////////////////////

TBlockStore::TBlockStore(TChunkHolderConfig& config)
    : Cache(new TBlockCache(config))
{ }

TCachedBlock::TAsync::TPtr TBlockStore::GetBlock(const TBlockId& blockId, i32 blockSize)
{
    return Cache->Get(blockId, blockSize);
}

void TBlockStore::PutBlock(const TBlockId& blockId, const TSharedRef& data)
{
    Cache->Put(blockId, data);
}

////////////////////////////////////////////////////////////////////////////////

TChunkSession::TPtr TChunkSessionManager::FindSession(const TChunkId& chunkId)
{
    TSessionMap::iterator it = SessionMap.find(chunkId);
    if (it == SessionMap.end())
        return NULL;
    
    TSession::TPtr session = it->Second();
    session->RenewLease();
    return session;
}

void TChunkSessionManager::InitLocations()
{

}

TChunkSession::TPtr TChunkSessionManager::StartSession(const TChunkId& chunkId)
{
    TSession::TPtr session = new TSession(this);
    TLeaseManager::TLease lease = LeaseManager->CreateLease(
        Config.LeaseTimeout,
        FromMethod(
            &TChunkSessionManager::OnLeaseExpired,
            TPtr(this),
            session));
    session->SetLease(lease);
    // TODO: use YVERIFY
    VERIFY(SessionMap.insert(MakePair(chunkId, session)).Second(), "oops");
    // TODO: add logging here
    return session;
}

void TChunkSessionManager::CancelSession(TSession::TPtr session)
{
    // TODO: code here
}

void TChunkSessionManager::OnLeaseExpired(TSession::TPtr session)
{
    CancelSession(session);
}

////////////////////////////////////////////////////////////////////////////////

void TChunkSession::SetLease(TLeaseManager::TLease lease)
{
    Lease = lease;
}

void TChunkSession::RenewLease()
{
    Store->LeaseManager->RenewLease(Lease);
}

////////////////////////////////////////////////////////////////////////////////

}
