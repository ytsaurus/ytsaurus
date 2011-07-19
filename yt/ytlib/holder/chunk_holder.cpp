#include "chunk_holder.h"

#include "../misc/fs.h"
#include "../misc/string.h"
#include "../misc/serialize.h"
#include "../misc/guid.h"
#include "../logging/log.h"
#include "../actions/action_util.h"

#include <util/folder/dirut.h>
#include <util/folder/filelist.h>

namespace NYT {

static NLog::TLogger Logger("ChunkHolder");

////////////////////////////////////////////////////////////////////////////////

void TChunkHolderConfig::Read(const TJsonObject* jsonConfig)
{
    if (jsonConfig == NULL)
        return;
    NYT::TryRead(jsonConfig, L"WindowSize", &WindowSize);
    NYT::TryRead(jsonConfig, L"Locations", &Locations);
    BlockCacheConfig.Read(jsonConfig->Value(L"BlockCache"));
}

////////////////////////////////////////////////////////////////////////////////

class TChunkHolder::TCompleteFlush
    : public IAction
{
    TSessionPtr Session;
    TCtxFlushBlocks::TPtr Context;
    i32 StartBlock;
    i32 EndBlock;
    bool Finished;

public:
    TCompleteFlush(TSessionPtr session, i32 startBlock, i32 endBlock, TCtxFlushBlocks::TPtr context)
        : Session(session)
        , Context(context)
        , StartBlock(startBlock)
        , EndBlock(endBlock)
        , Finished(false)
    {}

    inline bool IsFinished() const 
    {
        return Finished;
    }

    virtual void Do() throw();
};

////////////////////////////////////////////////////////////////////////////////

class TChunkHolder::TSession
    : public TRefCountedBase
{
    DECLARE_ENUM(EBlockState,
        (Empty)
        (Received)
        (BeingWritten)
        (Written)
    );


    struct TEntry
    {
        EBlockState State;
        TCachedBlock::TPtr CachedBlock;
        IAction::TPtr CompleteFlush;

        TEntry()
            : State(EBlockState::Empty)
            , CachedBlock(NULL)
            , CompleteFlush(NULL)
        { }
    };

    yvector<TEntry> Blocks; // Cyclic buffer
    i32 StartOffset;
    i32 StartIndex;

    TLeaseManager::TLease Lease;
    THolder<TFile> File; // Access from IO thread
    i32 Location;

    TChunkId ChunkId;

    TEntry& GetEntry(i32 index)
    {
        if (index < StartIndex || index >= StartIndex + Blocks.ysize())
            LOG_FATAL("Chunk %s: incorrect block index: %d",
                ~GetChunkIdAsString(), index);

        i32 i = (StartOffset + index - StartIndex) % Blocks.ysize();
        return Blocks[i];
    }

    void ShiftWindow(i32 index)
    {
        i32 minStartIndex = index - Blocks.ysize();
        if (minStartIndex <= StartIndex)
            return;

        for (i32 i = StartIndex; i < minStartIndex; ++i) {
            TEntry& entry = GetEntry(i);
            if (entry.State != EBlockState::Written) {
                LOG_FATAL("Chunk %s: missing block %d while moving window",
                           ~GetChunkIdAsString(), i);
            }
            entry.State = EBlockState::Empty;
        }
        StartOffset += minStartIndex - StartIndex;
        StartOffset %= Blocks.ysize();
        StartIndex = minStartIndex;
        LOG_DEBUG("Chunk %s: window moved (StartIndex: %d)",
                    ~GetChunkIdAsString(), StartIndex);
    }

public:
    TSession(const Stroka& fileName, i32 windowSize,
        TLeaseManager::TLease lease, i32 location, TChunkId chunkId)
        : Blocks(windowSize)
        , StartOffset(0)
        , StartIndex(0)
        , Lease(lease)
        , File(new TFile(fileName, WrOnly | Seq | CreateAlways))
        , Location(location)
        , ChunkId(chunkId)
    {}

    TLeaseManager::TLease GetLease() const
    {
        return Lease;
    }

    TFile* GetFile()
    {
        return ~File;
    }

    i32 GetLocation() const
    {
        return Location;
    }

    Stroka GetChunkIdAsString() const
    {
        return StringFromGuid(ChunkId);
    }

    void SetBlock(i32 blockIndex, TCachedBlock::TPtr cachedBlock)
    {
        ShiftWindow(blockIndex);
        TEntry& block = GetEntry(blockIndex);
        YASSERT(block.State == EBlockState::Empty);
        block.CachedBlock = cachedBlock;
        block.State = EBlockState::Received;
    }

    TCachedBlock::TPtr GetBlock(i32 blockIndex)
    {
        TEntry& e = GetEntry(blockIndex);
        YASSERT(e.State != EBlockState::Empty);
        return e.CachedBlock;
    }

    // Returns first unwritable index
    i32 GetWritableBlocks(yvector<TCachedBlock::TPtr>* output)
    {
        i32 i = StartIndex;
        while (true) {
            TEntry& b = GetEntry(i);
            if (b.State != EBlockState::Written && b.State != EBlockState::BeingWritten)
                break;
            ++i;
        }

        while (true) {
            TEntry& entry = GetEntry(i);
            if (entry.State != EBlockState::Received)
                break;
            output->push_back(entry.CachedBlock);
            entry.State = EBlockState::BeingWritten;
            ++i;
        }
        return i;
    }

    void FlushBlocks(TCtxFlushBlocks::TPtr context)
    {
        TCtxFlushBlocks::TTypedRequest* req = &context->Request();
        i32 startBlock = req->GetStartBlockIndex();
        i32 endBlock = req->GetEndBlockIndex();
        // do one iteration immediately
        TIntrusivePtr<TCompleteFlush> task =
            new TCompleteFlush(this, startBlock, endBlock, context);
        task->Do();

        if (!task->IsFinished())
            for (i32 i = startBlock; i <= endBlock; ++i) {
                YASSERT(~GetEntry(i).CompleteFlush == NULL);
                GetEntry(i).CompleteFlush = ~task;
            }
    }

    bool IsWritten(i32 blockIndex)
    {
        if (blockIndex < StartIndex)
            return true;
        if (blockIndex >= StartIndex + Blocks.ysize())
            return false;
        return GetEntry(blockIndex).State == EBlockState::Written;
    }

    void CompleteWriteTo(i32 firstUnwritten)
    {
        for (i32 i = StartIndex; i < firstUnwritten; ++i) {
            GetEntry(i).State = EBlockState::Written;
        }

        for (i32 i = StartIndex; i < firstUnwritten; ++i) {
            TEntry& b = GetEntry(i);
            if (~b.CompleteFlush != NULL) {
                b.CompleteFlush->Do();
                b.CompleteFlush.Drop();
            }
        }
        LOG_DEBUG("Chunk %s: blocks up to %d are marked as written",
            ~GetChunkIdAsString(), firstUnwritten);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TChunkHolder::TCompleteFlush::Do() throw() 
{
    if (Finished)
        return;

    // Check if all blocks are flushed
    for (i32 i = StartBlock; i <= EndBlock; ++i) {
        if (!Session->IsWritten(i))
            return;
    }

    Context->Reply();
    Finished = true;
    
    LOG_INFO("Chunk %s: blocks %d-%d are flushed",
               ~Session->GetChunkIdAsString(), StartBlock, EndBlock);
}

////////////////////////////////////////////////////////////////////////////////

TChunkHolder::TChunkHolder(const TChunkHolderConfig& config)
    : NRpc::TServiceBase(TProxy::GetServiceName(), Logger.GetCategory())
    , Config(config)
    , BlockCache(new TBlockCache(config.BlockCacheConfig))
    , ServiceQueue(new TActionQueue())
    , IOQueues(config.Locations.ysize())
    , LeaseManager(new TLeaseManager)
{
    RegisterMethods();
    InitLocations();
}

// Do not remove this!
// Required for TInstusivePtr with an incomplete type.
TChunkHolder::~TChunkHolder() 
{ }

void TChunkHolder::RegisterMethods()
{
    RPC_REGISTER_METHOD(TChunkHolder, StartChunk);
    RPC_REGISTER_METHOD(TChunkHolder, FinishChunk);
    RPC_REGISTER_METHOD(TChunkHolder, PutBlocks);
    RPC_REGISTER_METHOD(TChunkHolder, FlushBlocks);
    RPC_REGISTER_METHOD(TChunkHolder, SendBlocks);
    RPC_REGISTER_METHOD(TChunkHolder, GetBlocks);
}

void TChunkHolder::InitLocations()
{
    for (i32 i = 0; i < Config.Locations.ysize(); ++i) {
        MakeDirIfNotExist(~Config.Locations[i]);
        TFileList fileList;
        fileList.Fill(Config.Locations[i]);
        const char* fileName;
        while ((fileName = fileList.Next()) != NULL) {
            TChunkId id = GetGuid(fileName);
            LocationMap[id] = i;
        }

        IOQueues[i].Reset(new TActionQueue());
    }
}

// IO thread
void TChunkHolder::ReadBlocks(
    TCtxGetBlocks::TPtr context, 
    Stroka fileName,
    // TODO: size_t -> int
    const yvector<size_t>& uncached) 
{
    TCtxGetBlocks::TTypedRequest& req = context->Request();
    TCtxGetBlocks::TTypedResponse& rsp = context->Response();

    TChunkId chunkId = TGuid::FromProto(req.GetChunkId());
    TFile file(fileName, OpenExisting | RdOnly);
    // TODO: rename i and idx
    for (int i = 0; i < uncached.ysize(); ++i) {
        i32 idx = uncached[i];

        const NRpcChunkHolder::TBlockInfo& bi = req.GetBlocks(idx);
        TBlob buffer(bi.GetSize());
        file.Seek(bi.GetOffset(), sSet);
        i32 bytesRead = file.Read(buffer.begin(), bi.GetSize());

        if (bytesRead != bi.GetSize()) {
            // TODO: use Sprintf
            ythrow TServiceException() << "Error trying to read file " <<
                fileName << " (block at offset " << bi.GetOffset() <<
                " with size " << bi.GetSize() << ") - got only " << 
                bytesRead << " bytes";
        }
        
        TBlockId id(chunkId, bi.GetOffset());
        TCachedBlock::TPtr block = BlockCache->Put(id, TSharedRef(buffer));
        rsp.Attachments()[idx] = block->GetData();
    }

    context->Reply();

    LOG_DEBUG("Chunk %s: reading complete %d blocks, cached %d blocks",
        ~StringFromGuid(chunkId),
        static_cast<int>(req.BlocksSize()),
        uncached.ysize());
}

// IO thread
void TChunkHolder::WriteBlocks(TSessionPtr session,
                 TAutoPtr< yvector<TCachedBlock::TPtr> > blocks,
                 i32 firstUnwritable)
{
    try {
        LOG_DEBUG("Chunk %s: writing %d blocks at offset %" PRId64,
            ~session->GetChunkIdAsString(),
            blocks->ysize(),
            blocks->front()->GetKey().Offset);

        i64 minLength = blocks->back()->GetKey().Offset + blocks->back()->GetData().Size();
        TFile* file = session->GetFile();
        file->Reserve(minLength);
        file->Seek(blocks->front()->GetKey().Offset, sSet);

        for (i32 i = 0; i < blocks->ysize(); ++i) {
            const TSharedRef& data = blocks->at(i)->GetData();
            file->Write(data.Begin(), data.Size());
        }

        file->Flush();
        LOG_DEBUG("Chunk %s: %d blocks are written",
                ~session->GetChunkIdAsString(), blocks->ysize());

        ServiceQueue->Invoke(FromMethod(&TSession::CompleteWriteTo, session, firstUnwritable));
    } catch (const TFileError& e) {
        LOG_FATAL("Chunk %s: writing blocks failed: %s",
            ~session->GetChunkIdAsString(), e.what());
    }
}

// IO thread
void TChunkHolder::DoCancelSession(TSessionPtr session)
{
    session->GetFile()->Close();
    if (!NFS::Remove(~session->GetFile()->GetName())) {
        LOG_FATAL("Chunk %s: unable to remove file %s while canceling",
            ~session->GetChunkIdAsString(), 
            ~session->GetFile()->GetName());
    }

    LOG_INFO("Chunk %s: transaction cancelled",
        ~session->GetChunkIdAsString());
}

// IO thread
void TChunkHolder::DoFinishChunk(TCtxFinishChunk::TPtr context, TSessionPtr session)
{
    session->GetFile()->Close();
    context->Reply();

    LOG_INFO("Chunk %s: transaction finished",
        ~session->GetChunkIdAsString());
}

i32 TChunkHolder::GetNewLocation(const TChunkId& chunkId) const
{
    static TGuidHash hasher;
    YASSERT(Config.Locations.size() != 0);
    i32 index = hasher(chunkId) % Config.Locations.size();
    if (index < 0) {
        index += Config.Locations.size();
    }
    return index;
}

Stroka TChunkHolder::GetFileName(const TChunkId& id, i32 location) const
{
    return Config.Locations[location] + GetDirectorySeparator() + GetGuidAsString(id);
}

Stroka TChunkHolder::GetFileName(const TChunkId& id) const
{
    i32 location = GetNewLocation(id);
    return GetFileName(id, location);
}

TChunkHolder::TSessionPtr TChunkHolder::GetSession(const TChunkId& chunkId)
{
    return GetSessionIter(chunkId)->Second();
}

TChunkHolder::TSessionMap::iterator TChunkHolder::GetSessionIter(const TChunkId& chunkId)
{
    TSessionMap::iterator it = Sessions.find(chunkId);
    if (it == Sessions.end())
        ythrow TServiceException() << "Session does not exist " << StringFromGuid(chunkId);

    return it;
}

void TChunkHolder::CancelSession(const TChunkId& chunkId)
{
    TSessionMap::iterator it = GetSessionIter(chunkId);

    LOG_INFO("Chunk %s: cancelling transaction", ~StringFromGuid(chunkId));

    TSessionPtr session = it->second;
    Sessions.erase(it);
    TActionQueue* queue = ~IOQueues[session->GetLocation()];
    queue->Invoke(FromMethod(&TChunkHolder::DoCancelSession, this, session));
}

void TChunkHolder::PutBlocksCallback(TProxy::TRspPutBlocks::TPtr putResponse, TCtxSendBlocks::TPtr context)
{
    TCtxSendBlocks::TTypedRequest& req = context->Request();
    TChunkId chunkId = TGuid::FromProto(req.GetChunkId());
    i32 start = req.GetStartBlockIndex();
    i32 end = req.GetEndBlockIndex();
    const Stroka& dst = req.GetDestination();

    if (putResponse->GetErrorCode().IsOK()) {
        LOG_WARNING("Chunk %s: error sending blocks %d-%d to %s",
                      ~StringFromGuid(chunkId), start, end, ~dst);
        context->Reply(TProxy::EErrorCode::RemoteCallFailed);
    } else {
        LOG_WARNING("Chunk %s: sent blocks %d-%d to %s",
                      ~StringFromGuid(chunkId), start, end, ~dst);
        context->Reply();
    }
}

void TChunkHolder::VerifyNoSession(const TChunkId& chunkId)
{
    if (Sessions.find(chunkId) != Sessions.end())
        ythrow TServiceException() << "Session " << StringFromGuid(chunkId) << " already exists";
}

void TChunkHolder::VerifyNoChunk(const TChunkId& chunkId)
{
    if (LocationMap.find(chunkId) != LocationMap.end())
        ythrow TServiceException() << "Chunk " << StringFromGuid(chunkId) << " already exists";
}

void TChunkHolder::CheckLease(bool leaseResult, const TChunkId& chunkId)
{
    if (!leaseResult)
        ythrow TServiceException() << "No lease for session " << StringFromGuid(chunkId);
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, StartChunk)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());

    VerifyNoSession(chunkId);
    VerifyNoChunk(chunkId);

    TLeaseManager::TLease lease = LeaseManager->CreateLease(
        Config.LeaseTimeout,
        FromMethod(&TChunkHolder::CancelSession, this, chunkId)->Via(~ServiceQueue));
    i32 location = GetNewLocation(chunkId);
    Stroka fileName = GetFileName(chunkId, location);

    Sessions[chunkId] = new TSession(fileName, Config.WindowSize,
        lease, location, chunkId);
    context->Reply();

    LOG_INFO("Chunk %s: transaction started (Location: %d)",
        ~StringFromGuid(chunkId), location);
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FinishChunk)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    TSessionMap::iterator it = GetSessionIter(chunkId);
    YASSERT(LocationMap.find(chunkId) == LocationMap.end());

    TSessionPtr session = it->Second();
    CheckLease(LeaseManager->CloseLease(session->GetLease()), chunkId);
    Sessions.erase(it);
    i32 location = session->GetLocation();
    
    // ToDo: wrap with exception handling for completion
    IOQueues[location]->Invoke(FromMethod(
        &TChunkHolder::DoFinishChunk,
        this,
        context,
        session));

    // NB: read request may come and be processed before file is closed 
    // (although this is an unexpected client behavior, as far as we have not sent finish response)
    // but IO operation will go into the same IO thread and be executed there after the file is closed.
    LocationMap[chunkId] = location; 
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, PutBlocks)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    i32 startBlock = request->GetStartBlockIndex();
    TBlockOffset offset = request->GetStartOffset();

    LOG_DEBUG("Chunk %s: put blocks %d-%d",
        ~StringFromGuid(chunkId),
        startBlock, 
        startBlock + request->Attachments().ysize() - 1);

    TSessionPtr session = GetSession(chunkId);
    CheckLease(LeaseManager->RenewLease(session->GetLease()), chunkId);

    for (i32 i = 0; i < request->Attachments().ysize(); ++i) {
        // Make a copy of the attachment to enable separate caching
        // of blocks arriving within a single RPC request.
        TBlob data = request->Attachments()[i].ToBlob();
        TBlockId blockId(chunkId, offset);
        LOG_DEBUG("Chunk %s: putting block %d at offset %" PRId64,
            ~StringFromGuid(chunkId),
            startBlock + i,
            offset);
        TCachedBlock::TPtr cachedBlock = BlockCache->Put(blockId, data);
        session->SetBlock(startBlock + i, cachedBlock);
        offset += cachedBlock->GetData().Size();
    }

    TAutoPtr< yvector<TCachedBlock::TPtr> > blocks;
    i32 firstUnwritable = session->GetWritableBlocks(~blocks);
    if (blocks->ysize() > 0) {
        IOQueues[session->GetLocation()]->Invoke(FromMethod(
            &TChunkHolder::WriteBlocks,
            this,
            session,
            blocks,
            firstUnwritable));
    }
    
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, SendBlocks)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    i32 start = request->GetStartBlockIndex();
    i32 end = request->GetEndBlockIndex();
    const Stroka& dst = request->GetDestination();

    LOG_DEBUG("Chunk %s: sending blocks %d-%d to %s",
        ~StringFromGuid(chunkId), start, end, ~dst);

    TSessionPtr session = GetSession(chunkId);
    CheckLease(LeaseManager->RenewLease(session->GetLease()), chunkId);

    TProxy::TReqPutBlocks::TPtr putReq = TProxy(ChannelCache.GetChannel(dst)).PutBlocks();
    putReq->SetChunkId(request->GetChunkId());
    putReq->SetStartBlockIndex(start);
    putReq->SetStartOffset(session->GetBlock(start)->GetKey().Offset);

    for (i32 i = start; i <= end; ++i) {
        putReq->Attachments().push_back(session->GetBlock(i)->GetData());
    }

    putReq->Invoke()->Subscribe(FromMethod(&TChunkHolder::PutBlocksCallback, this, context));

    LOG_DEBUG("Chunk %s: putting blocks to %s (BlockCount: %d, StartBlockIndex: %d, StartOffset: %" PRId64 ")",
        ~TGuid::FromProto(putReq->GetChunkId()).ToString(),
        ~request->GetDestination(),
        putReq->Attachments().ysize(), 
        putReq->GetStartBlockIndex(), 
        putReq->GetStartOffset());
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, FlushBlocks)
{
    UNUSED(response);

    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());

    LOG_DEBUG("Chunk %s: flushing blocks %d-%d",
        ~StringFromGuid(chunkId), request->GetStartBlockIndex(), request->GetEndBlockIndex());

    TSessionPtr session = GetSession(chunkId);
    CheckLease(LeaseManager->RenewLease(session->GetLease()), chunkId);
    session->FlushBlocks(context);
}

RPC_SERVICE_METHOD_IMPL(TChunkHolder, GetBlocks)
{
    TChunkId chunkId = TGuid::FromProto(request->GetChunkId());
    int blockCount = static_cast<int>(request->BlocksSize());
    
    LOG_DEBUG("Chunk %s: getting %d blocks (IsPrecharge: %d)",
        ~StringFromGuid(chunkId),
        blockCount,
        request->GetIsPrecharge());

    TLocationMap::iterator locationIt =  LocationMap.find(chunkId);
    if (locationIt == LocationMap.end()) 
        ythrow TServiceException() << "Chunk " << StringFromGuid(chunkId) <<
            " does not exists";

    yvector<size_t> uncached;
    response->Attachments().resize(blockCount);
    for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
        const NRpcChunkHolder::TBlockInfo& bi = request->GetBlocks(blockIndex);
        LOG_DEBUG("Chunk %s: getting block with offset %d",
            ~StringFromGuid(chunkId),
            bi.GetOffset());
        TBlockId id(chunkId, bi.GetOffset());
        TCachedBlock::TPtr block = BlockCache->Get(id);
        if (~block == NULL) {
            uncached.push_back(blockIndex);
        } else {
            response->Attachments()[blockIndex] = block->GetData();
        }
     }

    if (!uncached.empty()) {
        i32 location = locationIt->second;
        Stroka fileName = GetFileName(chunkId, location);
        IOQueues[location]->Invoke(
            // ToDo: wrap with completion exception handling
            FromMethod(&TChunkHolder::ReadBlocks, this, context, fileName, uncached));
        LOG_DEBUG("Chunk %s: getting blocks, %d uncached",
            ~StringFromGuid(chunkId), uncached.ysize());
    } else {
        context->Reply();
        LOG_DEBUG("Chunk %s: getting blocks, all from cache",
            ~StringFromGuid(chunkId));
    }
}

////////////////////////////////////////////////////////////////////////////////

}

