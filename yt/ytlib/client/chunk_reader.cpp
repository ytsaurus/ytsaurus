#include "../misc/stdafx.h"
#include "chunk_reader.h"
#include "chunk_holder.pb.h"

#include "../misc/serialize.h"
#include "../logging/log.h"
#include "../actions/action_util.h"

#include <contrib/libs/protobuf/message.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace ::google::protobuf;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("IO");

////////////////////////////////////////////////////////////////////////////////

void ChannelFromMessage(const TMsgChannel& msg, TChannel& channel)
{
    for (int a = 0; a < msg.GetColumns().size(); ++a) {
        const Stroka& column = msg.GetColumns(a);
        channel( TValue(column.begin(), column.size()) );
    }

    for (int r = 0; r < msg.GetRanges().size(); ++r) {
        const TMsgRange& msgRange = msg.GetRanges(r);
        const Stroka& begin = msgRange.GetBegin();
        const Stroka& end = msgRange.GetEnd();
        channel( TRange( TValue(begin.begin(), begin.size()),
            TValue(end.begin(), end.size()) ) );
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TChunkReader::TBlock
    : public TRefCountedBase
{
    TBlob Data;
    ui64 Offset;

    TBlock(TRef data, ui64 offset)
        : Data(data.ToBlob())
        , Offset(offset)
    { }
};

////////////////////////////////////////////////////////////////////////////////
/*
class TChunkReader::TGetBlocks
    : public TMethodCallback<TReqGetBlocks, TRspGetBlocks>
{
    TChunkReader::TPtr Reader;
    i32 StartIdx, EndIdx;

public:
    TGetBlocks(i32 startIdx, i32 endIdx, TChunkReader* reader)
        : TMethodCallback<TReqGetBlocks, TRspGetBlocks>(TChunkReader::ReaderThread.Get())
        , Reader(reader)
        , StartIdx(startIdx)
        , EndIdx(endIdx)
    {
        Req->SetChunkId(ProtoGuidFromGuid(Reader->ChunkId));
        for (i32 i = startIdx; i < endIdx; ++i) {
            TBlockInfo* bi = Req->AddBlocks();
            // TODO: fix warning
            bi->SetOffset(Reader->BlocksQueue[i]->GetOffset());
            bi->SetSize(Reader->BlocksQueue[i]->GetSize());
        }
        
        Reader->Proxy->GetBlocks(RPC_PARAMS(this));
    }

    i32 GetStartIndex()
    {
        return StartIdx;
    }

    i32 GetEndIndex()
    {
        return EndIdx;
    }

    void Do() throw()
    {
        Reader->OnBlocksReceive(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

bool TChunkReader::TGetBlocksLess::operator()(const TGetBlocksPtr& left, const TGetBlocksPtr& right) const
{
    return left->GetReq()->GetBlocks(0).GetOffset() < 
        right->GetReq()->GetBlocks(0).GetOffset();
}

////////////////////////////////////////////////////////////////////////////////

typedef void (TChunkReader::*TReadHeaderMethod)(TRef);

class TChunkReader::TGetHeader
    : public TMethodCallback<TReqGetBlocks, TRspGetBlocks>
{
    TChunkReader::TPtr Reader;
    TReadHeaderMethod Callback;

public:
    TGetHeader(ui64 offset, ui64 size, TChunkReader* reader, TReadHeaderMethod callback)
        : TMethodCallback<TReqGetBlocks, TRspGetBlocks>(TChunkReader::ReaderThread.Get())
        , Reader(reader)
        , Callback(callback)
    {
        Req->SetChunkId(ProtoGuidFromGuid(Reader->ChunkId));
        TBlockInfo* bi = Req->AddBlocks();
        // TODO: fix warning
        bi->SetOffset(offset);
        // TODO: fix warning
        bi->SetSize(size);
        
        Reader->Proxy->GetBlocks(RPC_PARAMS(this));
    }

    void Do() throw() 
    {
        (Reader.Get()->*Callback)(Context->ResponseAttachments.front());
    }
};*/

////////////////////////////////////////////////////////////////////////////////
TLazyPtr<TActionQueue> TChunkReader::ReaderThread;

TChunkReader::TChunkReader(TChunkId chunkId, TChannel channel, 
    ui64 chunkSize, Stroka nodeAddress, TUserIndexes& userIndexes)
    : PipeSize(0)
    , PrechargeSize(0)
    , ChunkId(chunkId)
    , Channel(channel)
    , ChunkSize(chunkSize)
    , StartGetIdx(0)
    , NextGetIdx(0)
    , NextReceiveIdx(0)
    , GetReqSize(0)
    , RowIdx(-1)
    , UserIndexes(userIndexes)
    , CurReadChannel(-1)
{
    IsReady.Reset();
//    Proxy.Reset(new TChunkHolderProxy<NRpc::THelperNet>(nodeAddress));
    i64 size = sizeof(TFixedChunkFooter);
    i64 offset = chunkSize - size;
//    new TGetHeader(offset, size, this, &TChunkReader::ReadFixedHeader);
    // ToDo: call get header
}

void TChunkReader::ReadFixedHeader(TRef block)
{
    YASSERT(sizeof(TFixedChunkFooter) == block.Size());
    TFixedChunkFooter* fixed = reinterpret_cast<TFixedChunkFooter*>(block.Begin());
    // ToDo: call get blocks
    //new TGetHeader(fixed->HeaderOffset, fixed->HeaderSize, 
    //    this, &TChunkReader::ReadChunkHeader);
}

void TChunkReader::ReadChunkHeader(TRef block)
{
    io::ArrayInputStream in(block.Begin(), block.Size());
    if (!ChunkHeader.ParseFromZeroCopyStream(&in))
        LOG_FATAL("cannot read chunk header");

    RowCount = (size_t)ChunkHeader.GetRowCount();
    TChannel uncovered = Channel; // Columns and ranges uncovered by prev channels

    for (size_t c = 0; c < ChunkHeader.ChannelsSize() - 1; ++c) {
        const TMsgChunkChannel& msgChunkChannel = ChunkHeader.GetChannels(c);
        const TMsgChannel& msgChannel = msgChunkChannel.GetChannel();

        TChannel channel;
        ChannelFromMessage(msgChannel, channel);

        // TODO: more complex analysis
        if (channel.Match(uncovered)) {
            AddRead(c, uncovered);
            uncovered -= channel;
        }
    }

    // Misc channel
    if (!uncovered.IsEmpty())
        AddRead(ChunkHeader.ChannelsSize() - 1, uncovered);

    FillBlocksQueue();
    FetchBlocks();
}

void TChunkReader::FillBlocksQueue()
{
    yvector<i32> nextBlock(Reads.ysize(), 0);
    yvector<i32> readRows(Reads.ysize(), 0);
    while (true) {
        i32 minIdx = 0;
        i32 minRow = readRows[0];
        for (i32 c = 1; c < readRows.ysize(); ++c) {
            if (minRow > readRows[c]) {
                minRow = readRows[c];
                minIdx = c;
            }
        }
        
        if (minRow == RowCount)
            break;

        size_t blockIdx = nextBlock[minIdx];
        size_t channelIdx = ReadsToChunkChannels[minIdx];

        const TMsgChunkChannel& msgChunkChannel = ChunkHeader.GetChannels(channelIdx);
        const TMsgBlock& msgBlock = msgChunkChannel.GetBlocks(blockIdx);
        BlocksQueue.push_back(&msgBlock);
        readRows[minIdx] = msgBlock.GetLastRowIndex();
        ++nextBlock[minIdx];
    }
}

void TChunkReader::AddRead(size_t chunkChannelIdx, const TChannel& channel) 
{
    ReadsToChunkChannels.push_back(chunkChannelIdx);
    Reads.push_back();
    TReadChannel& read = Reads.back();
    read.SetChannel(channel);
}

void TChunkReader::FetchBlocks() 
{
    // Create and send Get requests
    while (PipeSize < Config.MaxPipeSize) {
        if (GetReqSize < Config.ReqSize && NextGetIdx < BlocksQueue.ysize()) {
            const TMsgBlock* msgBlock = BlocksQueue[NextGetIdx];
            GetReqSize += msgBlock->GetSize();
            PipeSize += msgBlock->GetSize();
            // PrechargeBufferSize -= msgBlock.GetSize();
            ++NextGetIdx;
        } else if (StartGetIdx < NextGetIdx) {
            // Send Get Request
            // ToDo: call getBlocks rpc
            //new TGetBlocks(StartGetIdx, NextGetIdx, this);
            GetReqSize = 0;
            StartGetIdx = NextGetIdx;
        } else
            break;
    }

/*
    if (StartPrechargeIdx < StartGetIdx) {
        StartPrechargeIdx = StartGetIdx;
    }

    while (PrechargeSize < Config.PrechargeSize) {
        if (GetSize < Config.RspSize) {
            const TMsgBlock* msgBlock = BlockQueue[NextGetIdx];
            GetBlocksSize += msgBlock.GetSize();
            BufferSize += msgBlock.GetSize();
            PrechargeSize -= msgBlock.GetSize();
            ++NextGetBlockIdx;
        } else {
            // Send Get Request
            new TGetBlocksCall(StartGetBlocksIdx, NextGetBlocksIdx);
            GetBlocksSize = 0;
            StartGetBlocksIdx = NextGetBlocksIdx;
        }
    }
*/
}
/*
void TChunkReader::EnqueueBlocks(TGetBlocks* getBlocks)
{
    NextReceiveIdx = getBlocks->GetEndIndex();

    const yvector<TRef>& blocks = 
        getBlocks->GetContext()->ResponseAttachments;
    for (size_t i = 0; i < blocks.size(); ++i) {
        const TBlockInfo& bi = getBlocks->GetReq()->GetBlocks(i);
        TBlockPtr block = new TBlock(blocks[i], bi.GetOffset());
        BlockPipe.Enqueue(block);
    }
    IsReady.Signal();
}

void TChunkReader::OnBlocksReceive(TGetBlocks* getBlocks)
{
    // ToDo: impl disorder replies
   if (getBlocks->GetStartIndex() == NextReceiveIdx) {
        EnqueueBlocks(getBlocks);
        while (DisorderReplies.size()) {
            getBlocks = DisorderReplies.begin()->Get();
            if (getBlocks->GetStartIndex() != NextReceiveIdx)
                break;

            EnqueueBlocks(getBlocks);
            DisorderReplies.erase(DisorderReplies.begin());
        } 
    } else
        DisorderReplies.insert(getBlocks);
        
}*/

void TChunkReader::ReadNextBlock(size_t r)
{
    IsReady.Reset();
    if (BlockPipe.IsEmpty())
        IsReady.Wait();
    IsReady.Signal();

    TBlockPtr b;
    BlockPipe.Dequeue(&b);
    Reads[r].SetBlock(&(b->Data), UserIndexes);
    AtomicAdd(PipeSize, -(b->Data.ysize()));
}

bool TChunkReader::NextRow()
{
    IsReady.Wait();
    CurReadChannel = -1;

    if (RowIdx < RowCount)
        ++RowIdx;

    if (RowIdx < RowCount) {
        for (size_t r = 0; r < Reads.size(); ++r) {
            TReadChannel& read = Reads[r];
            if (read.NeedsBlock()) {
                ReadNextBlock(r);
            } else
                read.NextRow();
        }
        if (PipeSize < Config.MaxPipeSize) {
            ReaderThread->Invoke(FromMethod(
                &TChunkReader::FetchBlocks, this));
        }
    } else 
        return false;
    return true;
}

bool TChunkReader::NextColumn()
{
    IsReady.Wait();
    if (CurReadChannel < 0)
        CurReadChannel = 0;
    else
        Reads[CurReadChannel].NextColumn();

    while (Reads[CurReadChannel].EndColumn()) {
        if (CurReadChannel < Reads.ysize() - 1)
            ++CurReadChannel;
        else
            return false;
    }
    return true;
}

size_t TChunkReader::GetIndex()
{
    IsReady.Wait();
    YASSERT(CurReadChannel >= 0);
    return Reads[CurReadChannel].GetIndex();
}

TValue TChunkReader::GetValue()
{
    IsReady.Wait();
    YASSERT(CurReadChannel >= 0);
    return Reads[CurReadChannel].GetValue();
}
}
