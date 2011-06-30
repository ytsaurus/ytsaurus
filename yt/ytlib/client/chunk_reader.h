#pragma once

#include "chunk.pb.h"
#include "user_indexes.h"
#include "io_channels.h"

#include "../holder/chunk_holder_rpc.h"
#include "../holder/chunk.h"
#include "../misc/common.h"
#include "../misc/lazy_ptr.h"
#include "../rpc/client.h"

#include <util/generic/set.h>

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

struct TChunkReaderConfig
{
    i64 MaxPipeSize;
    i64 PrechargeSize;
    i32 ReqSize; // Max sum size of blocks for one request

    TChunkReaderConfig()
        : MaxPipeSize(1024 * 1024 * 40)
        , ReqSize(1024 * 1024 * 8)
    {}
};

class TChunkReader
    : public TRefCountedBase
{

    struct TBlock;
    typedef TIntrusivePtr<TBlock> TBlockPtr;

    /*class TGetHeader;
    class TGetBlocks;
    typedef TIntrusivePtr<TGetBlocks> TGetBlocksPtr; 
     
    struct TGetBlocksLess
    {
        bool operator()(const TGetBlocksPtr& left, const TGetBlocksPtr& right) const;
    };*/

    static TLazyPtr<TActionQueue> ReaderThread;

    TAtomic PipeSize;
    TAtomic PrechargeSize;
     
    TChunkReaderConfig Config; 

    yvector<TReadChannel> Reads;
    yvector<size_t> ReadsToChunkChannels;
    TMsgChunkHeader ChunkHeader;
    i64 RowCount;
    yvector<const TMsgBlock*> BlocksQueue;
    // ToDo: think here
    //ymultiset<TGetBlocksPtr, TGetBlocksLess> DisorderReplies;

    // Prefetched blocks for ReadChannels
    // Communication between rpc calling thread and client thread
    TLockFreeQueue<TBlockPtr> BlockPipe;

    TChunkId ChunkId;
    TChannel Channel;
    ui64 ChunkSize;
    
    i32 StartGetIdx;
    i32 NextGetIdx;
    i32 NextReceiveIdx;
    i32 GetReqSize;
    i64 RowIdx;

    Event IsReady;
    TUserIndexes& UserIndexes;  
    i32 CurReadChannel;

private:
    // Fetching thread
    void ReadFixedHeader(TRef block);
    void ReadChunkHeader(TRef block);
    void FillBlocksQueue();
    void AddRead(size_t chunkChannelIdx, const TChannel& channel);
    void FetchBlocks();
 /*   void EnqueueBlocks(TGetBlocks* getBlocks); 
    void OnBlocksReceive(TGetBlocks* getBlocks); */

    // Client thread
    void ReadNextBlock(size_t r);

public:
    typedef TIntrusivePtr<TChunkReader> TPtr;

    // Client thread
    TChunkReader(TChunkId chunkId, TChannel channel, 
        ui64 chunkSize, Stroka nodeAddress, TUserIndexes& userIndexes);

    bool NextRow();
    bool NextColumn();
    size_t GetIndex();
    TValue GetValue();
};

} // namespace NYT
