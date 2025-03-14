#include "nbd_chunk_handler.h"

#include "location.h"

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <util/system/fs.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////////

struct TNbdChunkReaderBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TNbdChunkHandler
    : public INbdChunkHandler
{
public:
    TNbdChunkHandler(
        i64 chunkSize,
        TChunkId chunkId,
        TWorkloadDescriptor workloadDescriptor,
        TStoreLocationPtr storeLocation,
        IInvokerPtr invoker)
    : ChunkSize_(chunkSize)
    , ChunkId_(chunkId)
    , WorkloadDescriptor_(std::move(workloadDescriptor))
    , StoreLocation_(std::move(storeLocation))
    , Invoker_(std::move(invoker))
    , ChunkPath_(StoreLocation_->GetChunkPath(ChunkId_))
    , IOEngine_(StoreLocation_->GetIOEngine())
    , ReadThrottler_(StoreLocation_->GetInThrottler(WorkloadDescriptor_))
    , WriteThrottler_(StoreLocation_->GetOutThrottler(WorkloadDescriptor_))
    { }

    //! Open NBD file handler and create NBD chunk file.
    TFuture<void> Create() override
    {
        auto future = IOEngine_->Open(
            {.Path = ChunkPath_, .Mode = RdWr|CreateAlways},
            WorkloadDescriptor_.Category);

        return future.Apply(BIND([this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& ioEngineHandle) {
            IOEngineHandle_ = ioEngineHandle;
            return IOEngine_->Resize({
                .Handle = IOEngineHandle_,
                .Size = ChunkSize_},
                WorkloadDescriptor_.Category);
        })
        .AsyncVia(Invoker_));
    }

    //! Close NBD file handler and remove NBD chunk file.
    TFuture<void> Destroy() override
    {
        auto future = IOEngine_->Close(
            {.Handle = IOEngineHandle_, .Size = ChunkSize_},
            WorkloadDescriptor_.Category);

        return future.Apply(BIND([this, this_ = MakeStrong(this)] (const IIOEngine::TCloseResponse&) {
            NFs::Remove(ChunkPath_);
        })
        .AsyncVia(Invoker_));
    }

    //! Read size bytes from NBD chunk at offset.
    TFuture<NChunkClient::TBlock> Read(i64 offset, i64 length) override
    {
        if (offset + length > ChunkSize_) {
            THROW_ERROR_EXCEPTION("Read is out of range")
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("length", length)
                << TErrorAttribute("chunk_size", ChunkSize_);
        }

        // Throttle read.
        auto throttleFuture = ReadThrottler_->Throttle(length);

        // Perform read and return result.
        return throttleFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] () {
            return IOEngine_->Read(
                {{.Handle = IOEngineHandle_, .Offset = offset, .Size = length}},
                WorkloadDescriptor_.Category,
                GetRefCountedTypeCookie<TNbdChunkReaderBufferTag>());
        })
        .AsyncVia(Invoker_))
        .Apply(BIND([] (const IIOEngine::TReadResponse& response) {
            YT_VERIFY(response.OutputBuffers.size() == 1);
            return NChunkClient::TBlock(response.OutputBuffers[0]);
        })
        .AsyncVia(Invoker_));
    }

    // ! Write buffer to NBD chunk at offset.
    TFuture<NIO::TIOCounters> Write(i64 offset, const TBlock& block) override
    {
        if (offset + std::ssize(block.Data) > ChunkSize_) {
            THROW_ERROR_EXCEPTION("Write is out of range")
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("length", block.Size())
                << TErrorAttribute("chunk_size", ChunkSize_);
        }

        // Throttle write.
        auto throttleFuture = WriteThrottler_->Throttle(block.Data.Size());

        // Perform write and return result.
        return throttleFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] () {
            return IOEngine_->Write(
                {.Handle = IOEngineHandle_, .Offset = offset, .Buffers = {block.Data}},
                WorkloadDescriptor_.Category);
        })
        .AsyncVia(Invoker_))
        .Apply(BIND([] (const IIOEngine::TWriteResponse& response) {
            return NIO::TIOCounters {
                .Bytes = response.WrittenBytes,
                .IORequests = response.IOWriteRequests,
            };
        })
        .AsyncVia(Invoker_));
    }

private:
    const i64 ChunkSize_;
    const TChunkId ChunkId_;
    const TWorkloadDescriptor WorkloadDescriptor_;
    const TStoreLocationPtr StoreLocation_;
    const IInvokerPtr Invoker_;
    const TString ChunkPath_;
    const IIOEnginePtr IOEngine_;
    TIOEngineHandlePtr IOEngineHandle_;
    const IThroughputThrottlerPtr ReadThrottler_;
    const IThroughputThrottlerPtr WriteThrottler_;
};

////////////////////////////////////////////////////////////////////////////////

INbdChunkHandlerPtr CreateNbdChunkHandler(
    i64 chunkSize,
    TChunkId chunkId,
    TWorkloadDescriptor workloadDescriptor,
    TStoreLocationPtr storeLocation,
    IInvokerPtr invoker)
{
    return New<TNbdChunkHandler>(
        chunkSize,
        std::move(chunkId),
        std::move(workloadDescriptor),
        std::move(storeLocation),
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
