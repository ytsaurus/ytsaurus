#include "nbd_chunk_handler.h"

#include "location.h"
#include "private.h"

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

constinit const auto Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EState,
    (Uninitialized)
    (Initialized)
    (Initializing)
    (Finalizing)
);

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
        // Acquire a writer guard.
        return TAsyncLockWriterGuard::Acquire(&Lock_)
            .ApplyUnique(BIND([this, this_ = MakeStrong(this)] (TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>&& guard) {
                if (std::exchange(State_, EState::Initializing) != EState::Uninitialized) {
                    YT_LOG_WARNING("Creating not uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, State: %v",
                        ChunkId_,
                        ChunkPath_,
                        ChunkSize_,
                        State_);

                    THROW_ERROR_EXCEPTION("Creating not uninitialized nbd chunk handler")
                        << TErrorAttribute("chunk_id", ChunkId_)
                        << TErrorAttribute("chunk_path", ChunkPath_)
                        << TErrorAttribute("chunk_size", ChunkSize_)
                        << TErrorAttribute("state", State_);
                }

                auto openFuture = IOEngine_->Open(
                    {.Path = ChunkPath_, .Mode = RdWr|CreateAlways},
                    WorkloadDescriptor_.Category);

                return openFuture.Apply(BIND([guard = std::move(guard), this, this_ = MakeStrong(this)] (const TIOEngineHandlePtr& ioEngineHandle) {
                    auto resizeFuture = IOEngine_->Resize({
                        .Handle = ioEngineHandle,
                        .Size = ChunkSize_},
                        WorkloadDescriptor_.Category);

                    return resizeFuture
                        .Apply(BIND([guard = std::move(guard), ioEngineHandle, this, this_ = MakeStrong(this)] () {
                            IOEngineHandle_ = std::move(ioEngineHandle);
                            State_ = EState::Initialized;
                            return;
                        })
                        .AsyncVia(Invoker_));
                })
                .AsyncVia(Invoker_));
            })
            .AsyncVia(Invoker_));
    }

    //! Close NBD file handler and remove NBD chunk file.
    TFuture<void> Destroy() override
    {
        // Acquire a writer guard.
        return TAsyncLockWriterGuard::Acquire(&Lock_)
            .ApplyUnique(BIND([this, this_ = MakeStrong(this)] (TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>&& guard) {
                if (std::exchange(State_, EState::Finalizing) != EState::Initialized) {
                    YT_LOG_WARNING("Destroying not initialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, State: %v",
                        ChunkId_,
                        ChunkPath_,
                        ChunkSize_,
                        State_);

                    THROW_ERROR_EXCEPTION("Destroying not initialized nbd chunk handler")
                        << TErrorAttribute("chunk_id", ChunkId_)
                        << TErrorAttribute("chunk_path", ChunkPath_)
                        << TErrorAttribute("chunk_size", ChunkSize_)
                        << TErrorAttribute("state", State_);
                }

                auto closeFuture = IOEngine_->Close(
                    {.Handle = IOEngineHandle_, .Size = ChunkSize_},
                    WorkloadDescriptor_.Category);

                return closeFuture.Apply(BIND([guard = std::move(guard), this, this_ = MakeStrong(this)] (const TCloseResponse&) {
                    State_ = EState::Uninitialized;
                    NFs::Remove(ChunkPath_);
                })
                .AsyncVia(Invoker_));
            })
            .AsyncVia(Invoker_));
    }

    //! Read size bytes from NBD chunk at offset.
    TFuture<NChunkClient::TBlock> Read(i64 offset, i64 length, ui64 cookie) override
    {
        // Acquire a reader guard.
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .ApplyUnique(BIND([=, this, this_ = MakeStrong(this)] (TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockReaderTraits>>&& guard) {
                if (State_ != EState::Initialized) {
                    YT_LOG_WARNING("Read from uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, Offset: %v, Length: %v, Cookie: %v, State: %v",
                        ChunkId_,
                        ChunkPath_,
                        ChunkSize_,
                        offset,
                        length,
                        cookie,
                        State_);

                    THROW_ERROR_EXCEPTION("Read from uninitialized nbd chunk handler")
                        << TErrorAttribute("chunk_id", ChunkId_)
                        << TErrorAttribute("chunk_path", ChunkPath_)
                        << TErrorAttribute("chunk_size", ChunkSize_)
                        << TErrorAttribute("offset", offset)
                        << TErrorAttribute("length", length)
                        << TErrorAttribute("cookie", cookie)
                        << TErrorAttribute("state", State_);
                }

                if (offset + length > ChunkSize_) {
                    THROW_ERROR_EXCEPTION("Read is out of range")
                        << TErrorAttribute("chunk_id", ChunkId_)
                        << TErrorAttribute("chunk_path", ChunkPath_)
                        << TErrorAttribute("chunk_size", ChunkSize_)
                        << TErrorAttribute("offset", offset)
                        << TErrorAttribute("length", length)
                        << TErrorAttribute("cookie", cookie)
                        << TErrorAttribute("state", State_);
                }

                // Throttle disk read.
                auto throttleFuture = ReadThrottler_->Throttle(length);

                // Perform read and return result.
                return throttleFuture.Apply(BIND([=, guard = std::move(guard), this, this_ = MakeStrong(this)] () {
                    auto readFuture = IOEngine_->Read(
                        {{.Handle = IOEngineHandle_, .Offset = offset, .Size = length}},
                        WorkloadDescriptor_.Category,
                        GetRefCountedTypeCookie<TNbdChunkReaderBufferTag>());

                    return readFuture.Apply(BIND([guard = std::move(guard)] (const TReadResponse& response) {
                        YT_VERIFY(response.OutputBuffers.size() == 1);
                        return NChunkClient::TBlock(response.OutputBuffers[0]);
                    }).AsyncVia(Invoker_));
                })
                .AsyncVia(Invoker_));
            })
            .AsyncVia(Invoker_));
    }

    // ! Write buffer to NBD chunk at offset.
    TFuture<NIO::TIOCounters> Write(i64 offset, const TBlock& block, ui64 cookie) override
    {
        // Acquire a reader guard.
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .ApplyUnique(BIND([=, this, this_ = MakeStrong(this)] (TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockReaderTraits>>&& guard) {
                if (State_ != EState::Initialized) {
                    YT_LOG_WARNING("Write to uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, Offset: %v, Length: %v, Cookie: %v, State: %v",
                        ChunkId_,
                        ChunkPath_,
                        ChunkSize_,
                        offset,
                        block.Size(),
                        cookie,
                        State_);

                    THROW_ERROR_EXCEPTION("Write to uninitialized nbd chunk handler")
                        << TErrorAttribute("chunk_id", ChunkId_)
                        << TErrorAttribute("chunk_path", ChunkPath_)
                        << TErrorAttribute("chunk_size", ChunkSize_)
                        << TErrorAttribute("offset", offset)
                        << TErrorAttribute("length", block.Size())
                        << TErrorAttribute("cookie", cookie)
                        << TErrorAttribute("state", State_);
                }

                if (offset + std::ssize(block.Data) > ChunkSize_) {
                    THROW_ERROR_EXCEPTION("Write is out of range")
                        << TErrorAttribute("chunk_id", ChunkId_)
                        << TErrorAttribute("chunk_path", ChunkPath_)
                        << TErrorAttribute("chunk_size", ChunkSize_)
                        << TErrorAttribute("offset", offset)
                        << TErrorAttribute("length", block.Size())
                        << TErrorAttribute("cookie", cookie)
                        << TErrorAttribute("state", State_);
                }

                // Throttle disk write.
                auto throttleFuture = WriteThrottler_->Throttle(block.Data.Size());

                // Perform write and return result.
                return throttleFuture.Apply(BIND([=, guard = std::move(guard), this, this_ = MakeStrong(this)] () {
                    auto writeFuture = IOEngine_->Write(
                        {.Handle = IOEngineHandle_, .Offset = offset, .Buffers = {block.Data}},
                        WorkloadDescriptor_.Category);

                        return writeFuture.Apply(BIND([guard = std::move(guard)] (const TWriteResponse& response) {
                            return NIO::TIOCounters {
                                .Bytes = response.WrittenBytes,
                                .IORequests = response.IOWriteRequests,
                            };
                        })
                        .AsyncVia(Invoker_));
                })
                .AsyncVia(Invoker_));
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

    EState State_ = EState::Uninitialized;
    // This lock is needed to create and destory NBD chunk with exclusive access.
    TAsyncReaderWriterLock Lock_;
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
