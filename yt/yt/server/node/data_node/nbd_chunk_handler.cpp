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
            .AsUnique()
            .Apply(
                BIND([this, this_ = MakeStrong(this)] (TWriteLockPtr&& guard) {
                    auto oldState = std::exchange(State_, EState::Initializing);
                    if (oldState != EState::Uninitialized) {
                        YT_LOG_WARNING("Creating not uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, State: %v)",
                            ChunkId_,
                            ChunkPath_,
                            ChunkSize_,
                            oldState);

                        THROW_ERROR_EXCEPTION("Creating not uninitialized nbd chunk handler")
                            << TErrorAttribute("chunk_id", ChunkId_)
                            << TErrorAttribute("chunk_path", ChunkPath_)
                            << TErrorAttribute("chunk_size", ChunkSize_)
                            << TErrorAttribute("state", oldState);
                    }

                    auto openFuture = IOEngine_->Open(
                        {.Path = ChunkPath_, .Mode = RdWr|CreateAlways},
                        WorkloadDescriptor_.Category);

                    return openFuture
                        .AsUnique()
                        .Apply(
                            BIND([guard = std::move(guard)] (TIOEngineHandlePtr&& ioEngineHandle) {
                                // Return both guard and handler.
                                return std::make_pair(std::move(guard), ioEngineHandle);
                            })
                            .AsyncVia(Invoker_));

                })
                .AsyncVia(Invoker_))
            .AsUnique()
            .Apply(
                BIND([this, this_ = MakeStrong(this)] (std::pair<TWriteLockPtr, TIOEngineHandlePtr>&& p) {
                    auto guard = std::move(p.first);
                    auto ioEngineHandle = std::move(p.second);

                    auto resizeFuture = IOEngine_->Resize({
                        .Handle = ioEngineHandle,
                        .Size = ChunkSize_},
                        WorkloadDescriptor_.Category);

                    return resizeFuture.Apply(
                        BIND([guard = std::move(guard), ioEngineHandle] {
                            return std::make_pair(std::move(guard), ioEngineHandle);
                        })
                        .AsyncVia(Invoker_));

                })
                .AsyncVia(Invoker_))
            .AsUnique()
            .Apply(
                BIND([this, this_ = MakeStrong(this)] (std::pair<TWriteLockPtr, TIOEngineHandlePtr>&& p) {
                    IOEngineHandle_ = std::move(p.second);
                    State_ = EState::Initialized;
                    // Guard is released here when it goes out of scope.
                })
                .AsyncVia(Invoker_));
    }

    //! Close NBD file handler and remove NBD chunk file.
    TFuture<void> Destroy() override
    {
        // Acquire a writer guard.
        return TAsyncLockWriterGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([this, this_ = MakeStrong(this)] (TWriteLockPtr&& guard) {
                    auto oldState = std::exchange(State_, EState::Finalizing);
                    if (oldState != EState::Initialized) {
                        YT_LOG_WARNING("Destroying not initialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, State: %v)",
                            ChunkId_,
                            ChunkPath_,
                            ChunkSize_,
                            oldState);

                        THROW_ERROR_EXCEPTION("Destroying not initialized nbd chunk handler")
                            << TErrorAttribute("chunk_id", ChunkId_)
                            << TErrorAttribute("chunk_path", ChunkPath_)
                            << TErrorAttribute("chunk_size", ChunkSize_)
                            << TErrorAttribute("state", oldState);
                    }

                    auto closeFuture = IOEngine_->Close(
                        {.Handle = IOEngineHandle_, .Size = ChunkSize_},
                        WorkloadDescriptor_.Category);

                    return closeFuture
                        .AsUnique()
                        .Apply(
                            BIND([guard = std::move(guard)] (TCloseResponse&&) {
                                return std::move(guard);
                            })
                            .AsyncVia(Invoker_));
                })
                .AsyncVia(Invoker_))
            .AsUnique()
            .Apply(
                BIND([this, this_ = MakeStrong(this)] (TWriteLockPtr&&) {
                    IOEngineHandle_.Reset();
                    State_ = EState::Uninitialized;

                    try {
                        NFs::Remove(ChunkPath_);
                        YT_LOG_DEBUG("Destroyed nbd chunk handler (ChunkId: %v, ChunkPath: %v)",
                            ChunkId_,
                            ChunkPath_);
                    } catch (const std::exception& ex) {
                        YT_LOG_WARNING(ex, "Failed to remove nbd chunk file (ChunkId: %v, ChunkPath: %v)",
                            ChunkId_,
                            ChunkPath_);

                        throw;
                    }
                })
                .AsyncVia(Invoker_));
    }

    //! Read size bytes from NBD chunk at offset.
    TFuture<TBlock> Read(i64 offset, i64 length, ui64 cookie) override
    {
        // Acquire a reader guard.
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] (TReadLockPtr&& guard) {
                    if (State_ != EState::Initialized) {
                        YT_LOG_WARNING("Read from uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, Offset: %v, Length: %v, Cookie: %v, State: %v)",
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
                    return throttleFuture.Apply(
                        BIND([=, guard = std::move(guard), this, this_ = MakeStrong(this)] {
                            auto readFuture = IOEngine_->Read(
                                {{.Handle = IOEngineHandle_, .Offset = offset, .Size = length}},
                                WorkloadDescriptor_.Category,
                                GetRefCountedTypeCookie<TNbdChunkReaderBufferTag>());

                            return readFuture.Apply(
                                BIND([guard = std::move(guard)] (const TReadResponse& response) {
                                    YT_VERIFY(response.OutputBuffers.size() == 1);
                                    return TBlock(response.OutputBuffers[0]);
                                })
                                .AsyncVia(Invoker_));
                        })
                        .AsyncVia(Invoker_));
                })
                .AsyncVia(Invoker_));
    }

    //! Write buffer to NBD chunk at offset.
    TFuture<NIO::TIOCounters> Write(i64 offset, const TBlock& block, ui64 cookie) override
    {
        // Acquire a reader guard.
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this)] (TReadLockPtr&& guard) {
                    if (State_ != EState::Initialized) {
                        YT_LOG_WARNING("Write to uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, Offset: %v, Length: %v, Cookie: %v, State: %v)",
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
                    return throttleFuture.Apply(
                        BIND([=, guard = std::move(guard), this, this_ = MakeStrong(this)] {
                            auto writeFuture = IOEngine_->Write(
                                {.Handle = IOEngineHandle_, .Offset = offset, .Buffers = {block.Data}},
                                WorkloadDescriptor_.Category);

                            return writeFuture.Apply(
                                BIND([guard = std::move(guard)] (const TWriteResponse& response) {
                                    return NIO::TIOCounters {
                                        .Bytes = response.WrittenBytes,
                                        .IORequests = response.IOWriteRequests};
                                })
                                .AsyncVia(Invoker_));
                        })
                        .AsyncVia(Invoker_));
                })
                .AsyncVia(Invoker_));
    }

private:
    using TReadLockPtr = TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockReaderTraits>>;
    using TWriteLockPtr = TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>;

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
    // This lock is needed to create and destroy NBD chunk with exclusive access.
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
