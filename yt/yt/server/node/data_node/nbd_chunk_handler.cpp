#include "nbd_chunk_handler.h"

#include "location.h"
#include "private.h"

#include <yt/yt/server/lib/io/io_engine.h>
#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/system/fs.h>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NIO;
using namespace NProfiling;

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
        IInvokerPtr ioInvoker,
        IThroughputThrottlerPtr readNetThrottler,
        IThroughputThrottlerPtr writeNetThrottler)
    : ChunkSize_(chunkSize)
    , ChunkId_(chunkId)
    , WorkloadDescriptor_(std::move(workloadDescriptor))
    , StoreLocation_(std::move(storeLocation))
    , IOInvoker_(std::move(ioInvoker))
    , ChunkPath_(StoreLocation_->GetChunkPath(ChunkId_))
    , IOEngine_(StoreLocation_->GetIOEngine())
    , ReadStoreThrottler_(StoreLocation_->GetOutThrottler(WorkloadDescriptor_))
    , WriteStoreThrottler_(StoreLocation_->GetInThrottler(WorkloadDescriptor_))
    , ReadNetThrottler_(std::move(readNetThrottler))
    , WriteNetThrottler_(std::move(writeNetThrottler))
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
                                return std::make_pair(std::move(guard), std::move(ioEngineHandle));
                            }));
                }))
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
                        }));
                }))
            .AsUnique()
            .Apply(
                BIND([this, this_ = MakeStrong(this)] (std::pair<TWriteLockPtr, TIOEngineHandlePtr>&& p) {
                    IOEngineHandle_ = std::move(p.second);
                    State_ = EState::Initialized;
                    // Guard is released here when it goes out of scope.
                }));
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
                            }));
                }))
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
                .AsyncVia(IOInvoker_));
    }

    //! Read size bytes from NBD chunk at offset.
    TFuture<TBlock> Read(i64 offset, i64 length, ui64 cookie) override
    {
        YT_LOG_DEBUG("Started reading from NBD chunk (ChunkId: %v, Offset: %v, Length: %v, Cookie: %x)",
            ChunkId_,
            offset,
            length,
            cookie);

        // Acquire a reader guard.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        YT_LOG_WARNING("Read from uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, Offset: %v, Length: %v, Cookie: %x, State: %v)",
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

                    // Throttle both network and disk read in parallel.
                    TWallTimer throttleTimer;
                    auto throttleFuture = AllSucceeded(std::vector<TFuture<void>>{
                        ReadNetThrottler_->Throttle(length),
                        ReadStoreThrottler_->Throttle(length)
                    });

                    // Perform read and return result.
                    return throttleFuture.Apply(
                        BIND([=, guard = std::move(guard), throttleTimer = std::move(throttleTimer), this, this_ = MakeStrong(this)] {
                            auto throttleDuration = throttleTimer.GetElapsedTime();

                            TWallTimer ioTimer;
                            auto readFuture = IOEngine_->Read(
                                {{.Handle = IOEngineHandle_, .Offset = offset, .Size = length}},
                                WorkloadDescriptor_.Category,
                                GetRefCountedTypeCookie<TNbdChunkReaderBufferTag>());

                            return readFuture.Apply(
                                BIND([=, guard = std::move(guard), ioTimer = std::move(ioTimer), this, this_ = MakeStrong(this)] (const TReadResponse& response) {
                                    auto ioDuration = ioTimer.GetElapsedTime();

                                    YT_LOG_DEBUG("Finished reading from NBD chunk (ChunkId: %v, Offset: %v, Length: %v, LockWaitDuration: %v, ThrottleDuration: %v, IODuration: %v, Cookie: %x)",
                                        ChunkId_,
                                        offset,
                                        length,
                                        lockWaitDuration,
                                        throttleDuration,
                                        ioDuration,
                                        cookie);

                                    YT_VERIFY(response.OutputBuffers.size() == 1);
                                    return TBlock(response.OutputBuffers[0]);
                                }));
                        }));
                }));
    }

    //! Read multiple non-contiguous ranges from NBD chunk in a single IO engine call.
    TFuture<std::vector<TBlock>> ReadBatch(
        const std::vector<TNbdReadSubrequest>& subrequests,
        ui64 cookie) override
    {
        i64 totalLength = 0;
        for (const auto& sub : subrequests) {
            totalLength += sub.Length;
        }

        YT_LOG_DEBUG("Started batch reading from NBD chunk (ChunkId: %v, SubrequestCount: %v, TotalLength: %v, Cookie: %x)",
            ChunkId_,
            subrequests.size(),
            totalLength,
            cookie);

        // Acquire a reader guard once for all subrequests.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) mutable {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        THROW_ERROR_EXCEPTION("ReadBatch from uninitialized nbd chunk handler")
                            << TErrorAttribute("chunk_id", ChunkId_)
                            << TErrorAttribute("state", State_);
                    }

                    for (const auto& sub : subrequests) {
                        if (sub.Offset + sub.Length > ChunkSize_) {
                            THROW_ERROR_EXCEPTION("ReadBatch subrequest is out of range")
                                << TErrorAttribute("chunk_id", ChunkId_)
                                << TErrorAttribute("offset", sub.Offset)
                                << TErrorAttribute("length", sub.Length)
                                << TErrorAttribute("chunk_size", ChunkSize_);
                        }
                    }

                    // Throttle once for total bytes.
                    TWallTimer throttleTimer;
                    auto throttleFuture = AllSucceeded(std::vector<TFuture<void>>{
                        ReadNetThrottler_->Throttle(totalLength),
                        ReadStoreThrottler_->Throttle(totalLength)
                    });

                    return throttleFuture.Apply(
                        BIND([=, guard = std::move(guard), throttleTimer = std::move(throttleTimer), this, this_ = MakeStrong(this)] () mutable {
                            auto throttleDuration = throttleTimer.GetElapsedTime();

                            // Build one TReadRequest per subrequest.
                            std::vector<NIO::TReadRequest> ioRequests;
                            ioRequests.reserve(subrequests.size());
                            for (const auto& sub : subrequests) {
                                ioRequests.push_back({
                                    .Handle = IOEngineHandle_,
                                    .Offset = sub.Offset,
                                    .Size = sub.Length,
                                });
                            }

                            TWallTimer ioTimer;
                            return IOEngine_->Read(
                                std::move(ioRequests),
                                WorkloadDescriptor_.Category,
                                GetRefCountedTypeCookie<TNbdChunkReaderBufferTag>())
                                .Apply(BIND([=, guard = std::move(guard), ioTimer = std::move(ioTimer), this, this_ = MakeStrong(this)] (const NIO::TReadResponse& response) {
                                    auto ioDuration = ioTimer.GetElapsedTime();

                                    YT_LOG_DEBUG("Finished batch reading from NBD chunk (ChunkId: %v, SubrequestCount: %v, TotalLength: %v, LockWaitDuration: %v, ThrottleDuration: %v, IODuration: %v, Cookie: %x)",
                                        ChunkId_,
                                        subrequests.size(),
                                        totalLength,
                                        lockWaitDuration,
                                        throttleDuration,
                                        ioDuration,
                                        cookie);

                                    YT_VERIFY(response.OutputBuffers.size() == subrequests.size());
                                    std::vector<TBlock> blocks;
                                    blocks.reserve(response.OutputBuffers.size());
                                    for (const auto& buf : response.OutputBuffers) {
                                        blocks.emplace_back(buf);
                                    }
                                    return blocks;
                                }));
                        }));
                }));
    }

    //! Write buffer to NBD chunk at offset.
    TFuture<NIO::TIOCounters> Write(i64 offset, const TBlock& block, ui64 cookie) override
    {
        YT_LOG_DEBUG("Started writing to NBD chunk (ChunkId: %v, Offset: %v, Length: %v, Cookie: %x)",
            ChunkId_,
            offset,
            block.Size(),
            cookie);

        // Acquire a reader guard.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        YT_LOG_WARNING("Write to uninitialized nbd chunk handler (ChunkId: %v, ChunkPath: %v, ChunkSize: %v, Offset: %v, Length: %v, Cookie: %x, State: %v)",
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

                    // Throttle both network and disk write in parallel.
                    TWallTimer throttleTimer;
                    auto throttleFuture = AllSucceeded(std::vector<TFuture<void>>{
                        WriteNetThrottler_->Throttle(block.Data.Size()),
                        WriteStoreThrottler_->Throttle(block.Data.Size())
                    });

                    // Perform write and return result.
                    return throttleFuture.Apply(
                        BIND([=, guard = std::move(guard), throttleTimer = std::move(throttleTimer), this, this_ = MakeStrong(this)] {
                            auto throttleDuration = throttleTimer.GetElapsedTime();

                            TWallTimer ioTimer;
                            auto writeFuture = IOEngine_->Write(
                                {.Handle = IOEngineHandle_, .Offset = offset, .Buffers = {block.Data}},
                                WorkloadDescriptor_.Category);

                            return writeFuture.Apply(
                                BIND([=, guard = std::move(guard), ioTimer = std::move(ioTimer), this, this_ = MakeStrong(this)] (const TWriteResponse& response) {
                                    auto ioDuration = ioTimer.GetElapsedTime();

                                    YT_LOG_DEBUG("Finished writing to NBD chunk (ChunkId: %v, Offset: %v, Length: %v, LockWaitDuration: %v, ThrottleDuration: %v, IODuration: %v, Cookie: %x)",
                                        ChunkId_,
                                        offset,
                                        block.Size(),
                                        lockWaitDuration,
                                        throttleDuration,
                                        ioDuration,
                                        cookie);

                                    return NIO::TIOCounters {
                                        .Bytes = response.WrittenBytes,
                                        .IORequests = response.IOWriteRequests};
                                }));
                        }));
                }));
    }

    //! Flush dirty data to disk (fsync).
    TFuture<void> Flush(ui64 cookie) override
    {
        YT_LOG_DEBUG("Started flushing NBD chunk (ChunkId: %v, Cookie: %x)",
            ChunkId_,
            cookie);

        // Acquire a reader guard so that Destroy() (which acquires the writer lock)
        // cannot close the file handle while a flush is in flight.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        THROW_ERROR_EXCEPTION("Flush on uninitialized nbd chunk handler")
                            << TErrorAttribute("chunk_id", ChunkId_)
                            << TErrorAttribute("state", State_);
                    }

                    TWallTimer ioTimer;
                    auto flushFuture = IOEngine_->FlushFile(
                        {.Handle = IOEngineHandle_, .Mode = NIO::EFlushFileMode::All},
                        WorkloadDescriptor_.Category);

                    return flushFuture.Apply(
                        BIND([=, guard = std::move(guard), ioTimer = std::move(ioTimer), this, this_ = MakeStrong(this)] (const NIO::TFlushFileResponse&) {
                            auto ioDuration = ioTimer.GetElapsedTime();

                            YT_LOG_DEBUG("Finished flushing NBD chunk (ChunkId: %v, LockWaitDuration: %v, IODuration: %v)",
                                ChunkId_,
                                lockWaitDuration,
                                ioDuration);
                        }));
                }));
    }

    //! Flush a specific range of data to disk (sync_file_range).
    TFuture<void> FlushRange(i64 offset, i64 size) override
    {
        YT_LOG_DEBUG("Started flushing NBD chunk range (ChunkId: %v, Offset: %v, Size: %v)",
            ChunkId_,
            offset,
            size);

        // Acquire a reader guard so that Destroy() (which acquires the writer lock)
        // cannot close the file handle while a flush is in flight.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        THROW_ERROR_EXCEPTION("FlushRange on uninitialized nbd chunk handler")
                            << TErrorAttribute("chunk_id", ChunkId_)
                            << TErrorAttribute("state", State_);
                    }

                    TWallTimer ioTimer;
                    auto flushFuture = IOEngine_->FlushFileRange(
                        {.Handle = IOEngineHandle_, .Offset = offset, .Size = size},
                        WorkloadDescriptor_.Category);

                    return flushFuture.Apply(
                        BIND([=, guard = std::move(guard), ioTimer = std::move(ioTimer), this, this_ = MakeStrong(this)] (const NIO::TFlushFileRangeResponse&) {
                            auto ioDuration = ioTimer.GetElapsedTime();

                            YT_LOG_DEBUG("Finished flushing NBD chunk range (ChunkId: %v, Offset: %v, Size: %v, LockWaitDuration: %v, IODuration: %v)",
                                ChunkId_,
                                offset,
                                size,
                                lockWaitDuration,
                                ioDuration);
                        }));
                }));
    }

private:
    using TReadLockPtr = TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockReaderTraits>>;
    using TWriteLockPtr = TIntrusivePtr<TAsyncReaderWriterLockGuard<TAsyncLockWriterTraits>>;

    const i64 ChunkSize_;
    const TChunkId ChunkId_;
    const TWorkloadDescriptor WorkloadDescriptor_;
    const TStoreLocationPtr StoreLocation_;
    // Invoker for disk I/O requests (i.e. heavy storage operations).
    const IInvokerPtr IOInvoker_;
    const TString ChunkPath_;
    const IIOEnginePtr IOEngine_;
    TIOEngineHandlePtr IOEngineHandle_;
    const IThroughputThrottlerPtr ReadStoreThrottler_;
    const IThroughputThrottlerPtr WriteStoreThrottler_;
    const IThroughputThrottlerPtr ReadNetThrottler_;
    const IThroughputThrottlerPtr WriteNetThrottler_;

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
    IInvokerPtr ioInvoker,
    IThroughputThrottlerPtr readNetThrottler,
    IThroughputThrottlerPtr writeNetThrottler)
{
    return New<TNbdChunkHandler>(
        chunkSize,
        std::move(chunkId),
        std::move(workloadDescriptor),
        std::move(storeLocation),
        std::move(ioInvoker),
        std::move(readNetThrottler),
        std::move(writeNetThrottler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
