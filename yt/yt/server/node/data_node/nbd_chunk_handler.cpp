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
                        YT_TLOG_WARNING("Creating not uninitialized NBD chunk handler")
                            .With("ChunkId", ChunkId_)
                            .With("ChunkPath", ChunkPath_)
                            .With("ChunkSize", ChunkSize_)
                            .With("State", oldState);

                        THROW_ERROR_EXCEPTION("Creating not uninitialized NBD chunk handler")
                            .With("chunk_id", ChunkId_)
                            .With("chunk_path", ChunkPath_)
                            .With("chunk_size", ChunkSize_)
                            .With("state", oldState);
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
                        YT_TLOG_WARNING("Destroying not initialized NBD chunk handler")
                            .With("ChunkId", ChunkId_)
                            .With("ChunkPath", ChunkPath_)
                            .With("ChunkSize", ChunkSize_)
                            .With("State", oldState);

                        THROW_ERROR_EXCEPTION("Destroying not initialized NBD chunk handler")
                            .With("chunk_id", ChunkId_)
                            .With("chunk_path", ChunkPath_)
                            .With("chunk_size", ChunkSize_)
                            .With("state", oldState);
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
                        YT_TLOG_DEBUG("Destroyed NBD chunk handler")
                            .With("ChunkId", ChunkId_)
                            .With("ChunkPath", ChunkPath_);
                    } catch (const std::exception& ex) {
                        YT_TLOG_WARNING("Failed to remove NBD chunk file")
                            .With("ChunkId", ChunkId_)
                            .With("ChunkPath", ChunkPath_)
                            .With(ex);

                        throw;
                    }
                })
                .AsyncVia(IOInvoker_));
    }

    //! Read size bytes from NBD chunk at offset.
    TFuture<TBlock> Read(i64 offset, i64 length, ui64 cookie) override
    {
        YT_TLOG_DEBUG("Started reading from NBD chunk")
            .With("ChunkId", ChunkId_)
            .With("Offset", offset)
            .With("Length", length)
            .WithFormat("Cookie", "%x", cookie);

        // Acquire a reader guard.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        YT_TLOG_WARNING("Read from uninitialized NBD chunk handler")
                            .With("ChunkId", ChunkId_)
                            .With("ChunkPath", ChunkPath_)
                            .With("ChunkSize", ChunkSize_)
                            .With("Offset", offset)
                            .With("Length", length)
                            .WithFormat("Cookie", "%x", cookie)
                            .With("State", State_);

                        THROW_ERROR_EXCEPTION("Read from uninitialized NBD chunk handler")
                            .With("chunk_id", ChunkId_)
                            .With("chunk_path", ChunkPath_)
                            .With("chunk_size", ChunkSize_)
                            .With("offset", offset)
                            .With("length", length)
                            .With("cookie", cookie)
                            .With("state", State_);
                    }

                    if (offset + length > ChunkSize_) {
                        THROW_ERROR_EXCEPTION("Read is out of range")
                            .With("chunk_id", ChunkId_)
                            .With("chunk_path", ChunkPath_)
                            .With("chunk_size", ChunkSize_)
                            .With("offset", offset)
                            .With("length", length)
                            .With("cookie", cookie)
                            .With("state", State_);
                    }

                    // Throttle both network and disk read in parallel.
                    TWallTimer throttleTimer;
                    auto throttleFuture = AllSucceeded(std::vector<TFuture<void>>{
                        ReadNetThrottler_->Throttle(length),
                        ReadStoreThrottler_->Throttle(length),
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

                                    YT_TLOG_DEBUG("Finished reading from NBD chunk")
                                        .With("ChunkId", ChunkId_)
                                        .With("Offset", offset)
                                        .With("Length", length)
                                        .With("LockWaitDuration", lockWaitDuration)
                                        .With("ThrottleDuration", throttleDuration)
                                        .With("IODuration", ioDuration)
                                        .WithFormat("Cookie", "%x", cookie);

                                    YT_VERIFY(std::ssize(response.OutputBuffers) == 1);
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

        YT_TLOG_DEBUG("Started batch reading from NBD chunk")
            .With("ChunkId", ChunkId_)
            .With("SubrequestCount", std::ssize(subrequests))
            .With("TotalLength", totalLength)
            .WithFormat("Cookie", "%x", cookie);

        // Acquire a reader guard once for all subrequests.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) mutable {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        THROW_ERROR_EXCEPTION("ReadBatch from uninitialized NBD chunk handler")
                            .With("chunk_id", ChunkId_)
                            .With("state", State_);
                    }

                    for (const auto& sub : subrequests) {
                        if (sub.Offset + sub.Length > ChunkSize_) {
                            THROW_ERROR_EXCEPTION("ReadBatch subrequest is out of range")
                                .With("chunk_id", ChunkId_)
                                .With("offset", sub.Offset)
                                .With("length", sub.Length)
                                .With("chunk_size", ChunkSize_);
                        }
                    }

                    // Throttle both network and disk read in parallel.
                    TWallTimer throttleTimer;
                    auto throttleFuture = AllSucceeded(std::vector<TFuture<void>>{
                        ReadNetThrottler_->Throttle(totalLength),
                        ReadStoreThrottler_->Throttle(totalLength),
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

                                    YT_TLOG_DEBUG("Finished batch reading from NBD chunk")
                                        .With("ChunkId", ChunkId_)
                                        .With("SubrequestCount", std::ssize(subrequests))
                                        .With("TotalLength", totalLength)
                                        .With("LockWaitDuration", lockWaitDuration)
                                        .With("ThrottleDuration", throttleDuration)
                                        .With("IODuration", ioDuration)
                                        .WithFormat("Cookie", "%x", cookie);

                                    YT_VERIFY(std::ssize(response.OutputBuffers) == std::ssize(subrequests));
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
        YT_TLOG_DEBUG("Started writing to NBD chunk")
            .With("ChunkId", ChunkId_)
            .With("Offset", offset)
            .With("Length", block.Size())
            .WithFormat("Cookie", "%x", cookie);

        // Acquire a reader guard.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        YT_TLOG_WARNING("Write to uninitialized NBD chunk handler")
                            .With("ChunkId", ChunkId_)
                            .With("ChunkPath", ChunkPath_)
                            .With("ChunkSize", ChunkSize_)
                            .With("Offset", offset)
                            .With("Length", block.Size())
                            .WithFormat("Cookie", "%x", cookie)
                            .With("State", State_);

                        THROW_ERROR_EXCEPTION("Write to uninitialized NBD chunk handler")
                            .With("chunk_id", ChunkId_)
                            .With("chunk_path", ChunkPath_)
                            .With("chunk_size", ChunkSize_)
                            .With("offset", offset)
                            .With("length", block.Size())
                            .With("cookie", cookie)
                            .With("state", State_);
                    }

                    if (offset + std::ssize(block.Data) > ChunkSize_) {
                        THROW_ERROR_EXCEPTION("Write is out of range")
                            .With("chunk_id", ChunkId_)
                            .With("chunk_path", ChunkPath_)
                            .With("chunk_size", ChunkSize_)
                            .With("offset", offset)
                            .With("length", block.Size())
                            .With("cookie", cookie)
                            .With("state", State_);
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

                                    YT_TLOG_DEBUG("Finished writing to NBD chunk")
                                        .With("ChunkId", ChunkId_)
                                        .With("Offset", offset)
                                        .With("Length", block.Size())
                                        .With("LockWaitDuration", lockWaitDuration)
                                        .With("ThrottleDuration", throttleDuration)
                                        .With("IODuration", ioDuration)
                                        .WithFormat("Cookie", "%x", cookie);

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
        YT_TLOG_DEBUG("Started flushing NBD chunk")
            .With("ChunkId", ChunkId_)
            .WithFormat("Cookie", "%x", cookie);

        // Acquire a reader guard so that Destroy() (which acquires the writer lock)
        // cannot close the file handle while a flush is in flight.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        THROW_ERROR_EXCEPTION("Flush on uninitialized NBD chunk handler")
                            .With("chunk_id", ChunkId_)
                            .With("state", State_);
                    }

                    TWallTimer ioTimer;
                    auto flushFuture = IOEngine_->FlushFile(
                        {.Handle = IOEngineHandle_, .Mode = NIO::EFlushFileMode::All},
                        WorkloadDescriptor_.Category);

                    return flushFuture.Apply(
                        BIND([=, guard = std::move(guard), ioTimer = std::move(ioTimer), this, this_ = MakeStrong(this)] (const NIO::TFlushFileResponse&) {
                            auto ioDuration = ioTimer.GetElapsedTime();

                            YT_TLOG_DEBUG("Finished flushing NBD chunk")
                                .With("ChunkId", ChunkId_)
                                .With("LockWaitDuration", lockWaitDuration)
                                .With("IODuration", ioDuration);
                        }));
                }));
    }

    //! Flush a specific range of data to disk (sync_file_range).
    TFuture<void> FlushRange(i64 offset, i64 size) override
    {
        YT_TLOG_DEBUG("Started flushing NBD chunk range")
            .With("ChunkId", ChunkId_)
            .With("Offset", offset)
            .With("Size", size);

        // Acquire a reader guard so that Destroy() (which acquires the writer lock)
        // cannot close the file handle while a flush is in flight.
        TWallTimer lockWaitTimer;
        return TAsyncLockReaderGuard::Acquire(&Lock_)
            .AsUnique()
            .Apply(
                BIND([=, this, this_ = MakeStrong(this), lockWaitTimer = std::move(lockWaitTimer)] (TReadLockPtr&& guard) {
                    auto lockWaitDuration = lockWaitTimer.GetElapsedTime();
                    if (State_ != EState::Initialized) {
                        THROW_ERROR_EXCEPTION("FlushRange on uninitialized NBD chunk handler")
                            .With("chunk_id", ChunkId_)
                            .With("state", State_);
                    }

                    TWallTimer ioTimer;
                    auto flushFuture = IOEngine_->FlushFileRange(
                        {.Handle = IOEngineHandle_, .Offset = offset, .Size = size},
                        WorkloadDescriptor_.Category);

                    return flushFuture.Apply(
                        BIND([=, guard = std::move(guard), ioTimer = std::move(ioTimer), this, this_ = MakeStrong(this)] (const NIO::TFlushFileRangeResponse&) {
                            auto ioDuration = ioTimer.GetElapsedTime();

                            YT_TLOG_DEBUG("Finished flushing NBD chunk range")
                                .With("ChunkId", ChunkId_)
                                .With("Offset", offset)
                                .With("Size", size)
                                .With("LockWaitDuration", lockWaitDuration)
                                .With("IODuration", ioDuration);
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
