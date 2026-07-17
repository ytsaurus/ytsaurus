#include "chunk_handler.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/data_node_nbd_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NNbd::NChunk {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TChunkHandler
    : public IChunkHandler
{
public:
    TChunkHandler(
        IBlockDevice* blockDevice,
        TChunkBlockDeviceConfigPtr config,
        IInvokerPtr invoker,
        IChannelPtr channel,
        TSessionId sessionId,
        TLogger logger)
        : BlockDevice_(blockDevice)
        , Config_(std::move(config))
        , Invoker_(std::move(invoker))
        , Channel_(std::move(channel))
        , SessionId_(sessionId.ChunkId ? sessionId : GenerateSessionId(Config_->MediumIndex))
        , Proxy_(Channel_)
        , Logger(logger.WithTag("SessionId: %v", SessionId_))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Invoker_);
        YT_VERIFY(Channel_);
    }

    ~TChunkHandler()
    {
        YT_LOG_DEBUG("Destroying chunk handler (Size: %v, FsType: %v)",
            Config_->Size,
            Config_->FsType);
    }

    TFuture<void> Initialize() override
    {
        auto guard = TGuard(Lock_);
        if (InitializeFuture_) {
            return InitializeFuture_;
        }

        YT_LOG_DEBUG("Initializing chunk handler");

        InitializeFuture_ = DoInitialize();
        InitializeFuture_.Subscribe(BIND([this, this_ = MakeStrong(this)](const TError& error) {
            if (error.IsOK()) {
                YT_LOG_DEBUG("Initialized chunk handler");
            } else {
                YT_LOG_ERROR(error, "Failed to initialize chunk handler");
            }
        }));
        return InitializeFuture_;
    }

    TFuture<void> Finalize() override
    {
        auto guard = TGuard(Lock_);
        if (FinalizeFuture_) {
            return FinalizeFuture_;
        }

        YT_LOG_DEBUG("Finalizing chunk handler");

        FinalizeFuture_ = DoFinalize();
        FinalizeFuture_.Subscribe(BIND([this, this_ = MakeStrong(this)](const TError& error) {
            if (error.IsOK()) {
                YT_LOG_DEBUG("Finalized chunk handler");
            } else {
                YT_LOG_ERROR(error, "Failed to finalize chunk handler");
            }
        }));
        return FinalizeFuture_;
    }

    TFuture<void> Flush(const TFlushOptions& options) override
    {
        if (State_ != EState::Initialized) {
            YT_LOG_DEBUG("Can not flush uninitialized chunk handler (Cookie: %x)",
                options.Cookie);
            return OKFuture;
        }

        YT_LOG_DEBUG("Flushing chunk handler (Cookie: %x)",
            options.Cookie);

        auto req = Proxy_.Flush();
        req->SetRequestInfo("ChunkId: %v, Cookie: %x",
            SessionId_.ChunkId,
            options.Cookie);

        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_cookie(options.Cookie);
        req->SetMultiplexingBand(EMultiplexingBand::Interactive);
        req->SetMultiplexingParallelism(Config_->MultiplexingParallelism);

        auto startTime = TInstant::Now();
        return req->Invoke().Apply(BIND([startTime, Logger = Logger] (const TErrorOr<TDataNodeNbdServiceProxy::TRspFlushPtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);
            YT_LOG_DEBUG("Flushed chunk handler (Duration: %v)", TInstant::Now() - startTime);
        }));
    }

    TFuture<TReadResponse> Read(i64 offset, i64 length, const TReadOptions& options) override
    {
        if (State_ != EState::Initialized) {
            YT_LOG_ERROR("Can not read from uninitialized chunk handler (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                length,
                options.Cookie);

            THROW_ERROR_EXCEPTION("Read from uninitialized chunk handler")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("length", length)
                << TErrorAttribute("cookie", options.Cookie)
                << TErrorAttribute("state", State_);
        }

        auto req = Proxy_.Read();
        req->SetRequestInfo("ChunkId: %v, Offset: %v, Length: %v, Cookie: %x",
            SessionId_.ChunkId,
            offset,
            length,
            options.Cookie);

        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->SetMultiplexingBand(EMultiplexingBand::Interactive);
        req->SetMultiplexingParallelism(Config_->MultiplexingParallelism);
        req->set_offset(offset);
        req->set_length(length);
        req->set_cookie(options.Cookie);

        return req->Invoke().Apply(BIND([] (const TErrorOr<TDataNodeNbdServiceProxy::TRspReadPtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

            const auto& response = rspOrError.Value();
            auto blocks = GetRpcAttachedBlocks(response);
            YT_VERIFY(ssize(blocks) == 1);
            return TReadResponse(std::move(blocks[0].Data), response->should_close_session());
        }));
    }

    TFuture<TWriteResponse> Write(i64 offset, const TSharedRef& data, const TWriteOptions& options) override
    {
        if (State_ != EState::Initialized) {
            YT_LOG_ERROR("Can not write to uninitialized chunk handler (Offset: %v, Length: %v, Cookie: %x)",
                offset,
                data.size(),
                options.Cookie);

            THROW_ERROR_EXCEPTION("Write to uninitialized chunk handler")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("length", data.size())
                << TErrorAttribute("cookie", options.Cookie)
                << TErrorAttribute("state", State_);
        }

        auto req = Proxy_.Write();
        req->SetRequestInfo("ChunkId: %v, Offset: %v, Length: %v, Cookie: %x, Flush: %v",
            SessionId_.ChunkId,
            offset,
            data.size(),
            options.Cookie,
            options.Flush);

        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->SetMultiplexingBand(EMultiplexingBand::Interactive);
        req->SetMultiplexingParallelism(Config_->MultiplexingParallelism);
        req->set_offset(offset);
        req->set_cookie(options.Cookie);
        req->set_flush(options.Flush);
        SetRpcAttachedBlocks(req, {TBlock(data)});
        return req->Invoke().Apply(BIND([] (const TErrorOr<TDataNodeNbdServiceProxy::TRspWritePtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

            const auto& response = rspOrError.Value();
            return TWriteResponse(response->should_close_session());
        }));
    }

    TFuture<std::vector<TReadResponse>> ReadBatch(
        const std::vector<TReadBatchSubrequest>& subrequests,
        const TReadOptions& options) override
    {
        if (State_ != EState::Initialized) {
            THROW_ERROR_EXCEPTION("ReadBatch on uninitialized chunk handler")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("state", State_);
        }

        auto req = Proxy_.ReadBatch();
        req->SetRequestInfo("ChunkId: %v, SubrequestCount: %v, Cookie: %x",
            SessionId_.ChunkId,
            subrequests.size(),
            options.Cookie);

        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->SetMultiplexingBand(EMultiplexingBand::Interactive);
        req->SetMultiplexingParallelism(Config_->MultiplexingParallelism);
        req->set_cookie(options.Cookie);
        for (const auto& sub : subrequests) {
            auto* protoSub = req->add_subrequests();
            protoSub->set_offset(sub.Offset);
            protoSub->set_length(sub.Length);
        }

        return req->Invoke().Apply(BIND([] (const TErrorOr<TDataNodeNbdServiceProxy::TRspReadBatchPtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

            const auto& response = rspOrError.Value();
            const auto& attachments = response->Attachments();
            bool shouldClose = response->should_close_session();
            std::vector<TReadResponse> responses;
            responses.reserve(attachments.size());
            for (const auto& attachment : attachments) {
                responses.emplace_back(attachment, shouldClose);
            }
            return responses;
        }));
    }

    TFuture<TWriteResponse> WriteBatch(
        const std::vector<TWriteBatchSubrequest>& subrequests,
        const TWriteOptions& options) override
    {
        if (State_ != EState::Initialized) {
            THROW_ERROR_EXCEPTION("WriteBatch on uninitialized chunk handler")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("state", State_);
        }

        auto req = Proxy_.WriteBatch();
        req->SetRequestInfo("ChunkId: %v, SubrequestCount: %v, Cookie: %x",
            SessionId_.ChunkId,
            subrequests.size(),
            options.Cookie);

        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->SetMultiplexingBand(EMultiplexingBand::Interactive);
        req->SetMultiplexingParallelism(Config_->MultiplexingParallelism);
        req->set_cookie(options.Cookie);

        for (const auto& sub : subrequests) {
            auto* protoSub = req->add_subrequests();
            protoSub->set_offset(sub.Offset);
            req->Attachments().push_back(sub.Data);
        }

        return req->Invoke().Apply(BIND([] (const TErrorOr<TDataNodeNbdServiceProxy::TRspWriteBatchPtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

            const auto& response = rspOrError.Value();
            return TWriteResponse(response->should_close_session());
        }));
    }

private:
    IBlockDevice* const BlockDevice_;
    const TChunkBlockDeviceConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const IChannelPtr Channel_;
    const TSessionId SessionId_;
    const TDataNodeNbdServiceProxy Proxy_;
    const TLogger Logger;
    TPeriodicExecutorPtr KeepSessionAliveExecutor_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    TFuture<void> InitializeFuture_;
    TFuture<void> FinalizeFuture_;

    enum EState
    {
        Uninitialized,
        Initializing,
        Initialized,
        Finalizing,
    };
    std::atomic<EState> State_ = EState::Uninitialized;

    TFuture<void> DoInitialize()
    {
        auto expected = EState::Uninitialized;
        if (!State_.compare_exchange_strong(expected, EState::Initializing)) {
            auto error = TError("Can not initialize chunk handler in non-uninitialized state")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("size", Config_->Size)
                << TErrorAttribute("fs_type", Config_->FsType)
                << TErrorAttribute("actual_state", expected)
                << TErrorAttribute("expected_state", EState::Uninitialized);
            YT_LOG_WARNING(error);
            return MakeFuture(error);
        }

        auto req = Proxy_.OpenSession();
        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout + Config_->DataNodeNbdServiceMakeTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_size(Config_->Size);
        req->set_fs_type(ToProto(Config_->FsType));

        return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TDataNodeNbdServiceProxy::TRspOpenSessionPtr>& rspOrError) {
            if (!rspOrError.IsOK()) {
                // Reset state back to Uninitialized.
                State_ = EState::Uninitialized;
                THROW_ERROR_EXCEPTION("Failed to open session") << rspOrError;
            }

            // Set state to Initialized prior to starting KeepSessionAliveExecutor_.
            State_ = EState::Initialized;

            KeepSessionAliveExecutor_ = New<TPeriodicExecutor>(
                Invoker_,
                BIND_NO_PROPAGATE(&TChunkHandler::KeepSessionAlive, MakeWeak(this)),
                Config_->KeepSessionAlivePeriod);

            KeepSessionAliveExecutor_->Start();
        })
        .AsyncVia(Invoker_));
    }

    TFuture<void> DoFinalize()
    {
        auto expected = EState::Initialized;
        if (!State_.compare_exchange_strong(expected, EState::Finalizing)) {
            auto error = TError("Can not finalize chunk handler in non-initialized state")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("size", Config_->Size)
                << TErrorAttribute("fs_type", Config_->FsType)
                << TErrorAttribute("actual_state", expected)
                << TErrorAttribute("expected_state", EState::Initialized);
            YT_LOG_WARNING(error);
            return MakeFuture(error);
        }

        auto future = KeepSessionAliveExecutor_->Stop();
        return future.Apply(BIND([this, this_ = MakeStrong(this)] () {
            auto req = Proxy_.CloseSession();
            req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
            ToProto(req->mutable_session_id(), SessionId_);
            return req->Invoke().AsVoid();
        })
        .AsyncVia(Invoker_))
        .Apply(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            if (!error.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to close session") << error;
            }
            State_ = EState::Uninitialized;
        })
        .AsyncVia(Invoker_));
    }

    void KeepSessionAlive()
    {
        YT_LOG_DEBUG("Sending keep alive request");

        auto req = Proxy_.KeepSessionAlive();
        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->SetMultiplexingBand(EMultiplexingBand::Interactive);
        req->SetMultiplexingParallelism(Config_->MultiplexingParallelism);

        auto rspOrError = WaitFor(req->Invoke());

        if (!rspOrError.IsOK()) {
            // Only log error since it might be caused by intermittent problems.
            YT_LOG_ERROR(rspOrError, "Keep alive request failed");
        } else {
            YT_LOG_DEBUG("Received keep alive response (ShouldCloseSession: %v)",
                rspOrError.Value()->should_close_session());

            if (rspOrError.Value()->should_close_session()) {
                BlockDevice_->SetError(TError("Stop using device"));

                // Call BlockDevice_->Finalize() (not this->Finalize()) so that the full
                // TChunkBlockDevice::Finalize() chain runs.
                // Capture a strong reference (IBlockDevicePtr) rather than the raw
                // BlockDevice_ pointer: the lambda is queued on the invoker and will
                // execute asynchronously, so the block device must be kept alive until
                // then — otherwise the caller could drop its IBlockDevicePtr and destroy
                // TChunkBlockDevice before the queued callback runs, producing a
                // use-after-free when blockDevice->Finalize() is called.
                Invoker_->Invoke(BIND_NO_PROPAGATE([blockDevice = IBlockDevicePtr(BlockDevice_)] {
                    YT_UNUSED_FUTURE(blockDevice->Finalize());
                }));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkHandlerPtr CreateChunkHandler(
    IBlockDevice* blockDevice,
    TChunkBlockDeviceConfigPtr config,
    IInvokerPtr invoker,
    IChannelPtr channel,
    TSessionId sessionId,
    NLogging::TLogger logger)
{
    return New<TChunkHandler>(
        blockDevice,
        std::move(config),
        std::move(invoker),
        std::move(channel),
        sessionId,
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

TSessionId GenerateSessionId(int mediumIndex)
{
    return TSessionId(
        MakeRandomId(EObjectType::NbdChunk, InvalidCellTag),
        mediumIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NChunk
