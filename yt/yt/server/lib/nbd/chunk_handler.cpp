#include "chunk_handler.h"

#include "config.h"

#include <yt/yt/ytlib/chunk_client/data_node_nbd_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NNbd {

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
        TChunkBlockDeviceConfigPtr config,
        IInvokerPtr invoker,
        IChannelPtr channel,
        TLogger logger)
        : Config_(std::move(config))
        , Invoker_(std::move(invoker))
        , Channel_(std::move(channel))
        , SessionId_(GenerateSessonId())
        , Proxy_(Channel_)
        , Logger(logger.WithTag("SessionId: %v", SessionId_))
    {
        YT_VERIFY(Config_);
        YT_VERIFY(Invoker_);
        YT_VERIFY(Channel_);
    }

    ~TChunkHandler()
    {
        YT_LOG_DEBUG("Destructing chunk handler (Size: %v, FsType: %v)",
            Config_->Size,
            Config_->FsType);
    }

    TFuture<void> Initialize() override
    {
        if (Initialized_) {
            YT_LOG_WARNING("Can not initialize already initialized chunk handler");
            return VoidFuture;
        }

        auto req = Proxy_.OpenSession();
        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout + Config_->DataNodeNbdServiceMakeTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_size(Config_->Size);
        req->set_fs_type(ToProto(Config_->FsType));

        return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TDataNodeNbdServiceProxy::TRspOpenSessionPtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Failed to open session");

            KeepSessionAliveExecutor_ = New<TPeriodicExecutor>(
                Invoker_,
                BIND_NO_PROPAGATE(&TChunkHandler::KeepSessionAlive, MakeWeak(this)),
                Config_->KeepSessionAlivePeriod);

            KeepSessionAliveExecutor_->Start();

            Initialized_ = true;
        })
        .AsyncVia(Invoker_));
    }

    TFuture<void> Finalize() override
    {
        if (!Initialized_) {
            YT_LOG_WARNING("Can not finalize uninitialized chunk handler");
            return VoidFuture;
        }

        auto future = KeepSessionAliveExecutor_->Stop();
        return future.Apply(BIND([this, this_ = MakeStrong(this)] () {
            auto req = Proxy_.CloseSession();
            req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
            ToProto(req->mutable_session_id(), SessionId_);
            return req->Invoke().AsVoid();
        })
        .AsyncVia(Invoker_))
        .Apply(BIND([this, this_ = MakeStrong(this)] () {
            Initialized_ = false;
        })
        .AsyncVia(Invoker_));
    }

    TFuture<TSharedRef> Read(i64 offset, i64 length) override
    {
        if (!Initialized_) {
            YT_LOG_ERROR("Can not read from uninitialized chunk handler (Offset: %v, Length: %v)",
                offset,
                length);

            THROW_ERROR_EXCEPTION("Read from uninitialized chunk handler")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("length", length);
        }

        auto req = Proxy_.Read();
        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_offset(offset);
        req->set_length(length);

        return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TDataNodeNbdServiceProxy::TRspReadPtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

            const auto& response = rspOrError.Value();
            if (response->close_session()) {
                THROW_ERROR_EXCEPTION("Session needs to be closed.")
                    << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                    << TErrorAttribute("medium_index", SessionId_.MediumIndex);
            }

            auto blocks = GetRpcAttachedBlocks(response);
            YT_VERIFY(ssize(blocks) == 1);
            return std::move(blocks[0].Data);
        })
        .AsyncVia(Invoker_));
    }

    TFuture<void> Write(i64 offset, const TSharedRef& data, const TWriteOptions&) override
    {
        if (!Initialized_) {
            YT_LOG_ERROR("Can not write to uninitialized chunk handler (Offset: %v, Length: %v)",
                offset,
                data.size());

            THROW_ERROR_EXCEPTION("Write to uninitialized chunk handler")
                << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                << TErrorAttribute("medium_index", SessionId_.MediumIndex)
                << TErrorAttribute("offset", offset)
                << TErrorAttribute("length", data.size());
        }

        auto req = Proxy_.Write();
        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);
        req->set_offset(offset);
        SetRpcAttachedBlocks(req, {TBlock(data)});
        return req->Invoke().Apply(BIND([this, this_ = MakeStrong(this)] (const TErrorOr<TDataNodeNbdServiceProxy::TRspWritePtr>& rspOrError) {
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError);

            const auto& response = rspOrError.Value();
            if (response->close_session()) {
                THROW_ERROR_EXCEPTION("Session needs to be closed.")
                    << TErrorAttribute("chunk_id", SessionId_.ChunkId)
                    << TErrorAttribute("medium_index", SessionId_.MediumIndex);
            }
        })
        .AsyncVia(Invoker_));
    }

private:
    TSessionId GenerateSessonId()
    {
        TSessionId sessionId;
        sessionId.ChunkId = MakeRandomId(EObjectType::NbdChunk, InvalidCellTag);
        sessionId.MediumIndex = Config_->MediumIndex;
        return sessionId;
    }

    void KeepSessionAlive()
    {
        YT_LOG_DEBUG("Sending keep alive");

        auto req = Proxy_.KeepSessionAlive();
        req->SetTimeout(Config_->DataNodeNbdServiceRpcTimeout);
        ToProto(req->mutable_session_id(), SessionId_);

        auto rspOrError = WaitFor(req->Invoke());
        if (rspOrError.IsOK()) {
            if (rspOrError.Value()->close_session()) {
                YT_LOG_ERROR("Keep alive response demands to close session");
                std::ignore = WaitFor(KeepSessionAliveExecutor_->Stop());
                std::ignore = WaitFor(Finalize());
            }
        } else {
            YT_LOG_ERROR("Failed to send keep alive request (Error: %v)",
                rspOrError);
        }
    }

    const TChunkBlockDeviceConfigPtr Config_;
    const IInvokerPtr Invoker_;
    const IChannelPtr Channel_;
    const TSessionId SessionId_;
    const TDataNodeNbdServiceProxy Proxy_;
    const TLogger Logger;
    TPeriodicExecutorPtr KeepSessionAliveExecutor_;
    bool Initialized_ = false;
};

////////////////////////////////////////////////////////////////////////////////

IChunkHandlerPtr CreateChunkHandler(
    TChunkBlockDeviceConfigPtr config,
    IInvokerPtr invoker,
    IChannelPtr channel,
    NLogging::TLogger logger)
{
    return New<TChunkHandler>(
        std::move(config),
        std::move(invoker),
        std::move(channel),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd
