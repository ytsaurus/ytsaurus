#include "hedging_channel.h"
#include "channel.h"
#include "client.h"
#include "private.h"

#include <yt/yt/core/misc/small_vector.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <atomic>

namespace NYT::NRpc {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THedgingResponseHandler)
DECLARE_REFCOUNTED_CLASS(THedgingSession)

////////////////////////////////////////////////////////////////////////////////

class THedgingResponseHandler
    : public IClientResponseHandler
{
public:
    THedgingResponseHandler(
        THedgingSessionPtr session,
        bool backup)
        : Session_(std::move(session))
        , Backup_(backup)
    { }

    // IClientResponseHandler implementation.
    virtual void HandleAcknowledgement() override;
    virtual void HandleResponse(TSharedRefArray message) override;
    virtual void HandleError(const TError& error) override;
    virtual void HandleStreamingPayload(const TStreamingPayload& /*payload*/) override;
    virtual void HandleStreamingFeedback(const TStreamingFeedback& /*feedback*/) override;

private:
    const THedgingSessionPtr Session_;
    const bool Backup_;
};

DEFINE_REFCOUNTED_TYPE(THedgingResponseHandler)

////////////////////////////////////////////////////////////////////////////////

class THedgingSession
    : public IClientRequestControl
{
public:
    THedgingSession(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& sendOptions,
        IChannelPtr primaryChannel,
        IChannelPtr backupChannel,
        const THedgingChannelOptions& hedgingOptions)
        : Request_(std::move(request))
        , ResponseHandler_(std::move(responseHandler))
        , SendOptions_(sendOptions)
        , PrimaryChannel_(std::move(primaryChannel))
        , BackupChannel_(std::move(backupChannel))
        , HedgingOptions_(hedgingOptions)
    {
        auto hedgingResponseHandler = New<THedgingResponseHandler>(this, false);
        auto requestControl = PrimaryChannel_->Send(
            Request_,
            std::move(hedgingResponseHandler),
            SendOptions_);

        // NB: No locking is needed
        RequestControls_.push_back(requestControl);

        if (HedgingOptions_.Delay == TDuration::Zero()) {
            OnDeadlineReached(false);
        } else {
            DeadlineCookie_ = TDelayedExecutor::Submit(
                BIND(&THedgingSession::OnDeadlineReached, MakeStrong(this)),
                HedgingOptions_.Delay);
        }
    }

    void HandleAcknowledgement(bool backup)
    {
        bool expected = false;
        if (!Acknowledged_.compare_exchange_strong(expected, true)) {
            return;
        }

        YT_LOG_DEBUG_IF(backup, "Request acknowledged by backup (RequestId: %v)",
            Request_->GetRequestId());
        ResponseHandler_->HandleAcknowledgement();
    }

    void HandleResponse(TSharedRefArray message, bool backup)
    {
        bool expected = false;
        if (!Responded_.compare_exchange_strong(expected, true)) {
            return;
        }

        if (backup) {
            YT_LOG_DEBUG("Response received from backup (RequestId: %v)",
                Request_->GetRequestId());

            NRpc::NProto::TResponseHeader header;
            if (!TryParseResponseHeader(message, &header)) {
                ResponseHandler_->HandleError(TError(
                    NRpc::EErrorCode::ProtocolError,
                    "Error parsing response header from backup")
                    << TErrorAttribute(BackupFailedKey, Request_->GetRequestId())
                    << TErrorAttribute("request_id", Request_->GetRequestId()));
                return;
            }

            auto* ext = header.MutableExtension(NRpc::NProto::THedgingExt::hedging_ext);
            ext->set_backup_responded(true);
            message = SetResponseHeader(std::move(message), header);
        }

        ResponseHandler_->HandleResponse(std::move(message));
        Cleanup();
    }

    void HandleError(const TError& error, bool backup)
    {
        if (!backup && error.GetCode() == NYT::EErrorCode::Canceled && PrimaryCanceled_.load()) {
            return;
        }

        bool expected = false;
        if (!Responded_.compare_exchange_strong(expected, true)) {
            return;
        }

        YT_LOG_DEBUG_IF(backup, "Request failed at backup (RequestId: %v)",
            Request_->GetRequestId());

        ResponseHandler_->HandleError(
            backup
            ? error << TErrorAttribute(BackupFailedKey, true)
            : error);

        Cleanup();
    }

    // IClientRequestControl implementation.
    virtual void Cancel() override
    {
        Acknowledged_.store(true);
        Responded_.store(true);
        Cleanup();
        CancelSentRequests();
    }

    virtual TFuture<void> SendStreamingPayload(const TStreamingPayload& /*payload*/) override
    {
        YT_ABORT();
    }

    virtual TFuture<void> SendStreamingFeedback(const TStreamingFeedback& /*feedback*/) override
    {
        YT_ABORT();
    }

private:
    const IClientRequestPtr Request_;
    const IClientResponseHandlerPtr ResponseHandler_;
    const TSendOptions SendOptions_;
    const IChannelPtr PrimaryChannel_;
    const IChannelPtr BackupChannel_;
    const THedgingChannelOptions HedgingOptions_;

    TDelayedExecutorCookie DeadlineCookie_;

    std::atomic<bool> Acknowledged_ = false;
    std::atomic<bool> Responded_ = false;
    std::atomic<bool> PrimaryCanceled_ = false;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, SpinLock_);
    SmallVector<IClientRequestControlPtr, 2> RequestControls_; // always at most 2 items


    void CancelSentRequests()
    {
        SmallVector<IClientRequestControlPtr, 2> requestControls;
        {
            auto guard = Guard(SpinLock_);
            RequestControls_.swap(requestControls);
        }

        for (const auto& control : requestControls) {
            control->Cancel();
        }
    }

    void Cleanup()
    {
        TDelayedExecutor::CancelAndClear(DeadlineCookie_);
    }

    std::optional<TDuration> GetBackupTimeout()
    {
        if (!SendOptions_.Timeout) {
            return std::nullopt;
        }

        auto timeout = *SendOptions_.Timeout;
        if (timeout < HedgingOptions_.Delay) {
            return TDuration::Zero();
        }

        return timeout - HedgingOptions_.Delay;
    }

    void OnDeadlineReached(bool aborted)
    {
        if (aborted) {
            return;
        }

        // Shortcut.
        if (Responded_.load(std::memory_order_relaxed)) {
            return;
        }

        auto backupTimeout = GetBackupTimeout();
        if (backupTimeout == TDuration::Zero()) {
            // Makes no sense to send the request anyway.
            return;
        }

        if (HedgingOptions_.CancelPrimary && HedgingOptions_.Delay != TDuration::Zero()) {
            PrimaryCanceled_.store(true);
            CancelSentRequests();
        }

        YT_LOG_DEBUG("Resending request to backup (RequestId: %v)",
            Request_->GetRequestId());

        auto responseHandler = New<THedgingResponseHandler>(this, true);

        auto backupOptions = SendOptions_;
        backupOptions.Timeout = backupTimeout;
        auto requestControl = BackupChannel_->Send(
            Request_,
            std::move(responseHandler),
            backupOptions);

        {
            auto guard = Guard(SpinLock_);
            RequestControls_.push_back(requestControl);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(THedgingSession)

////////////////////////////////////////////////////////////////////////////////

void THedgingResponseHandler::HandleAcknowledgement()
{
    Session_->HandleAcknowledgement(Backup_);
}

void THedgingResponseHandler::HandleError(const TError& error)
{
    Session_->HandleError(error, Backup_);
}

void THedgingResponseHandler::HandleResponse(TSharedRefArray message)
{
    Session_->HandleResponse(std::move(message), Backup_);
}

void THedgingResponseHandler::HandleStreamingPayload(const TStreamingPayload& /*payload*/)
{
    YT_ABORT();
}

void THedgingResponseHandler::HandleStreamingFeedback(const TStreamingFeedback& /*feedback*/)
{
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

class THedgingChannel
    : public IChannel
{
public:
    THedgingChannel(
        IChannelPtr primaryChannel,
        IChannelPtr backupChannel,
        THedgingChannelOptions options)
        : PrimaryChannel_(std::move(primaryChannel))
        , BackupChannel_(std::move(backupChannel))
        , Options_(options)
        , EndpointDescription_(Format("Hedging(%v,%v)",
            PrimaryChannel_->GetEndpointDescription(),
            BackupChannel_->GetEndpointDescription()))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("primary").Value(PrimaryChannel_->GetEndpointAttributes())
                .Item("backup").Value(BackupChannel_->GetEndpointAttributes())
            .EndMap()))
    { }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual TNetworkId GetNetworkId() const override
    {
        return PrimaryChannel_->GetNetworkId();
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        return New<THedgingSession>(
            std::move(request),
            std::move(responseHandler),
            options,
            PrimaryChannel_,
            BackupChannel_,
            Options_);
    }

    virtual void Terminate(const TError& error) override
    {
        PrimaryChannel_->Terminate(error);
        BackupChannel_->Terminate(error);
    }

    virtual void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        PrimaryChannel_->SubscribeTerminated(callback);
        BackupChannel_->SubscribeTerminated(callback);
    }

    virtual void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        PrimaryChannel_->UnsubscribeTerminated(callback);
        BackupChannel_->UnsubscribeTerminated(callback);
    }

private:
    const IChannelPtr PrimaryChannel_;
    const IChannelPtr BackupChannel_;
    const THedgingChannelOptions Options_;

    const TString EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
};

IChannelPtr CreateHedgingChannel(
    IChannelPtr primaryChannel,
    IChannelPtr backupChannel,
    const THedgingChannelOptions& options)
{
    YT_VERIFY(primaryChannel);
    YT_VERIFY(backupChannel);

    return New<THedgingChannel>(
        std::move(primaryChannel),
        std::move(backupChannel),
        options);
}

bool IsBackup(const TClientResponsePtr& response)
{
     const auto& ext = response->Header().GetExtension(NRpc::NProto::THedgingExt::hedging_ext);
     return ext.backup_responded();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
