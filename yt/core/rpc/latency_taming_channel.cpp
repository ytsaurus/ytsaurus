#include "latency_taming_channel.h"
#include "channel.h"
#include "client.h"
#include "private.h"

#include <yt/core/misc/small_vector.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <atomic>

namespace NYT {
namespace NRpc {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLatencyTamingResponseHandler)
DECLARE_REFCOUNTED_CLASS(TLatencyTamingSession)

////////////////////////////////////////////////////////////////////////////////

class TLatencyTamingResponseHandler
    : public IClientResponseHandler
{
public:
    TLatencyTamingResponseHandler(
        TLatencyTamingSessionPtr session,
        bool backup)
        : Session_(std::move(session))
        , Backup_(backup)
    { }

    // IClientResponseHandler implementation.
    virtual void HandleAcknowledgement() override;
    virtual void HandleResponse(TSharedRefArray message) override;
    virtual void HandleError(const TError& error) override;

private:
    const TLatencyTamingSessionPtr Session_;
    const bool Backup_;

};

DEFINE_REFCOUNTED_TYPE(TLatencyTamingResponseHandler)

////////////////////////////////////////////////////////////////////////////////

class TLatencyTamingSession
    : public IClientRequestControl
{
public:
    TLatencyTamingSession(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options,
        IChannelPtr primaryChannel,
        IChannelPtr backupChannel,
        TDuration delay)
        : Request_(std::move(request))
        , ResponseHandler_(std::move(responseHandler))
        , Options_(options)
        , PrimaryChannel_(std::move(primaryChannel))
        , BackupChannel_(std::move(backupChannel))
        , Delay_(delay)
    { }

    void Run()
    {
        auto responseHandler = New<TLatencyTamingResponseHandler>(this, false);
        auto requestControl = PrimaryChannel_->Send(
            Request_,
            std::move(responseHandler),
            Options_);

        // NB: No locking is needed
        RequestControls_.push_back(requestControl);

        if (Delay_ == TDuration()) {
            OnDeadlineReached(false);
        } else {
            DeadlineCookie_ = TDelayedExecutor::Submit(
                BIND(&TLatencyTamingSession::OnDeadlineReached, MakeStrong(this)),
                Delay_);
        }
    }

    void HandleAcknowledgement(bool backup)
    {
        bool expected = false;
        if (!Acknowledged_.compare_exchange_strong(expected, true)) {
            return;
        }

        LOG_DEBUG_IF(backup, "Request acknowledged by backup (RequestId: %v)",
            Request_->GetRequestId());
        ResponseHandler_->HandleAcknowledgement();
    }

    void HandleResponse(TSharedRefArray message, bool backup)
    {
        bool expected = false;
        if (!Responded_.compare_exchange_strong(expected, true)) {
            return;
        }

        LOG_DEBUG_IF(backup, "Response received from backup (RequestId: %v)",
            Request_->GetRequestId());
        ResponseHandler_->HandleResponse(std::move(message));
        Cleanup();
    }

    void HandleError(const TError& error, bool backup)
    {
        bool expected = false;
        if (!Responded_.compare_exchange_strong(expected, true)) {
            return;
        }

        LOG_DEBUG_IF(backup, "Request failed at backup (RequestId: %v)",
            Request_->GetRequestId());
        ResponseHandler_->HandleError(error);
        Cleanup();
    }

    // IClientRequestControl implementation.
    virtual void Cancel() override
    {
        Acknowledged_.store(true);
        Responded_.store(true);

        SmallVector<IClientRequestControlPtr, 2> requestControls;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            // Avoid receiving any notifications.
            RequestControls_.swap(requestControls);
        }

        Cleanup();

        for (const auto& control : RequestControls_) {
            control->Cancel();
        }
    }

private:
    const IClientRequestPtr Request_;
    const IClientResponseHandlerPtr ResponseHandler_;
    const TSendOptions Options_;
    const IChannelPtr PrimaryChannel_;
    const IChannelPtr BackupChannel_;
    const TDuration Delay_;

    TDelayedExecutorCookie DeadlineCookie_;

    std::atomic<bool> Acknowledged_ = {false};
    std::atomic<bool> Responded_ = {false};

    TSpinLock SpinLock_;
    SmallVector<IClientRequestControlPtr, 2> RequestControls_; // always at most 2 items


    void Cleanup()
    {
        TDelayedExecutor::CancelAndClear(DeadlineCookie_);
    }

    std::optional<TDuration> GetBackupTimeout()
    {
        if (!Options_.Timeout) {
            return std::nullopt;
        }

        auto timeout = *Options_.Timeout;
        if (timeout < Delay_) {
            return TDuration();
        }

        return timeout - Delay_;
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
        if (backupTimeout == std::make_optional(TDuration())) {
            // Makes no sense to send the request anyway.
            return;
        }

        auto backupOptions = Options_;
        backupOptions.Timeout = backupTimeout;

        LOG_DEBUG("Resending request to backup (RequestId: %v)",
            Request_->GetRequestId());

        auto responseHandler = New<TLatencyTamingResponseHandler>(this, true);
        auto requestControl = BackupChannel_->Send(
            Request_,
            std::move(responseHandler),
            backupOptions);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            RequestControls_.push_back(requestControl);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TLatencyTamingSession)

////////////////////////////////////////////////////////////////////////////////

void TLatencyTamingResponseHandler::HandleAcknowledgement()
{
    Session_->HandleAcknowledgement(Backup_);
}

void TLatencyTamingResponseHandler::HandleError(const TError& error)
{
    Session_->HandleError(error, Backup_);
}

void TLatencyTamingResponseHandler::HandleResponse(TSharedRefArray message)
{
    Session_->HandleResponse(std::move(message), Backup_);
}

////////////////////////////////////////////////////////////////////////////////

class TLatencyTamingChannel
    : public IChannel
{
public:
    TLatencyTamingChannel(
        IChannelPtr primaryChannel,
        IChannelPtr backupChannel,
        TDuration delay)
        : PrimaryChannel_(std::move(primaryChannel))
        , BackupChannel_(std::move(backupChannel))
        , Delay_(delay)
        , EndpointDescription_(Format("LatencyTaming(%v,%v,%v)",
            PrimaryChannel_->GetEndpointDescription(),
            BackupChannel_->GetEndpointDescription(),
            Delay_))
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("primary").Value(PrimaryChannel_->GetEndpointAttributes())
                .Item("backup").Value(BackupChannel_->GetEndpointAttributes())
                .Item("delay").Value(Delay_)
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

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto session = New<TLatencyTamingSession>(
            std::move(request),
            std::move(responseHandler),
            options,
            PrimaryChannel_,
            BackupChannel_,
            Delay_);
        session->Run();
        return session;
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return Combine(std::vector<TFuture<void>>{
            PrimaryChannel_->Terminate(error),
            BackupChannel_->Terminate(error)
        });
    }

private:
    const IChannelPtr PrimaryChannel_;
    const IChannelPtr BackupChannel_;
    const TDuration Delay_;

    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

};

IChannelPtr CreateLatencyTamingChannel(
    IChannelPtr primaryChannel,
    IChannelPtr backupChannel,
    TDuration delay)
{
    YCHECK(primaryChannel);
    YCHECK(backupChannel);

    return New<TLatencyTamingChannel>(
        std::move(primaryChannel),
        std::move(backupChannel),
        delay);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
