#include "stdafx.h"
#include "local_channel.h"
#include "channel.h"
#include "server.h"
#include "client.h"
#include "service.h"
#include "message.h"

#include <core/ytree/convert.h>

#include <core/concurrency/delayed_executor.h>

#include <core/bus/bus.h>

#include <atomic>

namespace NYT {
namespace NRpc {

using namespace NYTree;
using namespace NConcurrency;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

class TLocalChannel
    : public IChannel
{
public:
    explicit TLocalChannel(IServerPtr server)
        : Server_(std::move(server))
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return DefaultTimeout_;
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        DefaultTimeout_ = timeout;
    }

    virtual TYsonString GetEndpointDescription() const override
    {
        return ConvertToYsonString(Stroka("<local>"));
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool /*requestAck*/) override
    {
        auto actualTimeout = timeout ? timeout : DefaultTimeout_;

        TServiceId serviceId(request->GetService(), request->GetRealmId());
        auto service = Server_->FindService(serviceId);
        if (!service) {
            auto error = TError(
                EErrorCode::NoSuchService,
                "Service is not registered (Service: %v, RealmId: %v)",
                serviceId.ServiceName,
                serviceId.RealmId);
            responseHandler->HandleError(error);
            return nullptr;
        }

        auto serializedRequest = request->Serialize();

        auto session = New<TSession>(
            std::move(responseHandler),
            actualTimeout);

        service->HandleRequest(
            std::make_unique<NProto::TRequestHeader>(request->Header()),
            std::move(serializedRequest),
            std::move(session));

        return New<TClientRequestControl>(std::move(service), request->GetRequestId());
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return VoidFuture;
    }

private:
    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;

    class TClientRequestControl;
    typedef TIntrusivePtr<TClientRequestControl> TClientRequestControlPtr;

    const IServerPtr Server_;

    TNullable<TDuration> DefaultTimeout_;


    class TSession
        : public IBus
    {
    public:
        TSession(IClientResponseHandlerPtr handler, const TNullable<TDuration>& timeout)
            : Handler_(std::move(handler))
            , Replied_(false)
        {
            if (timeout) {
                TDelayedExecutor::Submit(
                    BIND(&TSession::OnTimeout, MakeStrong(this)),
                    *timeout);
            }
        }

        virtual TYsonString GetEndpointDescription() const
        {
            return ConvertToYsonString(Stroka("<local>"));
        }

        virtual TFuture<void> Send(TSharedRefArray message, EDeliveryTrackingLevel /*level*/) override
        {
            NProto::TResponseHeader header;
            YCHECK(ParseResponseHeader(message, &header));
            if (AcquireLock()) {
                TError error;
                if (header.has_error()) {
                    error = FromProto<TError>(header.error());
                }
                if (error.IsOK()) {
                    Handler_->HandleResponse(std::move(message));
                } else {
                    Handler_->HandleError(error);
                }
            }
            return VoidFuture;
        }

        virtual void Terminate(const TError& /*error*/) override
        { }

        virtual void SubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
        { }

        virtual void UnsubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
        { }

    private:
        const IClientResponseHandlerPtr Handler_;

        std::atomic<bool> Replied_;


        bool AcquireLock()
        {
            bool expected = false;
            return Replied_.compare_exchange_strong(expected, true);
        }

        void OnTimeout()
        {
            if (AcquireLock()) {
                ReportError(TError(NYT::EErrorCode::Timeout, "Request timed out"));
            }
        }

        void ReportError(const TError& error)
        {
            auto detailedError = error
                << TErrorAttribute("endpoint", GetEndpointDescription());
            Handler_->HandleError(detailedError);
        }

    };

    class TClientRequestControl
        : public IClientRequestControl
    {
    public:
        TClientRequestControl(IServicePtr service, const TRequestId& requestId)
            : Service_(std::move(service))
            , RequestId_(requestId)
        { }

        virtual void Cancel() override
        {
            Service_->HandleRequestCancelation(RequestId_);
        }

    private:
        const IServicePtr Service_;
        const TRequestId RequestId_;

    };
};

IChannelPtr CreateLocalChannel(IServerPtr server)
{
    return New<TLocalChannel>(server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
