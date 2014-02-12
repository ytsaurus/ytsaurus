#include "stdafx.h"
#include "local_channel.h"
#include "channel.h"
#include "server.h"
#include "client.h"
#include "service.h"
#include "message.h"

#include <core/concurrency/delayed_executor.h>

#include <core/bus/bus.h>

#include <atomic>

namespace NYT {
namespace NRpc {

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

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool /*requestAck*/) override
    {
        auto actualTimeout = timeout ? timeout : DefaultTimeout_;

        const auto& header = request->Header();
        TServiceId serviceId(header.service(), FromProto<TRealmId>(header.realm_id()));

        auto service = Server_->FindService(serviceId);
        if (!service) {
            auto error = TError(
                EErrorCode::NoSuchService,
                "Service is not registered (Service: %s, RealmId: %s)",
                ~serviceId.ServiceName,
                ~ToString(serviceId.RealmId));
            responseHandler->OnError(error);
            return;
        }

        auto serializedRequest = request->Serialize();

        auto session = New<TSession>(
            std::move(responseHandler),
            actualTimeout);

        service->OnRequest(
            std::unique_ptr<NProto::TRequestHeader>(new NProto::TRequestHeader(header)),
            std::move(serializedRequest),
            std::move(session));
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return MakeFuture();
    }

private:
    IServerPtr Server_;

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

        virtual TAsyncError Send(TSharedRefArray message, EDeliveryTrackingLevel /*level*/) override
        {
            NProto::TResponseHeader header;
            YCHECK(ParseResponseHeader(message, &header));
            if (AcquireLock()) {
                if (header.has_error()) {
                    Handler_->OnError(FromProto(header.error()));
                } else {
                    Handler_->OnResponse(std::move(message));
                }
            }
            return MakeFuture(TError());
        }

        virtual void Terminate(const TError& /*error*/) override
        { }

        DEFINE_SIGNAL(void(TError), Terminated);

    private:
        IClientResponseHandlerPtr Handler_;
        
        std::atomic<bool> Replied_;


        bool AcquireLock()
        {
            bool expected = false;
            return Replied_.compare_exchange_strong(expected, true);
        }

        void OnTimeout()
        {
            if (AcquireLock()) {
                auto error = TError(EErrorCode::Timeout, "Request timed out");
                Handler_->OnError(error);
            }
        }

    };

};

IChannelPtr CreateLocalChannel(IServerPtr server)
{
    return New<TLocalChannel>(server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
