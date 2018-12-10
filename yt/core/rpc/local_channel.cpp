#include "local_channel.h"
#include "channel.h"
#include "client.h"
#include "message.h"
#include "server.h"
#include "service.h"

#include <yt/core/bus/bus.h>

#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/net/address.h>

#include <atomic>

namespace NYT::NRpc {

using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static const TString EndpointDescription = "<local>";
static const std::unique_ptr<IAttributeDictionary> EndpointAttributes =
    ConvertToAttributes(BuildYsonStringFluently()
        .BeginMap()
            .Item("local").Value(true)
        .EndMap());

////////////////////////////////////////////////////////////////////////////////

class TLocalChannel
    : public IChannel
{
public:
    explicit TLocalChannel(IServerPtr server)
        : Server_(std::move(server))
    { }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription;
    }

    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes;
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        TServiceId serviceId(request->GetService(), request->GetRealmId());
        auto service = Server_->FindService(serviceId);
        if (!service) {
            auto error = TError(
                EErrorCode::NoSuchService,
                "Service is not registered")
                << TErrorAttribute("service", serviceId.ServiceName)
                << TErrorAttribute("realm_id", serviceId.RealmId);
            responseHandler->HandleError(error);
            return nullptr;
        }

        auto& header = request->Header();
        header.set_start_time(ToProto<i64>(TInstant::Now()));
        if (options.Timeout) {
            header.set_timeout(ToProto<i64>(*options.Timeout));
        } else {
            header.clear_timeout();
        }

        auto serializedRequest = request->Serialize();

        auto session = New<TSession>(std::move(responseHandler), options.Timeout);

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


    class TSession
        : public IBus
    {
    public:
        TSession(IClientResponseHandlerPtr handler, std::optional<TDuration> timeout)
            : Handler_(std::move(handler))
        {
            if (timeout) {
                TDelayedExecutor::Submit(
                    BIND(&TSession::OnTimeout, MakeStrong(this)),
                    *timeout);
            }
        }

        virtual const TString& GetEndpointDescription() const
        {
            return EndpointDescription;
        }

        virtual const IAttributeDictionary& GetEndpointAttributes() const
        {
            return *EndpointAttributes;
        }

        virtual TTcpDispatcherStatistics GetStatistics() const override
        {
            return {};
        }

        virtual const NNet::TNetworkAddress& GetEndpointAddress() const override
        {
            return NNet::NullNetworkAddress;
        }

        virtual TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /*options*/) override
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

        virtual void SetTosLevel(TTosLevel /*tosLevel*/) override
        { }

        virtual void Terminate(const TError& /*error*/) override
        { }

        virtual void SubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
        { }

        virtual void UnsubscribeTerminated(const TCallback<void(const TError&)>& /*callback*/) override
        { }

    private:
        const IClientResponseHandlerPtr Handler_;

        std::atomic<bool> Replied_ = {false};


        bool AcquireLock()
        {
            bool expected = false;
            return Replied_.compare_exchange_strong(expected, true);
        }

        void OnTimeout(bool aborted)
        {
            if (AcquireLock()) {
                TError error;
                if (aborted) {
                    error = TError(NYT::EErrorCode::Canceled, "Request timed out (timer was aborted)");
                } else {
                    error = TError(NYT::EErrorCode::Timeout, "Request timed out");
                }

                ReportError(error);
            }
        }

        void ReportError(const TError& error)
        {
            auto detailedError = error
                << GetEndpointAttributes();
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

} // namespace NYT::NRpc
