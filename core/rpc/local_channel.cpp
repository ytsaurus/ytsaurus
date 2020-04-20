#include "local_channel.h"
#include "channel.h"
#include "client.h"
#include "message.h"
#include "server.h"
#include "service.h"
#include "dispatcher.h"

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

using NYT::FromProto;
using NYT::ToProto;

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

    virtual TNetworkId GetNetworkId() const override
    {
        static auto localNetworkId = TDispatcher::Get()->GetNetworkId(LocalNetworkName);
        return localNetworkId;
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

        TSharedRefArray serializedRequest;
        try {
            serializedRequest = request->Serialize();
        } catch (const std::exception& ex) {
            responseHandler->HandleError(TError(NRpc::EErrorCode::TransportError, "Request serialization failed")
                << ex);
            return nullptr;
        }

        auto session = New<TSession>(std::move(responseHandler), options.Timeout);

        service->HandleRequest(
            std::make_unique<NProto::TRequestHeader>(request->Header()),
            std::move(serializedRequest),
            std::move(session));

        return New<TClientRequestControl>(std::move(service), request->GetRequestId());
    }

    virtual void Terminate(const TError& error) override
    {
        Terminated_.Fire(error);
    }

    virtual void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        Terminated_.Subscribe(callback);
    }

    virtual void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
    {
        Terminated_.Unsubscribe(callback);
    }

private:
    class TSession;
    typedef TIntrusivePtr<TSession> TSessionPtr;

    const IServerPtr Server_;

    TSingleShotCallbackList<void(const TError&)> Terminated_;

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
            YT_VERIFY(ParseResponseHeader(message, &header));
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
        TClientRequestControl(IServicePtr service, TRequestId requestId)
            : Service_(std::move(service))
            , RequestId_(requestId)
        { }

        virtual void Cancel() override
        {
            Service_->HandleRequestCancelation(RequestId_);
        }

        virtual TFuture<void> SendStreamingPayload(const TStreamingPayload& payload) override
        {
            Service_->HandleStreamingPayload(RequestId_, payload);
            return VoidFuture;
        }

        virtual TFuture<void> SendStreamingFeedback(const TStreamingFeedback& feedback) override
        {
            Service_->HandleStreamingFeedback(RequestId_, feedback);
            return VoidFuture;
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
