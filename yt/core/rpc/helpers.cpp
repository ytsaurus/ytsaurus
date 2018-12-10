#include "helpers.h"
#include "client.h"
#include "channel_detail.h"
#include "service.h"

#include <yt/core/ytree/helpers.h>

#include <yt/core/yson/protobuf_interop.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NRpc {

using namespace NYson;
using namespace NRpc::NProto;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

bool IsRetriableError(const TError& error)
{
    if (IsChannelFailureError(error)) {
        return true;
    }
    auto code = error.GetCode();
    return code == NRpc::EErrorCode::RequestQueueSizeLimitExceeded;
}

bool IsChannelFailureError(const TError& error)
{
    auto code = error.GetCode();
    return code == NRpc::EErrorCode::TransportError ||
           code == NRpc::EErrorCode::Unavailable ||
           code == NRpc::EErrorCode::NoSuchService ||
           code == NRpc::EErrorCode::NoSuchMethod ||
           code == NRpc::EErrorCode::ProtocolError ||
           code == NYT::EErrorCode::Timeout;
}

////////////////////////////////////////////////////////////////////////////////

class TDefaultTimeoutChannel
    : public TChannelWrapper
{
public:
    TDefaultTimeoutChannel(IChannelPtr underlyingChannel, TDuration timeout)
        : TChannelWrapper(std::move(underlyingChannel))
        , Timeout_(timeout)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto adjustedOptions = options;
        if (!adjustedOptions.Timeout) {
            adjustedOptions.Timeout = Timeout_;
        }
        return UnderlyingChannel_->Send(
            request,
            responseHandler,
            adjustedOptions);
    }

private:
    const TDuration Timeout_;

};

IChannelPtr CreateDefaultTimeoutChannel(IChannelPtr underlyingChannel, TDuration timeout)
{
    YCHECK(underlyingChannel);

    return New<TDefaultTimeoutChannel>(underlyingChannel, timeout);
}

////////////////////////////////////////////////////////////////////////////////

class TDefaultTimeoutChannelFactory
    : public IChannelFactory
{
public:
    TDefaultTimeoutChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        TDuration timeout)
        : UnderlyingFactory_(underlyingFactory)
        , Timeout_(timeout)
    { }

    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        auto underlyingChannel = UnderlyingFactory_->CreateChannel(address);
        return CreateDefaultTimeoutChannel(underlyingChannel, Timeout_);
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;
    const TDuration Timeout_;

};

IChannelFactoryPtr CreateDefaultTimeoutChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    TDuration timeout)
{
    YCHECK(underlyingFactory);

    return New<TDefaultTimeoutChannelFactory>(underlyingFactory, timeout);
}

////////////////////////////////////////////////////////////////////////////////

class TAuthenticatedChannel
    : public TChannelWrapper
{
public:
    TAuthenticatedChannel(IChannelPtr underlyingChannel, const TString& user)
        : TChannelWrapper(std::move(underlyingChannel))
        , User_(user)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        request->SetUser(User_);
        return UnderlyingChannel_->Send(
            request,
            responseHandler,
            options);
    }

private:
    const TString User_;

};

IChannelPtr CreateAuthenticatedChannel(IChannelPtr underlyingChannel, const TString& user)
{
    YCHECK(underlyingChannel);

    return New<TAuthenticatedChannel>(std::move(underlyingChannel), user);
}

////////////////////////////////////////////////////////////////////////////////

class TAuthenticatedChannelFactory
    : public IChannelFactory
{
public:
    TAuthenticatedChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        const TString& user)
        : UnderlyingFactory_(std::move(underlyingFactory))
        , User_(user)
    { }

    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        auto underlyingChannel = UnderlyingFactory_->CreateChannel(address);
        return CreateAuthenticatedChannel(underlyingChannel, User_);
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;
    const TString User_;

};

IChannelFactoryPtr CreateAuthenticatedChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    const TString& user)
{
    YCHECK(underlyingFactory);

    return New<TAuthenticatedChannelFactory>(std::move(underlyingFactory), user);
}

////////////////////////////////////////////////////////////////////////////////

class TRealmChannel
    : public TChannelWrapper
{
public:
    TRealmChannel(IChannelPtr underlyingChannel, const TRealmId& realmId)
        : TChannelWrapper(std::move(underlyingChannel))
        , RealmId_(realmId)
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        ToProto(request->Header().mutable_realm_id(), RealmId_);
        return UnderlyingChannel_->Send(
            request,
            responseHandler,
            options);
    }

private:
    const TRealmId RealmId_;

};

IChannelPtr CreateRealmChannel(IChannelPtr underlyingChannel, const TRealmId& realmId)
{
    YCHECK(underlyingChannel);

    return New<TRealmChannel>(std::move(underlyingChannel), realmId);
}

////////////////////////////////////////////////////////////////////////////////

class TRealmChannelFactory
    : public IChannelFactory
{
public:
    TRealmChannelFactory(
        IChannelFactoryPtr underlyingFactory,
        const TRealmId& realmId)
        : UnderlyingFactory_(underlyingFactory)
        , RealmId_(realmId)
    { }

    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        auto underlyingChannel = UnderlyingFactory_->CreateChannel(address);
        return CreateRealmChannel(underlyingChannel, RealmId_);
    }

private:
    const IChannelFactoryPtr UnderlyingFactory_;
    const TRealmId RealmId_;

};

IChannelFactoryPtr CreateRealmChannelFactory(
    IChannelFactoryPtr underlyingFactory,
    const TRealmId& realmId)
{
    YCHECK(underlyingFactory);

    return New<TRealmChannelFactory>(std::move(underlyingFactory), realmId);
}

////////////////////////////////////////////////////////////////////////////////

class TFailureDetectingChannel
    : public TChannelWrapper
{
public:
    TFailureDetectingChannel(IChannelPtr underlyingChannel, TCallback<void(IChannelPtr)> onFailure)
        : TChannelWrapper(std::move(underlyingChannel))
        , OnFailure_(std::move(onFailure))
    { }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        return UnderlyingChannel_->Send(
            request,
            New<TResponseHandler>(this, std::move(responseHandler), OnFailure_),
            options);
    }

private:
    const TCallback<void(IChannelPtr)> OnFailure_;

    class TResponseHandler
        : public IClientResponseHandler
    {
    public:
        TResponseHandler(
            IChannelPtr channel,
            IClientResponseHandlerPtr underlyingHandler,
            TCallback<void(IChannelPtr)> onFailure)
            : Channel_(std::move(channel))
            , UnderlyingHandler_(std::move(underlyingHandler))
            , OnFailure_(std::move(onFailure))
        { }

        virtual void HandleAcknowledgement() override
        {
            UnderlyingHandler_->HandleAcknowledgement();
        }

        virtual void HandleResponse(TSharedRefArray message) override
        {
            UnderlyingHandler_->HandleResponse(std::move(message));
        }

        virtual void HandleError(const TError& error) override
        {
            if (IsChannelFailureError(error)) {
                OnFailure_.Run(Channel_);
            }
            UnderlyingHandler_->HandleError(error);
        }

    private:
        const IChannelPtr Channel_;
        const IClientResponseHandlerPtr UnderlyingHandler_;
        const TCallback<void(IChannelPtr)> OnFailure_;

    };

};

IChannelPtr CreateFailureDetectingChannel(
    IChannelPtr underlyingChannel,
    TCallback<void(IChannelPtr)> onFailure)
{
    return New<TFailureDetectingChannel>(
        std::move(underlyingChannel),
        std::move(onFailure));
}

////////////////////////////////////////////////////////////////////////////////

TTraceContext GetTraceContext(const TRequestHeader& header)
{
    if (!header.HasExtension(TTracingExt::tracing_ext)) {
        return TTraceContext();
    }

    const auto& ext = header.GetExtension(TTracingExt::tracing_ext);
    return TTraceContext(
        ext.trace_id(),
        ext.span_id(),
        ext.parent_span_id());
}

void SetTraceContext(TRequestHeader* header, const NTracing::TTraceContext& context)
{
    auto* ext = header->MutableExtension(TTracingExt::tracing_ext);
    ext->set_trace_id(context.GetTraceId());
    ext->set_span_id(context.GetSpanId());
    ext->set_parent_span_id(context.GetParentSpanId());
}

////////////////////////////////////////////////////////////////////////////////

TMutationId GenerateMutationId()
{
    while (true) {
        auto id = TMutationId::Create();
        if (id != NullMutationId) {
            return id;
        }
    }
}

void GenerateMutationId(const IClientRequestPtr& request)
{
    SetMutationId(request, GenerateMutationId(), false);
}

void SetMutationId(TRequestHeader* header, const TMutationId& id, bool retry)
{
    if (id) {
        ToProto(header->mutable_mutation_id(), id);
        if (retry) {
            header->set_retry(true);
        }
    }
}

void SetMutationId(const IClientRequestPtr& request, const TMutationId& id, bool retry)
{
    SetMutationId(&request->Header(), id, retry);
}

void SetOrGenerateMutationId(const IClientRequestPtr& request, const TMutationId& id, bool retry)
{
    SetMutationId(request, id ? id : TMutationId::Create(), retry);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
