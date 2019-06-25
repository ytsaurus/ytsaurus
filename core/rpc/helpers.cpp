#include "helpers.h"
#include "client.h"
#include "dispatcher.h"
#include "channel_detail.h"
#include "service.h"

#include <yt/core/ytree/helpers.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/yson/consumer.h>

#include <yt/core/yson/protobuf_interop.h>

#include <yt/core/misc/hash.h>

#include <contrib/libs/protobuf/io/coded_stream.h>
#include <contrib/libs/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYT::NRpc {

using namespace NYson;
using namespace NYTree;
using namespace NRpc::NProto;
using namespace NTracing;
using namespace NYT::NBus;

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TAddressWithNetwork& lhs, const TAddressWithNetwork& rhs)
{
    return lhs.Address == rhs.Address && lhs.Network == rhs.Network;
}

TString ToString(const TAddressWithNetwork& addressWithNetwork)
{
    return Format("%v/%v", addressWithNetwork.Address, addressWithNetwork.Network);
}

void Serialize(const TAddressWithNetwork& addressWithNetwork, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("address").Value(addressWithNetwork.Address)
            .Item("network").Value(addressWithNetwork.Network)
        .EndMap();
}

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

    virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override
    {
        auto underlyingChannel = UnderlyingFactory_->CreateChannel(addressWithNetwork);
        return CreateDefaultTimeoutChannel(underlyingChannel, Timeout_);
    }

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

    virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override
    {
        auto underlyingChannel = UnderlyingFactory_->CreateChannel(addressWithNetwork);
        return CreateAuthenticatedChannel(underlyingChannel, User_);
    }

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
    TRealmChannel(IChannelPtr underlyingChannel, TRealmId realmId)
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

IChannelPtr CreateRealmChannel(IChannelPtr underlyingChannel, TRealmId realmId)
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
        TRealmId realmId)
        : UnderlyingFactory_(underlyingFactory)
        , RealmId_(realmId)
    { }

    virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override
    {
        auto underlyingChannel = UnderlyingFactory_->CreateChannel(addressWithNetwork);
        return CreateRealmChannel(underlyingChannel, RealmId_);
    }

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
    TRealmId realmId)
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

        virtual void HandleStreamingPayload(const TStreamingPayload& payload) override
        {
            UnderlyingHandler_->HandleStreamingPayload(payload);
        }

        virtual void HandleStreamingFeedback(const TStreamingFeedback& feedback) override
        {
            UnderlyingHandler_->HandleStreamingFeedback(feedback);
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

TSpanContext GetSpanContext(const TRequestHeader& header)
{
    if (!header.HasExtension(TTracingExt::tracing_ext)) {
        return {};
    }

    const auto& ext = header.GetExtension(TTracingExt::tracing_ext);

    auto traceId = InvalidTraceId;
    if (ext.has_trace_id()) {
        FromProto(&traceId, ext.trace_id());
    } else if (ext.has_trace_id_old()) {
        // COMPAT(prime)
        traceId.Parts64[0] = ext.trace_id_old();
    }

    auto spanId = InvalidSpanId;
    if (ext.has_span_id()) {
        spanId = ext.span_id();
    } else if (ext.has_span_id_old()) {
        spanId = ext.span_id_old();
    }

    return {
        traceId,
        spanId,
        ext.sampled(),
        ext.debug()
    };
}

TTraceContextPtr GetOrCreateTraceContext(const NProto::TRequestHeader& header)
{
    auto clientSpan = GetSpanContext(header);
    auto spanName = header.service() + "." + header.method();
    if (clientSpan.TraceId != InvalidTraceId) {
        return New<NTracing::TTraceContext>(clientSpan, spanName);
    } else {
        return CreateRootTraceContext(spanName);
    }
}

TTraceContextPtr CreateCallTraceContext(const TString& service, const TString& method)
{
    auto context = GetCurrentTraceContext();
    if (!context) {
        return nullptr;
    }

    auto spanName = service + "." + method;
    return CreateChildTraceContext(spanName);
}

void SetTraceContext(TRequestHeader* header, const TTraceContextPtr& traceContext)
{
    if (!traceContext) {
        return;
    }

    auto* ext = header->MutableExtension(TTracingExt::tracing_ext);
    ToProto(ext->mutable_trace_id(), traceContext->GetTraceId());
    ext->set_span_id(traceContext->GetSpanId());
    ext->set_sampled(traceContext->IsSampled());
    ext->set_debug(traceContext->IsDebug());

    // COMPAT(prime)
    ext->set_trace_id_old(traceContext->GetTraceId().Parts64[0]);
    ext->set_span_id_old(traceContext->GetSpanId());
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

void SetMutationId(TRequestHeader* header, TMutationId id, bool retry)
{
    if (id) {
        ToProto(header->mutable_mutation_id(), id);
        if (retry) {
            header->set_retry(true);
        }
    }
}

void SetMutationId(const IClientRequestPtr& request, TMutationId id, bool retry)
{
    SetMutationId(&request->Header(), id, retry);
}

void SetOrGenerateMutationId(const IClientRequestPtr& request, TMutationId id, bool retry)
{
    SetMutationId(request, id ? id : TMutationId::Create(), retry);
}

////////////////////////////////////////////////////////////////////////////////

template <class TInput, class TFunctor>
TFuture<std::vector<std::invoke_result_t<TFunctor, const TInput&>>> AsyncTransform(
    TRange<TInput> input,
    const TFunctor& unaryFunc,
    const IInvokerPtr& invoker)
{
    using TOutput = std::invoke_result_t<TFunctor, const TInput&>;
    std::vector<TFuture<TOutput>> asyncResults(input.Size());
    std::transform(
        input.Begin(),
        input.End(),
        asyncResults.begin(),
        [&] (const TInput& value) {
            return BIND(unaryFunc, value)
                .AsyncVia(invoker)
                .Run();
        });

    return Combine(asyncResults);
}

////////////////////////////////////////////////////////////////////////////////

TFuture<std::vector<TSharedRef>> AsyncCompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId)
{
    if (codecId == NCompression::ECodec::None) {
        return MakeFuture(attachments.ToVector());
    }

    auto* codec = NCompression::GetCodec(codecId);
    return AsyncTransform(
        attachments,
        [=] (const TSharedRef& attachment) {
            return codec->Compress(attachment);
        },
        TDispatcher::Get()->GetCompressionPoolInvoker());
}

TFuture<std::vector<TSharedRef>> AsyncDecompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId)
{
    if (codecId == NCompression::ECodec::None) {
        return MakeFuture(attachments.ToVector());
    }

    auto* codec = NCompression::GetCodec(codecId);
    return AsyncTransform(
        attachments,
        [=] (const TSharedRef& compressedAttachment) {
            return codec->Decompress(compressedAttachment);
        },
        TDispatcher::Get()->GetCompressionPoolInvoker());
}

std::vector<TSharedRef> CompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId)
{
    if (codecId == NCompression::ECodec::None) {
        return attachments.ToVector();
    }
    return NConcurrency::WaitFor(AsyncCompressAttachments(attachments, codecId))
        .ValueOrThrow();
}

std::vector<TSharedRef> DecompressAttachments(
    TRange<TSharedRef> attachments,
    NCompression::ECodec codecId)
{
    if (codecId == NCompression::ECodec::None) {
        return attachments.ToVector();
    }
    return NConcurrency::WaitFor(AsyncDecompressAttachments(attachments, codecId))
        .ValueOrThrow();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NRpc::TAddressWithNetwork>::operator()(const NYT::NRpc::TAddressWithNetwork& addressWithNetwork) const
{
    size_t hash = 0;
    NYT::HashCombine(hash, addressWithNetwork.Address);
    NYT::HashCombine(hash, addressWithNetwork.Network);
    return hash;
}

/////////////////////////////////////////////////////////////////////////////
