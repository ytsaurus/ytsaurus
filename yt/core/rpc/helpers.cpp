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

using NYT::FromProto;

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
    return code == NRpc::EErrorCode::RequestQueueSizeLimitExceeded ||
           code == NYT::EErrorCode::Timeout;
}

bool IsChannelFailureError(const TError& error)
{
    auto code = error.GetCode();
    return code == NRpc::EErrorCode::TransportError ||
           code == NRpc::EErrorCode::Unavailable ||
           code == NRpc::EErrorCode::NoSuchService ||
           code == NRpc::EErrorCode::NoSuchMethod ||
           code == NRpc::EErrorCode::ProtocolError;
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
    YT_VERIFY(underlyingChannel);

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
    YT_VERIFY(underlyingFactory);

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
    YT_VERIFY(underlyingChannel);

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
    YT_VERIFY(underlyingFactory);

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
    YT_VERIFY(underlyingChannel);

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
    YT_VERIFY(underlyingFactory);

    return New<TRealmChannelFactory>(std::move(underlyingFactory), realmId);
}

////////////////////////////////////////////////////////////////////////////////

class TFailureDetectingChannel
    : public TChannelWrapper
{
public:
    TFailureDetectingChannel(
        IChannelPtr underlyingChannel,
        std::optional<TDuration> acknowledgementTimeout,
        TCallback<void(const IChannelPtr&, const TError&)> onFailure,
        TCallback<bool(const TError&)> isError)
        : TChannelWrapper(std::move(underlyingChannel))
        , AcknowledgementTimeout_(acknowledgementTimeout)
        , OnFailure_(std::move(onFailure))
        , IsError_(std::move(isError))
        , OnTerminated_(BIND(&TFailureDetectingChannel::OnTerminated, MakeWeak(this)))
    {
        UnderlyingChannel_->SubscribeTerminated(OnTerminated_);
    }

    ~TFailureDetectingChannel()
    {
        UnderlyingChannel_->UnsubscribeTerminated(OnTerminated_);
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto updatedOptions = options;
        if (AcknowledgementTimeout_) {
            updatedOptions.AcknowledgementTimeout = AcknowledgementTimeout_;
        }
        return UnderlyingChannel_->Send(
            request,
            New<TResponseHandler>(this, std::move(responseHandler), OnFailure_, IsError_),
            updatedOptions);
    }

private:
    const std::optional<TDuration> AcknowledgementTimeout_;
    const TCallback<void(const IChannelPtr&, const TError&)> OnFailure_;
    const TCallback<bool(const TError&)> IsError_;
    const TCallback<void(const TError&)> OnTerminated_;


    void OnTerminated(const TError& error)
    {
        OnFailure_(this, error);
    }

    class TResponseHandler
        : public IClientResponseHandler
    {
    public:
        TResponseHandler(
            IChannelPtr channel,
            IClientResponseHandlerPtr underlyingHandler,
            TCallback<void(const IChannelPtr&, const TError&)> onFailure,
            TCallback<bool(const TError&)> isError)
            : Channel_(std::move(channel))
            , UnderlyingHandler_(std::move(underlyingHandler))
            , OnFailure_(std::move(onFailure))
            , IsError_(std::move(isError))
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
            if (IsError_(error)) {
                OnFailure_.Run(Channel_, error);
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
        const TCallback<void(const IChannelPtr&, const TError&)> OnFailure_;
        const TCallback<bool(const TError&)> IsError_;
    };
};

IChannelPtr CreateFailureDetectingChannel(
    IChannelPtr underlyingChannel,
    std::optional<TDuration> acknowledgementTimeout,
    TCallback<void(const IChannelPtr&, const TError& error)> onFailure,
    TCallback<bool(const TError&)> isError)
{
    return New<TFailureDetectingChannel>(
        std::move(underlyingChannel),
        acknowledgementTimeout,
        std::move(onFailure),
        std::move(isError));
}

////////////////////////////////////////////////////////////////////////////////

TTraceContextPtr CreateHandlerTraceContext(const NProto::TRequestHeader& header)
{
    const auto& ext = header.GetExtension(NProto::TRequestHeader::tracing_ext);
    // Fast path.
    if (!ext.has_trace_id()) {
        return nullptr;
    }
    // Slow path.
    return NTracing::CreateChildTraceContext(
        ext,
        ConcatToString(AsStringBuf("RpcServer:"), header.service(), AsStringBuf("."), header.method()));
}

TTraceContextPtr CreateCallTraceContext(const TString& service, const TString& method)
{
    auto context = GetCurrentTraceContext();
    // Fast path.
    if (!context) {
        return nullptr;
    }
    // Slow path.
    return CreateChildTraceContext(
        std::move(context),
        ConcatToString(AsStringBuf("RpcClient:"), service, AsStringBuf("."), method));
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

TMutationId GenerateNextBatchMutationId(TMutationId id)
{
    ++id.Parts32[0];
    return id;
}

TMutationId GenerateNextForwardedMutationId(TMutationId id)
{
    ++id.Parts32[1];
    return id;
}

void GenerateMutationId(const IClientRequestPtr& request)
{
    SetMutationId(request, GenerateMutationId(), false);
}

TMutationId GetMutationId(const TRequestHeader& header)
{
    return FromProto<TMutationId>(header.mutation_id());
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
