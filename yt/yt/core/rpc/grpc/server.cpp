#include "server.h"
#include "dispatcher.h"
#include "config.h"
#include "helpers.h"

#include <yt/core/rpc/grpc/proto/grpc.pb.h>

#include <yt/core/rpc/server_detail.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/bus/bus.h>

#include <yt/core/misc/small_vector.h>

#include <yt/core/net/address.h>

#include <yt/core/ytree/convert.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/grpc_security.h>

#include <array>

namespace NYT::NRpc::NGrpc {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;
using namespace NNet;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServer)

DEFINE_ENUM(EServerCallStage,
    (Accept)
    (ReceivingRequest)
    (SendingInitialMetadata)
    (WaitingForService)
    (SendingResponse)
    (WaitingForClose)
    (Done)
);

DEFINE_ENUM(EServerCallCookie,
    (Normal)
    (Close)
);

class TServer
    : public TServerBase
{
public:
    explicit TServer(TServerConfigPtr config)
        : TServerBase(NLogging::TLogger(GrpcLogger)
            .AddTag("ServerId: %v", TGuid::Create()))
        , Config_(std::move(config))
        , LibraryLock_(TDispatcher::Get()->CreateLibraryLock())
        , CompletionQueue_(TDispatcher::Get()->PickRandomCompletionQueue())
    { }

private:
    const TServerConfigPtr Config_;

    const TGrpcLibraryLockPtr LibraryLock_;
    grpc_completion_queue* const CompletionQueue_;

    TGrpcServerPtr Native_;
    std::vector<TGrpcServerCredentialsPtr> Credentials_;

    std::atomic<int> CallHandlerCount_ = {0};
    TPromise<void> ShutdownPromise_ = NewPromise<void>();


    void OnCallHandlerConstructed()
    {
        ++CallHandlerCount_;
    }

    void OnCallHandlerDestroyed()
    {
        if (--CallHandlerCount_ > 0) {
            return;
        }

        Cleanup();
        ShutdownPromise_.SetFrom(TServerBase::DoStop(true));
        Unref();
    }


    virtual void DoStart() override
    {
        TGrpcChannelArgs args(Config_->GrpcArguments);

        Native_ = TGrpcServerPtr(grpc_server_create(
            args.Unwrap(),
            nullptr));

        grpc_server_register_completion_queue(
            Native_.Unwrap(),
            CompletionQueue_,
            nullptr);

        try {
            for (const auto& addressConfig : Config_->Addresses) {
                int result;
                if (addressConfig->Credentials) {
                    Credentials_.push_back(LoadServerCredentials(addressConfig->Credentials));
                    result = grpc_server_add_secure_http2_port(
                        Native_.Unwrap(),
                        addressConfig->Address.c_str(),
                        Credentials_.back().Unwrap());
                } else {
                    result = grpc_server_add_insecure_http2_port(
                        Native_.Unwrap(),
                        addressConfig->Address.c_str());
                }
                if (result == 0) {
                    THROW_ERROR_EXCEPTION("Error configuring server to listen at %Qv",
                        addressConfig->Address);
                }
                YT_LOG_DEBUG("Server address configured (Address: %v)", addressConfig->Address);
            }
        } catch (const std::exception& ex) {
            Cleanup();
            throw;
        }

        grpc_server_start(Native_.Unwrap());

        Ref();

        // This instance is fake; see DoStop.
        OnCallHandlerConstructed();

        TServerBase::DoStart();

        New<TCallHandler>(this);
    }

    virtual TFuture<void> DoStop(bool graceful) override
    {
        class TStopTag
            : public TCompletionQueueTag
        {
        public:
            explicit TStopTag(TServerPtr owner)
                : Owner_(std::move(owner))
            { }

            virtual void Run(bool success, int /*cookie*/) override
            {
                YT_VERIFY(success);
                Owner_->OnCallHandlerDestroyed();
                delete this;
            }

        private:
            const TServerPtr Owner_;
        };

        auto* shutdownTag = new TStopTag(this);

        grpc_server_shutdown_and_notify(Native_.Unwrap(), CompletionQueue_, shutdownTag->GetTag());

        if (!graceful) {
            grpc_server_cancel_all_calls(Native_.Unwrap());
        }

        return ShutdownPromise_;
    }

    void Cleanup()
    {
        Native_.Reset();
    }

    class TCallHandler;

    class TReplyBus
        : public IBus
    {
    public:
        explicit TReplyBus(TCallHandler* handler)
            : Handler_(MakeWeak(handler))
            , PeerAddress_(handler->PeerAddress_)
            , PeerAddressString_(handler->PeerAddressString_)
        { }

        // IBus overrides
        virtual const TString& GetEndpointDescription() const override
        {
            return PeerAddressString_;
        }

        virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
        {
            YT_ABORT();
        }

        virtual TTcpDispatcherStatistics GetStatistics() const override
        {
            return {};
        }

        virtual const NNet::TNetworkAddress& GetEndpointAddress() const override
        {
            return PeerAddress_;
        }

        virtual TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /*options*/) override
        {
            if (auto handler = Handler_.Lock()) {
                handler->OnResponseMessage(std::move(message));
            }
            return {};
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
        const TWeakPtr<TCallHandler> Handler_;
        const TNetworkAddress PeerAddress_;
        const TString PeerAddressString_;
    };

    class TCallHandler
        : public TCompletionQueueTag
        , public TRefCounted
    {
    public:
        explicit TCallHandler(TServerPtr owner)
            : Owner_(std::move(owner))
            , CompletionQueue_(TDispatcher::Get()->PickRandomCompletionQueue())
            , Logger(Owner_->Logger)
        {
            auto result = grpc_server_request_call(
                Owner_->Native_.Unwrap(),
                Call_.GetPtr(),
                CallDetails_.Unwrap(),
                CallMetadata_.Unwrap(),
                CompletionQueue_,
                Owner_->CompletionQueue_,
                GetTag());
            YT_VERIFY(result == GRPC_CALL_OK);

            Ref();
            Owner_->OnCallHandlerConstructed();
        }

        ~TCallHandler()
        {
            Owner_->OnCallHandlerDestroyed();
        }

        // TCompletionQueueTag overrides
        virtual void Run(bool success, int cookie_) override
        {
            auto cookie = static_cast<EServerCallCookie>(cookie_);
            switch (cookie) {
                case EServerCallCookie::Normal:
                    switch (Stage_) {
                        case EServerCallStage::Accept:
                            OnAccepted(success);
                            break;

                        case EServerCallStage::ReceivingRequest:
                            OnRequestReceived(success);
                            break;

                        case EServerCallStage::SendingInitialMetadata:
                            OnInitialMetadataSent(success);
                            break;

                        case EServerCallStage::SendingResponse:
                            OnResponseSent(success);
                            break;

                        default:
                            YT_ABORT();
                    }
                    break;

                case EServerCallCookie::Close:
                    OnCloseReceived(success);
                    break;

                default:
                    YT_ABORT();
            }
        }

    private:
        friend class TReplyBus;

        const TServerPtr Owner_;

        grpc_completion_queue* const CompletionQueue_;
        const NLogging::TLogger& Logger;

        TSpinLock SpinLock_;
        EServerCallStage Stage_ = EServerCallStage::Accept;
        bool CancelRequested_ = false;
        TSharedRefArray ResponseMessage_;

        TString PeerAddressString_;
        TNetworkAddress PeerAddress_;

        TRequestId RequestId_;
        std::optional<TString> User_;
        std::optional<TString> UserAgent_;
        std::optional<NGrpc::NProto::TSslCredentialsExt> SslCredentialsExt_;
        std::optional<NRpc::NProto::TCredentialsExt> RpcCredentialsExt_;
        TString ServiceName_;
        TString MethodName_;
        std::optional<TDuration> Timeout_;
        IServicePtr Service_;

        TGrpcMetadataArrayBuilder InitialMetadataBuilder_;
        TGrpcMetadataArrayBuilder TrailingMetadataBuilder_;

        TGrpcCallDetails CallDetails_;
        TGrpcMetadataArray CallMetadata_;
        TGrpcCallPtr Call_;
        TGrpcByteBufferPtr RequestBodyBuffer_;
        std::optional<ui32> RequestMessageBodySize_;
        TProtocolVersion ProtocolVersion_ = DefaultProtocolVersion;
        TGrpcByteBufferPtr ResponseBodyBuffer_;
        TString ErrorMessage_;
        grpc_slice ErrorMessageSlice_ = grpc_empty_slice();
        int RawCanceled_ = 0;


        template <class TOps>
        void StartBatch(const TOps& ops, EServerCallCookie cookie)
        {
            auto result = grpc_call_start_batch(
                Call_.Unwrap(),
                ops.data(),
                ops.size(),
                GetTag(static_cast<int>(cookie)),
                nullptr);
            YT_VERIFY(result == GRPC_CALL_OK);
        }

        void OnAccepted(bool success)
        {
            if (!success) {
                // This normally happens on server shutdown.
                YT_LOG_DEBUG("Server accept failed");
                Unref();
                return;
            }

            New<TCallHandler>(Owner_);

            if (!TryParsePeerAddress()) {
                YT_LOG_DEBUG("Malformed peer address (PeerAddress: %v, RequestId: %v)",
                    PeerAddressString_,
                    RequestId_);
                Unref();
                return;
            }

            ParseRequestId();
            ParseUser();
            ParseUserAgent();
            ParseRpcCredentials();
            ParseSslCredentials();
            ParseTimeout();

            if (!TryParseRoutingParameters()) {
                YT_LOG_DEBUG("Malformed request routing parameters (RawMethod: %v, RequestId: %v)",
                    ToStringBuf(CallDetails_->method),
                    RequestId_);
                Unref();
                return;
            }

            if (!TryParseMessageBodySize()) {
                Unref();
                return;
            }

            if (!TryParseProtocolVersion()) {
                Unref();
                return;
            }

            YT_LOG_DEBUG("Request accepted (RequestId: %v, Host: %v, Method: %v.%v, User: %v, PeerAddress: %v, Timeout: %v, ProtocolVersion: %v)",
                RequestId_,
                ToStringBuf(CallDetails_->host),
                ServiceName_,
                MethodName_,
                User_,
                PeerAddressString_,
                Timeout_,
                ProtocolVersion_);

            Service_ = Owner_->FindService(TServiceId(ServiceName_));

            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::ReceivingRequest;
            }

            std::array<grpc_op, 1> ops;

            ops[0].op = GRPC_OP_RECV_MESSAGE;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.recv_message.recv_message = RequestBodyBuffer_.GetPtr();

            StartBatch(ops, EServerCallCookie::Normal);
        }

        bool TryParsePeerAddress()
        {
            auto addressString = MakeGprString(grpc_call_get_peer(Call_.Unwrap()));
            PeerAddressString_ = TString(addressString.get());

            if (PeerAddressString_.StartsWith("ipv6:") || PeerAddressString_.StartsWith("ipv4:")) {
                PeerAddressString_ = PeerAddressString_.substr(5);
            }

            auto address = NNet::TNetworkAddress::TryParse(PeerAddressString_);
            if (address.IsOK()) {
                PeerAddress_ = address.Value();
                return true;
            } else {
                return false;
            }
        }

        void ParseRequestId()
        {
            auto idString = CallMetadata_.Find(RequestIdMetadataKey);
            if (!idString) {
                RequestId_ = TRequestId::Create();
                return;
            }

            if (!TRequestId::FromString(idString, &RequestId_)) {
                RequestId_ = TRequestId::Create();
                YT_LOG_WARNING("Malformed request id, using a random one (MalformedRequestId: %v, RequestId: %v)",
                    idString,
                    RequestId_);
            }
        }

        void ParseUser()
        {
            auto userString = CallMetadata_.Find(UserMetadataKey);
            if (!userString) {
                return;
            }

            User_ = TString(userString);
        }

        void ParseUserAgent()
        {
            auto userAgentString = CallMetadata_.Find(UserAgentMetadataKey);
            if (!userAgentString) {
                return;
            }

            UserAgent_ = TString(userAgentString);
        }

        void ParseRpcCredentials()
        {
            // COMPAT(babenko)
            auto legacyTokenString = CallMetadata_.Find("yt-token");
            auto tokenString = CallMetadata_.Find(AuthTokenMetadataKey);
            auto sessionIdString = CallMetadata_.Find(AuthSessionIdMetadataKey);
            auto sslSessionIdString = CallMetadata_.Find(AuthSslSessionIdMetadataKey);
            auto userTicketString = CallMetadata_.Find(AuthUserTicketMetadataKey);

            if (!tokenString &&
                !legacyTokenString &&
                !sessionIdString &&
                !sslSessionIdString &&
                !userTicketString)
            {
                return;
            }

            RpcCredentialsExt_.emplace();

            if (legacyTokenString) {
                RpcCredentialsExt_->set_token(TString(legacyTokenString));
            }
            if (tokenString) {
                RpcCredentialsExt_->set_token(TString(tokenString));
            }
            if (sessionIdString) {
                RpcCredentialsExt_->set_session_id(TString(sessionIdString));
            }
            if (sslSessionIdString) {
                RpcCredentialsExt_->set_ssl_session_id(TString(sslSessionIdString));
            }
            if (userTicketString) {
                RpcCredentialsExt_->set_user_ticket(TString(userTicketString));
            }
        }

        void ParseSslCredentials()
        {
            auto authContext = TGrpcAuthContextPtr(grpc_call_auth_context(Call_.Unwrap()));
            if (!authContext) {
                return;
            }

            ParsePeerIdentity(authContext);
            ParseIssuer(authContext);
        }

        void ParsePeerIdentity(const TGrpcAuthContextPtr& authContext)
        {
            const char* peerIdentityPropertyName = grpc_auth_context_peer_identity_property_name(authContext.Unwrap());
            if (!peerIdentityPropertyName) {
                return;
            }

            auto peerIdentityPropertyIt = grpc_auth_context_find_properties_by_name(authContext.Unwrap(), peerIdentityPropertyName);
            auto* peerIdentityProperty = grpc_auth_property_iterator_next(&peerIdentityPropertyIt);
            if (!peerIdentityProperty) {
                return;
            }

            if (!SslCredentialsExt_) {
                SslCredentialsExt_.emplace();
            }
            SslCredentialsExt_->set_peer_identity(TString(peerIdentityProperty->value, peerIdentityProperty->value_length));
        }

        void ParseIssuer(const TGrpcAuthContextPtr& authContext)
        {
            const char* peerIdentityPropertyName = grpc_auth_context_peer_identity_property_name(authContext.Unwrap());
            if (!peerIdentityPropertyName) {
                return;
            }

            auto pemCertPropertyIt = grpc_auth_context_find_properties_by_name(authContext.Unwrap(), GRPC_X509_PEM_CERT_PROPERTY_NAME);
            auto* pemCertProperty = grpc_auth_property_iterator_next(&pemCertPropertyIt);
            if (!pemCertProperty) {
                return;
            }

            auto issuer = ParseIssuerFromX509(TStringBuf(pemCertProperty->value, pemCertProperty->value_length));
            if (!issuer) {
                return;
            }

            if (!SslCredentialsExt_) {
                SslCredentialsExt_.emplace();
            }
            SslCredentialsExt_->set_issuer(std::move(*issuer));
        }

        void ParseTimeout()
        {
            auto deadline = CallDetails_->deadline;
            deadline = gpr_convert_clock_type(deadline, GPR_CLOCK_REALTIME);
            auto now = gpr_now(GPR_CLOCK_REALTIME);
            if (gpr_time_cmp(now, deadline) >= 0) {
                Timeout_ = TDuration::Zero();
                return;
            }

            auto micros = gpr_timespec_to_micros(gpr_time_sub(deadline, now));
            if (micros > std::numeric_limits<ui64>::max() / 2) {
                return;
            }

            Timeout_ = TDuration::MicroSeconds(static_cast<ui64>(micros));
        }

        bool TryParseRoutingParameters()
        {
            const size_t methodLength = GRPC_SLICE_LENGTH(CallDetails_->method);
            if (methodLength == 0) {
                return false;
            }

            if (*GRPC_SLICE_START_PTR(CallDetails_->method) != '/') {
                return false;
            }

            auto methodWithoutLeadingSlash = grpc_slice_sub_no_ref(CallDetails_->method, 1, methodLength);
            const int secondSlashIndex = grpc_slice_chr(methodWithoutLeadingSlash, '/');
            if (secondSlashIndex < 0) {
                return false;
            }

            const char *serviceNameStart = reinterpret_cast<const char *>(GRPC_SLICE_START_PTR(methodWithoutLeadingSlash));
            ServiceName_.assign(serviceNameStart, secondSlashIndex);
            MethodName_.assign(serviceNameStart + secondSlashIndex + 1, methodLength - 1 - (secondSlashIndex + 1));
            return true;
        }

        bool TryParseMessageBodySize()
        {
            auto messageBodySizeString = CallMetadata_.Find(MessageBodySizeMetadataKey);
            if (!messageBodySizeString) {
                return true;
            }

            try {
                RequestMessageBodySize_ = FromString<ui32>(messageBodySizeString);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to parse message body size from request metadata (RequestId: %v)",
                    RequestId_);
                return false;
            }

            return true;
        }

        bool TryParseProtocolVersion()
        {
            auto protocolVersionString = CallMetadata_.Find(ProtocolVersionMetadataKey);
            if (!protocolVersionString) {
                return true;
            }

            try {
                ProtocolVersion_ = TProtocolVersion::FromString(protocolVersionString);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Failed to parse protocol version from string (RequestId: %v)",
                    RequestId_);
                return false;
            }

            return true;
        }

        void OnRequestReceived(bool success)
        {
            if (!success) {
                YT_LOG_DEBUG("Failed to receive request body (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            if (!RequestBodyBuffer_) {
                YT_LOG_DEBUG("Empty request body received (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            TMessageWithAttachments messageWithAttachments;
            try {
                messageWithAttachments = ByteBufferToMessageWithAttachments(
                    RequestBodyBuffer_.Unwrap(),
                    RequestMessageBodySize_);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to receive request body (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            auto header = std::make_unique<NRpc::NProto::TRequestHeader>();
            ToProto(header->mutable_request_id(), RequestId_);
            if (User_) {
                header->set_user(*User_);
            }
            if (UserAgent_) {
                header->set_user_agent(*UserAgent_);
            }
            header->set_service(ServiceName_);
            header->set_method(MethodName_);
            header->set_protocol_version_major(ProtocolVersion_.Major);
            header->set_protocol_version_minor(ProtocolVersion_.Minor);
            if (Timeout_) {
                header->set_timeout(ToProto<i64>(*Timeout_));
            }
            if (SslCredentialsExt_) {
                *header->MutableExtension(NGrpc::NProto::TSslCredentialsExt::ssl_credentials_ext) = std::move(*SslCredentialsExt_);
            }
            if (RpcCredentialsExt_) {
                *header->MutableExtension(NRpc::NProto::TCredentialsExt::credentials_ext) = std::move(*RpcCredentialsExt_);
            }

            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::SendingInitialMetadata;
            }

            YT_LOG_DEBUG("Request received (RequestId: %v)",
                RequestId_);

            InitialMetadataBuilder_.Add(RequestIdMetadataKey, ToString(RequestId_));

            {
                std::array<grpc_op, 1> ops;

                ops[0].op = GRPC_OP_SEND_INITIAL_METADATA;
                ops[0].flags = 0;
                ops[0].reserved = nullptr;
                ops[0].data.send_initial_metadata.maybe_compression_level.is_set = false;
                ops[0].data.send_initial_metadata.metadata = InitialMetadataBuilder_.Unwrap();
                ops[0].data.send_initial_metadata.count = InitialMetadataBuilder_.GetSize();

                StartBatch(ops, EServerCallCookie::Normal);
            }

            {
                Ref();

                std::array<grpc_op, 1> ops;

                ops[0].op = GRPC_OP_RECV_CLOSE_ON_SERVER;
                ops[0].flags = 0;
                ops[0].reserved = nullptr;
                ops[0].data.recv_close_on_server.cancelled = &RawCanceled_;

                StartBatch(ops, EServerCallCookie::Close);
            }

            auto replyBus = New<TReplyBus>(this);
            if (Service_) {
                auto requestMessage = CreateRequestMessage(
                    *header,
                    messageWithAttachments.Message,
                    messageWithAttachments.Attachments);

                Service_->HandleRequest(std::move(header), std::move(requestMessage), std::move(replyBus));
            } else {
                auto error = TError(
                    NRpc::EErrorCode::NoSuchService,
                    "Service is not registered")
                    << TErrorAttribute("service", ServiceName_);
                YT_LOG_WARNING(error);

                auto responseMessage = CreateErrorResponseMessage(RequestId_, error);
                replyBus->Send(std::move(responseMessage), NBus::TSendOptions(EDeliveryTrackingLevel::None));
            }
        }

        void OnInitialMetadataSent(bool success)
        {
            if (!success) {
                YT_LOG_DEBUG("Failed to send initial metadata (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            {
                auto guard = Guard(SpinLock_);
                if (ResponseMessage_) {
                    SendResponse(guard);
                } else {
                    Stage_ = EServerCallStage::WaitingForService;
                    CheckCanceled(guard);
                }
            }
        }

        void OnResponseMessage(TSharedRefArray message)
        {
            auto guard = Guard(SpinLock_);

            YT_VERIFY(!ResponseMessage_);
            ResponseMessage_ = std::move(message);

            if (Stage_ == EServerCallStage::WaitingForService) {
                SendResponse(guard);
            }
        }

        void SendResponse(TGuard<TSpinLock>& guard)
        {
            Stage_ = EServerCallStage::SendingResponse;
            guard.Release();

            YT_LOG_DEBUG("Sending response (RequestId: %v)",
                RequestId_);

            NRpc::NProto::TResponseHeader responseHeader;
            YT_VERIFY(ParseResponseHeader(ResponseMessage_, &responseHeader));

            SmallVector<grpc_op, 2> ops;

            TError error;
            if (responseHeader.has_error() && responseHeader.error().code() != static_cast<int>(NYT::EErrorCode::OK)) {
                FromProto(&error, responseHeader.error());
                ErrorMessage_ = ToString(error);
                ErrorMessageSlice_ = grpc_slice_from_static_string(ErrorMessage_.c_str());
                TrailingMetadataBuilder_.Add(ErrorMetadataKey, SerializeError(error));
            } else {
                YT_VERIFY(ResponseMessage_.Size() >= 2);

                TMessageWithAttachments messageWithAttachments;
                messageWithAttachments.Message = ExtractMessageFromEnvelopedMessage(ResponseMessage_[1]);
                for (int index = 2; index < ResponseMessage_.Size(); ++index) {
                    messageWithAttachments.Attachments.push_back(ResponseMessage_[index]);
                }

                ResponseBodyBuffer_ = MessageWithAttachmentsToByteBuffer(messageWithAttachments);

                if (!messageWithAttachments.Attachments.empty()) {
                    TrailingMetadataBuilder_.Add(MessageBodySizeMetadataKey, ToString(messageWithAttachments.Message.Size()));
                }

                ops.emplace_back();
                ops.back().op = GRPC_OP_SEND_MESSAGE;
                ops.back().data.send_message.send_message = ResponseBodyBuffer_.Unwrap();
                ops.back().flags = 0;
                ops.back().reserved = nullptr;
            }

            ops.emplace_back();
            ops.back().op = GRPC_OP_SEND_STATUS_FROM_SERVER;
            ops.back().flags = 0;
            ops.back().reserved = nullptr;
            ops.back().data.send_status_from_server.status = error.IsOK() ? GRPC_STATUS_OK : grpc_status_code(GenericErrorStatusCode);
            ops.back().data.send_status_from_server.status_details = error.IsOK() ? nullptr : &ErrorMessageSlice_;
            ops.back().data.send_status_from_server.trailing_metadata_count = TrailingMetadataBuilder_.GetSize();
            ops.back().data.send_status_from_server.trailing_metadata = TrailingMetadataBuilder_.Unwrap();

            StartBatch(ops, EServerCallCookie::Normal);
        }


        void OnResponseSent(bool success)
        {
            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::Done;
            }

            if (success) {
                YT_LOG_DEBUG("Response sent (RequestId: %v)",
                    RequestId_);
            } else {
                YT_LOG_DEBUG("Failed to send response (RequestId: %v)",
                    RequestId_);
            }

            Unref();
        }

        void OnCloseReceived(bool success)
        {
            if (success) {
                if (RawCanceled_) {
                    OnCanceled();
                } else {
                    YT_LOG_DEBUG("Request closed (RequestId: %v)",
                        RequestId_);
                }
            } else {
                YT_LOG_DEBUG("Failed to close request (RequestId: %v)",
                    RequestId_);
            }

            Unref();
        }

        void OnCanceled()
        {
            YT_LOG_DEBUG("Request cancelation received (RequestId: %v)",
                RequestId_);

            if (Service_) {
                Service_->HandleRequestCancelation(RequestId_);
            }

            {
                auto guard = Guard(SpinLock_);
                CancelRequested_ = true;
                CheckCanceled(guard);
            }
        }

        void CheckCanceled(TGuard<TSpinLock>& guard)
        {
            if (CancelRequested_ && Stage_ == EServerCallStage::WaitingForService) {
                Stage_ = EServerCallStage::Done;
                guard.Release();
                Unref();
            }
        }
    };
};

DEFINE_REFCOUNTED_TYPE(TServer)

IServerPtr CreateServer(TServerConfigPtr config)
{
    return New<TServer>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc::NGrpc
