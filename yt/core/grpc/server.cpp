#include "server.h"
#include "dispatcher.h"
#include "config.h"
#include "helpers.h"

#include <yt/core/rpc/server_detail.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/bus/bus.h>

#include <yt/core/misc/small_vector.h>

#include <yt/core/ytree/convert.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/impl/codegen/grpc_types.h>

#include <array>

namespace NYT {
namespace NGrpc {

using namespace NRpc;
using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TServer)

DEFINE_ENUM(EServerCallStage,
    (Accept)
    (ReceivingRequest)
    (SendingInitialMetadata)
    (WaitingForService)
    (SendingResponse)
    (WaitingForClose)
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
        : TServerBase(GrpcLogger)
        , Config_(std::move(config))
        , LibraryLock_(TDispatcher::Get()->CreateLibraryLock())
        , CompletionQueue_(TDispatcher::Get()->PickRandomCompletionQueue())
    { }

private:
    const TServerConfigPtr Config_;

    const TGrpcLibraryLockPtr LibraryLock_;
    grpc_completion_queue* const CompletionQueue_;

    TGrpcServerPtr Native_;


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
                // TODO(babenko): secured ports
                YCHECK(addressConfig->Type == EAddressType::Insecure);
                auto result = grpc_server_add_insecure_http2_port(
                    Native_.Unwrap(),
                    addressConfig->Address.c_str());
                if (result == 0) {
                    THROW_ERROR_EXCEPTION("Error configuring server to listen at %Qlv address %Qv",
                        addressConfig->Type,
                        addressConfig->Address);
                }
                LOG_DEBUG("Insecure server address configured (Address: %v)", addressConfig->Address);
            }
        } catch (const std::exception& ex) {
            Cleanup();
            throw;
        }

        grpc_server_start(Native_.Unwrap());

        Ref();

        TServerBase::DoStart();

        New<TCallHandler>(this);
    }

    virtual TFuture<void> DoStop(bool graceful) override
    {
        class TStopTag
            : public TCompletionQueueTag
        {
        public:
            TFuture<void> GetFuture()
            {
                return Promise_.ToFuture();
            }

            virtual void Run(bool success, int /*cookie*/) override
            {
                Promise_.Set(success ? TError() : TError("GRPC server shutdown failed"));
                delete this;
            }

        private:
            TPromise<void> Promise_ = NewPromise<void>();

        };

        auto* shutdownTag = graceful ? new TStopTag() : nullptr;
        auto shutdownFuture = shutdownTag ? shutdownTag->GetFuture() : VoidFuture;

        grpc_server_shutdown_and_notify(
            Native_.Unwrap(),
            CompletionQueue_,
            shutdownTag ? shutdownTag->GetTag() : nullptr);

        if (!graceful) {
            grpc_server_cancel_all_calls(Native_.Unwrap());
        }

        return shutdownFuture.Apply(BIND(&TServer::OnShutdownFinished, MakeStrong(this), graceful));
    }

    TFuture<void> OnShutdownFinished(bool graceful)
    {
        Cleanup();
        Unref();
        return TServerBase::DoStop(graceful);
    }

    void Cleanup()
    {
        Native_.Reset();
    }

    class TCallHandler
        : public TCompletionQueueTag
        , public IBus
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
            YCHECK(result == GRPC_CALL_OK);

            EndpointDescription_ = CallDetails_->host;

            Ref();
        }

        // TCompletionQueueTag overrides
        virtual void Run(bool success, int cookie_) override
        {
            auto cookie = EServerCallCookie(cookie_);
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
                            Y_UNREACHABLE();
                    }
                    break;

                case EServerCallCookie::Close:
                    OnCloseReceived(success);
                    break;

                default:
                    Y_UNREACHABLE();
            }
        }

        // IBus overrides
        virtual const TString& GetEndpointDescription() const override
        {
            return EndpointDescription_;
        }

        virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
        {
            Y_UNREACHABLE();
        }

        virtual TTcpDispatcherStatistics GetStatistics() const override
        {
            return {};
        }

        virtual TFuture<void> Send(TSharedRefArray message, const NBus::TSendOptions& /*options*/) override
        {
            auto guard = Guard(SpinLock_);
            YCHECK(!ResponseMessage_);
            ResponseMessage_ = std::move(message);
            MaybeSendResponse(guard);
            return TFuture<void>();
        }

        virtual void SetTosLevel(TTosLevel /*tosLevel*/) override
        { }

        virtual void Terminate(const TError& error) override
        { }

        virtual void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override
        { }

        virtual void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override
        { }

    private:
        const TServerPtr Owner_;

        grpc_completion_queue* const CompletionQueue_;
        const NLogging::TLogger& Logger;

        TSpinLock SpinLock_;
        EServerCallStage Stage_ = EServerCallStage::Accept;
        TSharedRefArray ResponseMessage_;

        TRequestId RequestId_;
        TString ServiceName_;
        TString MethodName_;
        TNullable<TDuration> Timeout_;
        IServicePtr Service_;

        TString EndpointDescription_;

        TGrpcMetadataArrayBuilder InitialMetadataBuilder_;
        TGrpcMetadataArrayBuilder TrailingMetadataBuilder_;

        TGrpcCallDetails CallDetails_;
        TGrpcMetadataArray CallMetadata_;
        TGrpcCallPtr Call_;
        TGrpcByteBufferPtr RequestBodyBuffer_;
        TGrpcByteBufferPtr ResponseBodyBuffer_;
        TString ErrorMessage_;
        int Canceled_ = 0;


        template <class TOps>
        void StartBatch(const TOps& ops, EServerCallCookie cookie)
        {
            auto result = grpc_call_start_batch(
                Call_.Unwrap(),
                ops.data(),
                ops.size(),
                GetTag(static_cast<int>(cookie)),
                nullptr);
            YCHECK(result == GRPC_CALL_OK);
        }

        void OnAccepted(bool success)
        {
            if (!success) {
                // This normally happens on server shutdown.
                Unref();
                return;
            }

            New<TCallHandler>(Owner_);

            ParseRequestId();

            if (!ParseRoutingParameters()) {
                LOG_DEBUG("Malformed request routing parameters (RawMethod: %v, RequestId: %v)",
                    CallDetails_->method,
                    RequestId_);
                Unref();
                return;
            }

            Timeout_ = GetTimeout(CallDetails_->deadline);

            LOG_DEBUG("Request accepted (RequestId: %v, Method: %v:%v, Host: %v, Timeout: %v)",
                RequestId_,
                ServiceName_,
                MethodName_,
                TStringBuf(CallDetails_->host),
                Timeout_);

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

        void ParseRequestId()
        {
            auto idString = CallMetadata_.Find(RequestIdMetadataKey);
            if (!idString) {
                RequestId_ = TRequestId::Create();
                return;
            }

            if (!TRequestId::FromString(idString, &RequestId_)) {
                RequestId_ = TRequestId::Create();
                LOG_WARNING("Malformed request id, using a random one (MalformedRequestId: %v, RequestId: %v)",
                    idString,
                    RequestId_);
            }
        }

        bool ParseRoutingParameters()
        {
            if (CallDetails_->method[0] != '/') {
                return false;
            }

            const char* secondSlash = strchr(CallDetails_->method + 1, '/');
            if (!secondSlash) {
                return false;
            }

            auto methodLength = strlen(CallDetails_->method);
            ServiceName_.assign(CallDetails_->method + 1, secondSlash);
            MethodName_.assign(secondSlash + 1, CallDetails_->method + methodLength);
            return true;
        }

        static TNullable<TDuration> GetTimeout(gpr_timespec deadline)
        {
            deadline = gpr_convert_clock_type(deadline, GPR_CLOCK_REALTIME);
            auto now = gpr_now(GPR_CLOCK_REALTIME);
            if (gpr_time_cmp(now, deadline) >= 0) {
                return TDuration::Zero();
            }
            auto micros = gpr_timespec_to_micros(gpr_time_sub(deadline, now));
            if (micros > std::numeric_limits<ui64>::max() / 2) {
                return Null;
            }
            return TDuration::MicroSeconds(static_cast<ui64>(micros));
        }

        void OnRequestReceived(bool success)
        {
            if (!success) {
                LOG_DEBUG("Failed to receive request body (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            if (!RequestBodyBuffer_) {
                LOG_DEBUG("Empty request body received (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            auto requestBody = ByteBufferToEnvelopedMessage(RequestBodyBuffer_.Unwrap());

            auto header = std::make_unique<NRpc::NProto::TRequestHeader>();
            ToProto(header->mutable_request_id(), RequestId_);
            header->set_service(ServiceName_);
            header->set_method(MethodName_);
            header->set_protocol_version(GenericProtocolVersion);
            if (Timeout_) {
                header->set_timeout(ToProto<i64>(*Timeout_));
            }
            // TODO: start time
            // TODO: user

            LOG_DEBUG("Request received (RequestId: %v)",
                RequestId_);

            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::SendingInitialMetadata;
            }

            LOG_DEBUG("Sending initial metadata (RequestId: %v)",
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
                ops[0].data.recv_close_on_server.cancelled = &Canceled_;

                StartBatch(ops, EServerCallCookie::Close);
            }

            if (Service_) {
                auto requestMessage = CreateRequestMessage(*header, requestBody, {});
                Service_->HandleRequest(std::move(header), std::move(requestMessage), this);
            } else {
                auto error = TError(
                    NRpc::EErrorCode::NoSuchService,
                    "Service is not registered")
                    << TErrorAttribute("service", ServiceName_);
                LOG_WARNING(error);
                auto response = CreateErrorResponseMessage(RequestId_, error);
                Send(std::move(response), NBus::TSendOptions(EDeliveryTrackingLevel::None));
            }
        }

        void OnInitialMetadataSent(bool success)
        {
            if (!success) {
                LOG_DEBUG("Failed to send initial metadata (RequestId: %v)",
                    RequestId_);
                Unref();
                return;
            }

            {
                auto guard = Guard(SpinLock_);
                Stage_ = EServerCallStage::WaitingForService;
                MaybeSendResponse(guard);
            }
        }


        void MaybeSendResponse(TGuard<TSpinLock>& guard)
        {
            if (!ResponseMessage_) {
                return;
            }
            if (Stage_ != EServerCallStage::WaitingForService) {
                return;
            }
            Stage_ = EServerCallStage::SendingResponse;
            guard.Release();

            LOG_DEBUG("Sending response (RequestId: %v)",
                RequestId_);

            NRpc::NProto::TResponseHeader responseHeader;
            YCHECK(ParseResponseHeader(ResponseMessage_, &responseHeader));

            SmallVector<grpc_op, 2> ops;

            TError error;
            if (responseHeader.has_error() && responseHeader.error().code() != static_cast<int>(NYT::EErrorCode::OK)) {
                FromProto(&error, responseHeader.error());
                ErrorMessage_ = ToString(error);
                TrailingMetadataBuilder_.Add(ErrorMetadataKey, SerializeError(error));
            } else {
                // Attachments are not supported.
                YCHECK(ResponseMessage_.Size() == 2);
                ResponseBodyBuffer_ = EnvelopedMessageToByteBuffer(ResponseMessage_[1]);

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
            ops.back().data.send_status_from_server.status_details = error.IsOK() ? nullptr : ErrorMessage_.c_str();
            ops.back().data.send_status_from_server.trailing_metadata_count = TrailingMetadataBuilder_.GetSize();
            ops.back().data.send_status_from_server.trailing_metadata = TrailingMetadataBuilder_.Unwrap();

            StartBatch(ops, EServerCallCookie::Normal);
        }


        void OnResponseSent(bool success)
        {
            if (success) {
                LOG_DEBUG("Response sent (RequestId: %v)",
                    RequestId_);
            } else {
                LOG_DEBUG("Failed to send response (RequestId: %v)",
                    RequestId_);
            }

            Unref();
        }


        void OnCloseReceived(bool success)
        {
            if (success) {
                if (Canceled_) {
                    LOG_DEBUG("Request cancelation received (RequestId: %v)",
                        RequestId_);
                    if (Service_) {
                        Service_->HandleRequestCancelation(RequestId_);
                    }
                } else {
                    LOG_DEBUG("Request closed (RequestId: %v)",
                        RequestId_);
                }
            } else {
                LOG_DEBUG("Failed to close request (RequestId: %v)",
                    RequestId_);
            }

            Unref();
        }
    };
};

DEFINE_REFCOUNTED_TYPE(TServer)

IServerPtr CreateServer(TServerConfigPtr config)
{
    return New<TServer>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NYT
