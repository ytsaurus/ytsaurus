#include "channel.h"
#include "config.h"
#include "dispatcher.h"
#include "helpers.h"

#include <yt/core/misc/singleton.h>

#include <yt/core/rpc/channel.h>
#include <yt/core/rpc/message.h>
#include <yt/core/rpc/proto/rpc.pb.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/profiling/timing.h>

#include <contrib/libs/grpc/include/grpc/grpc.h>
#include <contrib/libs/grpc/include/grpc/impl/codegen/grpc_types.h>
#include <contrib/libs/grpc/include/grpc/support/alloc.h>

#include <array>

namespace NYT {
namespace NRpc {
namespace NGrpc {

using namespace NRpc;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TChannel)

DEFINE_ENUM(EClientCallStage,
    (SendingRequest)
    (ReceivingInitialMetadata)
    (ReceivingResponse)
);

class TChannel
    : public IChannel
{
public:
    explicit TChannel(TChannelConfigPtr config)
        : Config_(std::move(config))
        , EndpointDescription_(Config_->Address)
        , EndpointAttributes_(ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
                .Item("address").Value(EndpointDescription_)
            .EndMap()))
    {
        TGrpcChannelArgs args(Config_->GrpcArguments);
        if (Config_->Credentials) {
            Credentials_ = LoadChannelCredentials(Config_->Credentials);
            Channel_= TGrpcChannelPtr(grpc_secure_channel_create(
                Credentials_.Unwrap(),
                Config_->Address.c_str(),
                args.Unwrap(),
                nullptr));
        } else {
            Channel_= TGrpcChannelPtr(grpc_insecure_channel_create(
                Config_->Address.c_str(),
                args.Unwrap(),
                nullptr));
        }
    }

    virtual const TString& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        TReaderGuard guard(SpinLock_);
        if (!TerminationError_.IsOK()) {
            auto error = TerminationError_;
            guard.Release();
            responseHandler->HandleError(error);
            return nullptr;
        }
        return New<TCallHandler>(
            this,
            options,
            std::move(request),
            std::move(responseHandler));
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        TWriterGuard guard(SpinLock_);
        if (!TerminationError_.IsOK()) {
            return VoidFuture;
        }
        TerminationError_ = error;
        LibraryLock_.Reset();
        Channel_.Reset();
        return VoidFuture;
    }

private:
    const TChannelConfigPtr Config_;
    const TString EndpointDescription_;
    const std::unique_ptr<IAttributeDictionary> EndpointAttributes_;

    TReaderWriterSpinLock SpinLock_;
    TError TerminationError_;
    TGrpcLibraryLockPtr LibraryLock_ = TDispatcher::Get()->CreateLibraryLock();
    TGrpcChannelPtr Channel_;
    TGrpcChannelCredentialsPtr Credentials_;


    class TCallHandler
        : public TCompletionQueueTag
        , public IClientRequestControl
    {
    public:
        TCallHandler(
            TChannelPtr owner,
            const TSendOptions& options,
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler)
            : Owner_(std::move(owner))
            , Options_(options)
            , Request_(std::move(request))
            , ResponseHandler_(std::move(responseHandler))
            , CompletionQueue_(TDispatcher::Get()->PickRandomCompletionQueue())
            , Logger(GrpcLogger)
        {
            Ref();

            LOG_DEBUG("Sending request (RequestId: %v, Method: %v:%v, Timeout: %v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod(),
                Options_.Timeout);

            Call_ = TGrpcCallPtr(grpc_channel_create_call(
                Owner_->Channel_.Unwrap(),
                nullptr,
                0,
                CompletionQueue_,
                Format("/%v/%v", Request_->GetService(), Request_->GetMethod()).c_str(),
                nullptr,
                GetDeadline(),
                nullptr));

            InitialMetadataBuilder_.Add(RequestIdMetadataKey, ToString(Request_->GetRequestId()));

            auto serializedMessage = Request_->Serialize();

            // Attachments are not supported.
            YCHECK(serializedMessage.Size() == 2);
            RequestBodyBuffer_ = EnvelopedMessageToByteBuffer(serializedMessage[1]);

            Stage_ = EClientCallStage::SendingRequest;

            std::array<grpc_op, 3> ops;

            ops[0].op = GRPC_OP_SEND_INITIAL_METADATA;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.send_initial_metadata.maybe_compression_level.is_set = false;
            ops[0].data.send_initial_metadata.metadata = InitialMetadataBuilder_.Unwrap();
            ops[0].data.send_initial_metadata.count = InitialMetadataBuilder_.GetSize();

            ops[1].op = GRPC_OP_SEND_MESSAGE;
            ops[1].data.send_message.send_message = RequestBodyBuffer_.Unwrap();
            ops[1].flags = 0;
            ops[1].reserved = nullptr;

            ops[2].op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
            ops[2].flags = 0;
            ops[2].reserved = nullptr;

            StartBatch(ops);
        }

        ~TCallHandler()
        {
            if (ResponseStatusDetails_) {
                gpr_free(ResponseStatusDetails_);
            }
        }

        // TCompletionQueueTag overrides
        virtual void Run(bool success, int /*cookie*/) override
        {
            switch (Stage_) {
                case EClientCallStage::SendingRequest:
                    OnRequestSent(success);
                    break;

                case EClientCallStage::ReceivingInitialMetadata:
                    OnInitialMetadataReceived(success);
                    break;

                case EClientCallStage::ReceivingResponse:
                    OnResponseReceived(success);
                    break;

                default:
                    Y_UNREACHABLE();
            }
        }

        // IClientRequestControl overrides
        virtual void Cancel() override
        {
            auto result = grpc_call_cancel(Call_.Unwrap(), nullptr);
            YCHECK(result == GRPC_CALL_OK);

            LOG_DEBUG("Request canceled (RequestId: %v)", Request_->GetRequestId());

            NotifyError(
                AsStringBuf("Request canceled"),
                TError(NYT::EErrorCode::Canceled, "Request canceled"));
        }

    private:
        const TChannelPtr Owner_;
        const TSendOptions Options_;
        const IClientRequestPtr Request_;
        const IClientResponseHandlerPtr ResponseHandler_;

        grpc_completion_queue* const CompletionQueue_;
        const NLogging::TLogger& Logger;

        NProfiling::TWallTimer Timer_;

        TGrpcCallPtr Call_;
        TGrpcByteBufferPtr RequestBodyBuffer_;
        TGrpcMetadataArray ResponseInitialMetadata_;
        TGrpcByteBufferPtr ResponseBodyBuffer_;
        TGrpcMetadataArray ResponseFinalMetdata_;
        grpc_status_code ResponseStatusCode_ = GRPC_STATUS_UNKNOWN;
        char* ResponseStatusDetails_ = nullptr;
        size_t ResponseStatusCapacity_ = 0;

        EClientCallStage Stage_;
        std::atomic_flag Notified_ = ATOMIC_FLAG_INIT;

        TGrpcMetadataArrayBuilder InitialMetadataBuilder_;


        gpr_timespec GetDeadline() const
        {
            return Options_.Timeout
                ? gpr_time_add(
                    gpr_now(GPR_CLOCK_REALTIME),
                    gpr_time_from_micros(Options_.Timeout->MicroSeconds(), GPR_TIMESPAN))
                : gpr_inf_future(GPR_CLOCK_REALTIME);
        }

        void OnRequestSent(bool success)
        {
            if (!success) {
                NotifyError(
                    AsStringBuf("Failed to send request"),
                    TError(NRpc::EErrorCode::TransportError, "Failed to send request"));
                Unref();
                return;
            }

            LOG_DEBUG("Request sent (RequestId: %v, Method: %v:%v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod());

            Stage_ = EClientCallStage::ReceivingInitialMetadata;

            std::array<grpc_op, 1> ops;

            ops[0].op = GRPC_OP_RECV_INITIAL_METADATA;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.recv_initial_metadata.recv_initial_metadata = ResponseInitialMetadata_.Unwrap();

            StartBatch(ops);
        }

        void OnInitialMetadataReceived(bool success)
        {
            if (!success) {
                NotifyError(
                    AsStringBuf("Failed to receive initial response metadata"),
                    TError(NRpc::EErrorCode::TransportError, "Failed to receive initial response metadata"));
                Unref();
                return;
            }

            LOG_DEBUG("Initial response metadata received (RequestId: %v)",
                Request_->GetRequestId());

            Stage_ = EClientCallStage::ReceivingResponse;

            std::array<grpc_op, 2> ops;

            ops[0].op = GRPC_OP_RECV_MESSAGE;
            ops[0].flags = 0;
            ops[0].reserved = nullptr;
            ops[0].data.recv_message.recv_message = ResponseBodyBuffer_.GetPtr();

            ops[1].op = GRPC_OP_RECV_STATUS_ON_CLIENT;
            ops[1].flags = 0;
            ops[1].reserved = nullptr;
            ops[1].data.recv_status_on_client.trailing_metadata = ResponseFinalMetdata_.Unwrap();
            ops[1].data.recv_status_on_client.status = &ResponseStatusCode_;
            ops[1].data.recv_status_on_client.status_details = &ResponseStatusDetails_;
            ops[1].data.recv_status_on_client.status_details_capacity = &ResponseStatusCapacity_;

            StartBatch(ops);
        }

        void OnResponseReceived(bool success)
        {
            if (!success) {
                NotifyError(
                    AsStringBuf("Failed to receive response"),
                    TError(NRpc::EErrorCode::TransportError, "Failed to receive response"));
                Unref();
                return;
            }

            if (ResponseStatusCode_ == GRPC_STATUS_OK) {
                if (ResponseBodyBuffer_) {
                    NRpc::NProto::TResponseHeader responseHeader;
                    ToProto(responseHeader.mutable_request_id(), Request_->GetRequestId());

                    auto responseBody = ByteBufferToEnvelopedMessage(ResponseBodyBuffer_.Unwrap());

                    auto responseMessage = CreateResponseMessage(
                        responseHeader,
                        std::move(responseBody),
                        {});

                    NotifyResponse(std::move(responseMessage));
                } else {
                    auto error = TError(NRpc::EErrorCode::ProtocolError, "Empty response body");
                    NotifyError(AsStringBuf("Request failed"), error);
                }
            } else {
                TError error;
                auto serializedError = ResponseFinalMetdata_.Find(ErrorMetadataKey);
                if (serializedError) {
                    error = DeserializeError(serializedError);
                } else {
                    error = TError(NRpc::EErrorCode::TransportError, "GRPC error")
                        << TErrorAttribute("details", TString(ResponseStatusDetails_));
                }
                NotifyError(AsStringBuf("Request failed"), error);
            }

            Unref();
        }


        template <class TOps>
        void StartBatch(const TOps& ops)
        {
            auto result = grpc_call_start_batch(
                Call_.Unwrap(),
                ops.data(),
                ops.size(),
                GetTag(),
                nullptr);
            YCHECK(result == GRPC_CALL_OK);
        }

        void NotifyError(const TStringBuf& reason, const TError& error)
        {
            if (Notified_.test_and_set()) {
                return;
            }

            auto detailedError = error
                << TErrorAttribute("service", Request_->GetService())
                << TErrorAttribute("method", Request_->GetMethod())
                << TErrorAttribute("request_id", Request_->GetRequestId())
                << Owner_->GetEndpointAttributes();
            if (Options_.Timeout) {
                detailedError = detailedError
                    << TErrorAttribute("timeout", Options_.Timeout);
            }

            LOG_DEBUG(detailedError, "%v (RequestId: %v)",
                reason,
                Request_->GetRequestId());

            ResponseHandler_->HandleError(detailedError);
        }

        void NotifyResponse(TSharedRefArray message)
        {
            if (Notified_.test_and_set()) {
                return;
            }

            LOG_DEBUG("Response received (RequestId: %v, Method: %v:%v, TotalTime: %v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod(),
                Timer_.GetElapsedTime());

            ResponseHandler_->HandleResponse(std::move(message));
        }
    };
};

DEFINE_REFCOUNTED_TYPE(TChannel)

IChannelPtr CreateGrpcChannel(TChannelConfigPtr config)
{
    return New<TChannel>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

class TChannelFactory
    : public IChannelFactory
{
public:
    virtual IChannelPtr CreateChannel(const TString& address) override
    {
        auto config = New<TChannelConfig>();
        config->Address = address;
        return CreateGrpcChannel(config);
    }
};

IChannelFactoryPtr GetGrpcChannelFactory()
{
    return RefCountedSingleton<TChannelFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NGrpc
} // namespace NRpc
} // namespace NYT
