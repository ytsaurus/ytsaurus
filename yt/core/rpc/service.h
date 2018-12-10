#pragma once

#include "public.h"

#include "protocol_version.h"

#include <yt/core/actions/signal.h>

#include <yt/core/bus/public.h>

#include <yt/core/net/public.h>

#include <yt/core/logging/log.h>

#include <yt/core/misc/ref.h>

#include <yt/core/rpc/proto/rpc.pb.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

//! Represents an RPC request at server-side.
/*!
 *  \note
 *  Implementations are not thread-safe.
 */
struct IServiceContext
    : public virtual TIntrinsicRefCounted
{
    //! Returns raw header of the request being handled.
    virtual const NProto::TRequestHeader& GetRequestHeader() const = 0;

    //! Returns the message that contains the request being handled.
    virtual TSharedRefArray GetRequestMessage() const = 0;

    //! Returns the id of the request.
    /*!
     *  These ids are assigned by the client to distinguish between responses.
     *  The server should not rely on their uniqueness.
     *
     *  #NullRequestId is a possible value.
     */
    virtual TRequestId GetRequestId() const = 0;

    //! Return statistics from underlying bus.
    /*!
     *  For implementations not using bus, returns all zeroes.
     */
    virtual NYT::NBus::TTcpDispatcherStatistics GetBusStatistics() const = 0;

    virtual const NYTree::IAttributeDictionary& GetEndpointAttributes() const = 0;

    //! Returns the instant when the current retry of request was issued by the client, if known.
    virtual std::optional<TInstant> GetStartTime() const = 0;

    //! Returns the client-specified request timeout, if any.
    virtual std::optional<TDuration> GetTimeout() const = 0;

    //! Returns |true| if this is a duplicate copy of a previously sent (and possibly served) request.
    virtual bool IsRetry() const = 0;

    //! Returns the mutation id for this request, i.e. a unique id used to distinguish copies of the
    //! (semantically) same request. If no mutation id is assigned then returns null id.
    virtual TMutationId GetMutationId() const = 0;

    //! Returns request service name.
    virtual const TString& GetService() const = 0;

    //! Returns request method name.
    virtual const TString& GetMethod() const = 0;

    //! Returns request realm id.
    virtual const TRealmId& GetRealmId() const = 0;

    //! Returns the name of the user issuing the request.
    virtual const TString& GetUser() const = 0;

    //! Returns |true| if the request was already replied.
    virtual bool IsReplied() const = 0;

    //! Signals that the request processing is complete and sends reply to the client.
    virtual void Reply(const TError& error) = 0;

    //! Parses the message and forwards to the client.
    virtual void Reply(const TSharedRefArray& message) = 0;

    //! Called by the service request handler (prior to calling #Reply or #ReplyFrom) to indicate
    //! that the bulk of the request processing is complete.
    /*!
     *  Both calling and handling this method is completely optional.
     *
     *  Upon receiving this call, the current RPC infrastructure decrements the queue size counters and
     *  starts pumping more requests from the queue.
     */
    virtual void SetComplete() = 0;

    //! Returns |true| is the request was canceled.
    virtual bool IsCanceled() = 0;

    //! Raised when request processing is canceled.
    DECLARE_INTERFACE_SIGNAL(void(), Canceled);

    //! Cancels request processing.
    /*!
     *  Implementations are free to ignore this call.
     */
    virtual void Cancel() = 0;

    //! Returns a future representing the response message.
    /*!
     *  \note
     *  Can only be called before the request handling is started.
     */
    virtual TFuture<TSharedRefArray> GetAsyncResponseMessage() const = 0;

    //! Returns the serialized response message.
    /*!
     *  \note
     *  Can only be called after the context is replied.
     */
    virtual TSharedRefArray GetResponseMessage() const = 0;

    //! Returns the error that was previously set by #Reply.
    /*!
     *  Can only be called after the context is replied.
     */
    virtual const TError& GetError() const = 0;

    //! Returns the request body.
    virtual TSharedRef GetRequestBody() const = 0;

    //! Returns the response body.
    virtual TSharedRef GetResponseBody() = 0;

    //! Sets the response body.
    virtual void SetResponseBody(const TSharedRef& responseBody) = 0;

    //! Returns a vector of request attachments.
    virtual std::vector<TSharedRef>& RequestAttachments() = 0;

    //! Returns a vector of response attachments.
    virtual std::vector<TSharedRef>& ResponseAttachments() = 0;

    //! Returns immutable request header.
    virtual const NProto::TRequestHeader& RequestHeader() const = 0;

    //! Returns mutable request header.
    virtual NProto::TRequestHeader& RequestHeader() = 0;

    //! Sets and immediately logs the request logging info.
    virtual void SetRawRequestInfo(const TString& info) = 0;

    //! Sets the response logging info. This info will be logged when the context is replied.
    virtual void SetRawResponseInfo(const TString& info) = 0;

    //! Returns the logger for request/response messages.
    virtual const NLogging::TLogger& GetLogger() const = 0;

    //! Returns the logging level for request/response messages.
    virtual NLogging::ELogLevel GetLogLevel() const = 0;

    //! Returns |true| if requests and responses are pooled.
    virtual bool IsPooled() const = 0;


    // Extension methods.

    void SetRequestInfo();
    void SetResponseInfo();

    template <class... TArgs>
    void SetRequestInfo(const char* format, const TArgs&... args);

    template <class... TArgs>
    void SetResponseInfo(const char* format, const TArgs&... args);

    //! Replies with a given message when the latter is set.
    void ReplyFrom(TFuture<TSharedRefArray> asyncMessage);

    //! Replies with a given error when the latter is set.
    void ReplyFrom(TFuture<void> asyncError);
};

DEFINE_REFCOUNTED_TYPE(IServiceContext)

////////////////////////////////////////////////////////////////////////////////

struct TServiceId
{
    TServiceId();
    TServiceId(const TString& serviceName, const TRealmId& realmId = NullRealmId);

    TString ServiceName;
    TRealmId RealmId;
};

bool operator == (const TServiceId& lhs, const TServiceId& rhs);
bool operator != (const TServiceId& lhs, const TServiceId& rhs);

TString ToString(const TServiceId& serviceId);

////////////////////////////////////////////////////////////////////////////////

//! Represents an abstract service registered within IServer.
/*!
 *  \note
 *  Implementations must be fully thread-safe.
 */
struct IService
    : public virtual TRefCounted
{
    //! Applies a new configuration.
    virtual void Configure(NYTree::INodePtr config) = 0;

    //! Stops the service forbidding new requests to be served
    //! and returns the future that is set when all currently
    //! executing requests are finished.
    virtual TFuture<void> Stop() = 0;

    //! Returns the service id.
    virtual const TServiceId& GetServiceId() const = 0;

    //! Handles incoming request.
    virtual void HandleRequest(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray message,
        NYT::NBus::IBusPtr replyBus) = 0;

    //! Handles request cancelation.
    virtual void HandleRequestCancelation(
        const TRequestId& requestId) = 0;

};

DEFINE_REFCOUNTED_TYPE(IService)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

//! A hasher for TServiceId.
template <>
struct THash<NYT::NRpc::TServiceId>
{
    inline size_t operator()(const NYT::NRpc::TServiceId& id) const
    {
        return
            THash<TString>()(id.ServiceName) * 497 +
            THash<NYT::NRpc::TRealmId>()(id.RealmId);
    }
};

////////////////////////////////////////////////////////////////////////////////

#define SERVICE_INL_H_
#include "service-inl.h"
#undef SERVICE_INL_H_

